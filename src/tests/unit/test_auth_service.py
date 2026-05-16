import uuid
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from src.core.security import decode_token, hash_password, utcnow
from src.core.settings import Settings
from src.models.refresh_session import RefreshSession
from src.models.user import User, UserRole
from src.services.auth import AuthService

TEST_SETTINGS = Settings(
    JWT_SECRET_KEY="unit-test-secret-key-with-32-plus-bytes",
    APP_ENV="test",
    DATABASE_URL="postgresql+psycopg://ignored/ignored",
)


class FakeUserRepository:
    def __init__(self) -> None:
        self._by_email: dict[str, User] = {}
        self._by_id: dict[uuid.UUID, User] = {}

    async def get_by_email(self, email: str) -> User | None:
        return self._by_email.get(email)

    async def get_by_id(self, user_id: uuid.UUID) -> User | None:
        return self._by_id.get(user_id)

    async def email_exists(
        self,
        *,
        email: str,
        exclude_user_id: uuid.UUID | None = None,
    ) -> bool:
        user = self._by_email.get(email)
        if user is None:
            return False
        if exclude_user_id is not None and user.id == exclude_user_id:
            return False
        return True

    async def create(
        self,
        *,
        email: str,
        full_name: str,
        password_hash: str,
        role: UserRole = UserRole.USER,
    ) -> User:
        now = utcnow()
        user = User(email=email, full_name=full_name, password_hash=password_hash, role=role)
        user.id = uuid.uuid4()  # SQLAlchemy default=uuid.uuid4 only fires on DB insert
        user.created_at = now
        user.updated_at = now
        self._by_email[email] = user
        self._by_id[user.id] = user
        return user

    def seed(self, user: User) -> None:
        self._by_email[user.email] = user
        self._by_id[user.id] = user


class FakeRefreshSessionRepository:
    def __init__(self) -> None:
        self._by_jti: dict[str, RefreshSession] = {}

    async def create(
        self,
        *,
        user_id: uuid.UUID,
        token_jti: str,
        token_hash: str,
        expires_at: datetime,
    ) -> RefreshSession:
        now = utcnow()
        rs = RefreshSession(
            user_id=user_id,
            token_jti=token_jti,
            token_hash=token_hash,
            expires_at=expires_at,
        )
        rs.id = uuid.uuid4()  # SQLAlchemy default=uuid.uuid4 only fires on DB insert
        rs.created_at = now
        rs.updated_at = now
        self._by_jti[token_jti] = rs
        return rs

    async def get_by_jti(self, token_jti: str) -> RefreshSession | None:
        return self._by_jti.get(token_jti)

    async def revoke(self, refresh_session: RefreshSession, *, revoked_at: datetime) -> None:
        refresh_session.revoked_at = revoked_at

    async def revoke_all_for_user(self, *, user_id: uuid.UUID, revoked_at: datetime) -> None:
        for rs in self._by_jti.values():
            if rs.user_id == user_id and rs.revoked_at is None:
                rs.revoked_at = revoked_at


def make_service(
    fake_users: FakeUserRepository | None = None,
    fake_sessions: FakeRefreshSessionRepository | None = None,
) -> AuthService:
    return AuthService(
        AsyncMock(),
        TEST_SETTINGS,
        users=fake_users or FakeUserRepository(),
        refresh_sessions=fake_sessions or FakeRefreshSessionRepository(),
    )


def make_user(email: str = "user@example.com", password: str = "Password123") -> User:
    now = utcnow()
    user = User(
        email=email,
        full_name="Test User",
        password_hash=hash_password(password),
        role=UserRole.USER,
    )
    user.id = uuid.uuid4()  # SQLAlchemy default=uuid.uuid4 only fires on DB insert
    user.created_at = now
    user.updated_at = now
    return user


# ── register ──────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_register_success_returns_user_and_tokens() -> None:
    fake_users = FakeUserRepository()
    fake_sessions = FakeRefreshSessionRepository()
    svc = make_service(fake_users, fake_sessions)

    user, tokens = await svc.register(
        email="new@example.com",
        full_name="New User",
        password="Password123",
    )

    assert user.email == "new@example.com"
    assert user.full_name == "New User"
    assert user.role == UserRole.USER
    assert tokens.access_token
    assert tokens.refresh_token
    assert len(fake_sessions._by_jti) == 1


@pytest.mark.anyio
async def test_register_duplicate_email_raises_409() -> None:
    fake_users = FakeUserRepository()
    fake_users.seed(make_user("taken@example.com"))
    svc = make_service(fake_users)

    with pytest.raises(HTTPException) as exc_info:
        await svc.register(email="taken@example.com", full_name="Other", password="Password123")

    assert exc_info.value.status_code == 409


# ── login ─────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_login_success_returns_user_and_tokens() -> None:
    fake_users = FakeUserRepository()
    fake_users.seed(make_user("user@example.com", "Password123"))
    svc = make_service(fake_users)

    user, tokens = await svc.login(email="user@example.com", password="Password123")

    assert user.email == "user@example.com"
    payload = decode_token(token=tokens.access_token, settings=TEST_SETTINGS, expected_type="access")
    assert payload["sub"] == str(user.id)


@pytest.mark.anyio
async def test_login_wrong_password_raises_401() -> None:
    fake_users = FakeUserRepository()
    fake_users.seed(make_user("user@example.com", "Password123"))
    svc = make_service(fake_users)

    with pytest.raises(HTTPException) as exc_info:
        await svc.login(email="user@example.com", password="WrongPassword")

    assert exc_info.value.status_code == 401


@pytest.mark.anyio
async def test_login_unknown_email_raises_401() -> None:
    svc = make_service()

    with pytest.raises(HTTPException) as exc_info:
        await svc.login(email="nobody@example.com", password="Password123")

    assert exc_info.value.status_code == 401


# ── refresh ───────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_refresh_valid_token_rotates_session() -> None:
    fake_users = FakeUserRepository()
    user = make_user()
    fake_users.seed(user)
    fake_sessions = FakeRefreshSessionRepository()
    svc = make_service(fake_users, fake_sessions)

    _, first_tokens = await svc.login(email=user.email, password="Password123")
    assert len(fake_sessions._by_jti) == 1

    refreshed_user, new_tokens = await svc.refresh(refresh_token=first_tokens.refresh_token)

    assert refreshed_user.id == user.id
    assert new_tokens.refresh_token != first_tokens.refresh_token
    old_jti = list(fake_sessions._by_jti.keys())[0]
    assert fake_sessions._by_jti[old_jti].revoked_at is not None


@pytest.mark.anyio
async def test_refresh_revoked_token_raises_401() -> None:
    fake_users = FakeUserRepository()
    user = make_user()
    fake_users.seed(user)
    fake_sessions = FakeRefreshSessionRepository()
    svc = make_service(fake_users, fake_sessions)

    _, tokens = await svc.login(email=user.email, password="Password123")
    await svc.refresh(refresh_token=tokens.refresh_token)

    with pytest.raises(HTTPException) as exc_info:
        await svc.refresh(refresh_token=tokens.refresh_token)

    assert exc_info.value.status_code == 401


# ── logout ────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_logout_revokes_refresh_session() -> None:
    fake_users = FakeUserRepository()
    user = make_user()
    fake_users.seed(user)
    fake_sessions = FakeRefreshSessionRepository()
    svc = make_service(fake_users, fake_sessions)

    _, tokens = await svc.login(email=user.email, password="Password123")
    await svc.logout(refresh_token=tokens.refresh_token)

    rs = list(fake_sessions._by_jti.values())[0]
    assert rs.revoked_at is not None

    with pytest.raises(HTTPException) as exc_info:
        await svc.refresh(refresh_token=tokens.refresh_token)
    assert exc_info.value.status_code == 401


# ── change_password ───────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_change_password_wrong_current_raises_400() -> None:
    user = make_user(password="Password123")
    svc = make_service()

    with pytest.raises(HTTPException) as exc_info:
        await svc.change_password(
            user=user,
            current_password="WrongPassword",
            new_password="NewPassword123",
        )

    assert exc_info.value.status_code == 400


@pytest.mark.anyio
async def test_change_password_revokes_all_sessions() -> None:
    fake_users = FakeUserRepository()
    user = make_user(password="Password123")
    fake_users.seed(user)
    fake_sessions = FakeRefreshSessionRepository()
    svc = make_service(fake_users, fake_sessions)

    await svc.login(email=user.email, password="Password123")
    await svc.login(email=user.email, password="Password123")
    assert len(fake_sessions._by_jti) == 2

    await svc.change_password(
        user=user,
        current_password="Password123",
        new_password="NewPassword123",
    )

    assert all(rs.revoked_at is not None for rs in fake_sessions._by_jti.values())
