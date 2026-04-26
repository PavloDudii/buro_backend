import uuid
from dataclasses import dataclass

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    hash_token,
    utcnow,
    verify_password,
)
from src.core.settings import Settings
from src.models.refresh_session import RefreshSession
from src.models.user import User
from src.repositories.refresh_session import RefreshSessionRepository
from src.repositories.user import UserRepository


@dataclass
class AuthTokens:
    access_token: str
    refresh_token: str


class AuthService:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self.session = session
        self.settings = settings
        self.users = UserRepository(session)
        self.refresh_sessions = RefreshSessionRepository(session)

    async def register(self, *, email: str, full_name: str, password: str) -> tuple[User, AuthTokens]:
        if await self.users.email_exists(email=email):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A user with this email already exists.",
            )

        user = await self.users.create(
            email=email,
            full_name=full_name.strip(),
            password_hash=hash_password(password),
        )
        tokens = await self._issue_tokens(user)
        await self.session.commit()
        await self.session.refresh(user)
        return user, tokens

    async def login(self, *, email: str, password: str) -> tuple[User, AuthTokens]:
        user = await self.users.get_by_email(email)
        if user is None or not verify_password(password, user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password.",
            )

        tokens = await self._issue_tokens(user)
        await self.session.commit()
        return user, tokens

    async def refresh(self, *, refresh_token: str) -> tuple[User, AuthTokens]:
        payload = decode_token(token=refresh_token, settings=self.settings, expected_type="refresh")
        user_id = self._parse_user_id(payload["sub"])
        stored_session = await self._validate_refresh_session(
            user_id=user_id,
            refresh_token=refresh_token,
            token_jti=payload["jti"],
        )

        user = await self.users.get_by_id(user_id)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User no longer exists.",
            )

        tokens, replacement_session = await self._issue_tokens(user, return_refresh_session=True)
        stored_session.revoked_at = utcnow()
        stored_session.replaced_by_id = replacement_session.id
        await self.session.commit()
        return user, tokens

    async def logout(self, *, refresh_token: str) -> None:
        payload = decode_token(token=refresh_token, settings=self.settings, expected_type="refresh")
        user_id = self._parse_user_id(payload["sub"])
        stored_session = await self._validate_refresh_session(
            user_id=user_id,
            refresh_token=refresh_token,
            token_jti=payload["jti"],
        )
        await self.refresh_sessions.revoke(stored_session, revoked_at=utcnow())
        await self.session.commit()

    async def change_password(
        self,
        *,
        user: User,
        current_password: str,
        new_password: str,
    ) -> User:
        if not verify_password(current_password, user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Current password is incorrect.",
            )

        user.password_hash = hash_password(new_password)
        await self.refresh_sessions.revoke_all_for_user(user_id=user.id, revoked_at=utcnow())
        await self.session.commit()
        await self.session.refresh(user)
        return user

    async def _issue_tokens(
        self,
        user: User,
        *,
        return_refresh_session: bool = False,
    ) -> AuthTokens | tuple[AuthTokens, RefreshSession]:
        access_token = create_access_token(user_id=user.id, settings=self.settings)
        refresh_token, token_jti, expires_at = create_refresh_token(
            user_id=user.id,
            settings=self.settings,
        )
        refresh_session = await self.refresh_sessions.create(
            user_id=user.id,
            token_jti=token_jti,
            token_hash=hash_token(refresh_token),
            expires_at=expires_at,
        )
        tokens = AuthTokens(access_token=access_token, refresh_token=refresh_token)
        if return_refresh_session:
            return tokens, refresh_session
        return tokens

    async def _validate_refresh_session(
        self,
        *,
        user_id: uuid.UUID,
        refresh_token: str,
        token_jti: str,
    ) -> RefreshSession:
        stored_session = await self.refresh_sessions.get_by_jti(token_jti)
        if stored_session is None or stored_session.user_id != user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token is invalid.",
            )

        if stored_session.token_hash != hash_token(refresh_token):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token is invalid.",
            )

        if stored_session.revoked_at is not None or stored_session.expires_at <= utcnow():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Refresh token is no longer active.",
            )

        return stored_session

    @staticmethod
    def _parse_user_id(raw_user_id: str) -> uuid.UUID:
        try:
            return uuid.UUID(raw_user_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token subject is invalid.",
            ) from exc
