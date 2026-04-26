import hashlib
import uuid
from datetime import UTC, datetime, timedelta

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError
from fastapi import HTTPException, status

from src.core.settings import Settings

password_hasher = PasswordHasher()


def utcnow() -> datetime:
    return datetime.now(UTC)


def hash_password(password: str) -> str:
    return password_hasher.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return password_hasher.verify(password_hash, password)
    except (InvalidHashError, VerifyMismatchError):
        return False


def hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def create_access_token(*, user_id: uuid.UUID, settings: Settings) -> str:
    return _encode_token(
        subject=str(user_id),
        token_type="access",
        expires_delta=timedelta(minutes=settings.access_token_ttl_minutes),
        settings=settings,
    )


def create_refresh_token(*, user_id: uuid.UUID, settings: Settings) -> tuple[str, str, datetime]:
    token_jti = str(uuid.uuid4())
    expires_at = utcnow() + timedelta(days=settings.refresh_token_ttl_days)
    token = _encode_token(
        subject=str(user_id),
        token_type="refresh",
        expires_delta=timedelta(days=settings.refresh_token_ttl_days),
        settings=settings,
        token_jti=token_jti,
    )
    return token, token_jti, expires_at


def decode_token(*, token: str, settings: Settings, expected_type: str) -> dict:
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired.",
        ) from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token.",
        ) from exc

    if payload.get("type") != expected_type:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type.",
        )

    if "sub" not in payload or "jti" not in payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token payload is invalid.",
        )

    return payload


def _encode_token(
    *,
    subject: str,
    token_type: str,
    expires_delta: timedelta,
    settings: Settings,
    token_jti: str | None = None,
) -> str:
    now = utcnow()
    payload = {
        "sub": subject,
        "type": token_type,
        "jti": token_jti or str(uuid.uuid4()),
        "iat": now,
        "exp": now + expires_delta,
    }
    return jwt.encode(payload, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
