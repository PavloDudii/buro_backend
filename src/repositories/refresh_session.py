import uuid
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.refresh_session import RefreshSession


class RefreshSessionRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self,
        *,
        user_id: uuid.UUID,
        token_jti: str,
        token_hash: str,
        expires_at: datetime,
    ) -> RefreshSession:
        refresh_session = RefreshSession(
            user_id=user_id,
            token_jti=token_jti,
            token_hash=token_hash,
            expires_at=expires_at,
        )
        self.session.add(refresh_session)
        await self.session.flush()
        return refresh_session

    async def get_by_jti(self, token_jti: str) -> RefreshSession | None:
        result = await self.session.execute(
            select(RefreshSession).where(RefreshSession.token_jti == token_jti)
        )
        return result.scalar_one_or_none()

    async def revoke(self, refresh_session: RefreshSession, *, revoked_at: datetime) -> None:
        refresh_session.revoked_at = revoked_at
        await self.session.flush()

    async def revoke_all_for_user(self, *, user_id: uuid.UUID, revoked_at: datetime) -> None:
        await self.session.execute(
            update(RefreshSession)
            .where(RefreshSession.user_id == user_id, RefreshSession.revoked_at.is_(None))
            .values(revoked_at=revoked_at)
        )
