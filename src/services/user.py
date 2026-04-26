import uuid

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.settings import Settings
from src.models.user import User, UserRole
from src.repositories.user import UserRepository
from src.schemas.auth import UserResponse
from src.schemas.user import UserListResponse


class UserService:
    def __init__(self, session: AsyncSession, settings: Settings | None = None) -> None:
        self.session = session
        self.settings = settings
        self.users = UserRepository(session)

    async def update_current_user(
        self,
        *,
        user: User,
        full_name: str | None,
    ) -> User:
        if full_name is not None:
            user.full_name = full_name.strip()

        await self.session.commit()
        await self.session.refresh(user)
        return user

    async def update_user_role(self, *, email: str, role: UserRole) -> User:
        user = await self.users.get_by_email(email)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User was not found.",
            )

        if self._is_bootstrap_admin(user) and role != UserRole.ADMIN:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Configured bootstrap admin cannot be demoted.",
            )

        user.role = role
        await self.session.commit()
        await self.session.refresh(user)
        return user

    async def list_users(
        self,
        *,
        limit: int,
        offset: int,
        search: str | None,
    ) -> UserListResponse:
        normalized_search = search.strip() if search else None
        if normalized_search == "":
            normalized_search = None

        users, total = await self.users.list_users(
            limit=limit,
            offset=offset,
            search=normalized_search,
        )
        return UserListResponse(
            items=[UserResponse.model_validate(user) for user in users],
            total=total,
            limit=limit,
            offset=offset,
        )

    async def get_user_by_id_for_requester(
        self,
        *,
        user_id: uuid.UUID,
        requester: User,
    ) -> User:
        user = await self.users.get_by_id(user_id)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User was not found.",
            )

        if requester.role != UserRole.ADMIN and requester.id != user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You are not allowed to access this user.",
            )

        return user

    def _is_bootstrap_admin(self, user: User) -> bool:
        if self.settings is None or not self.settings.admin_email:
            return False
        return user.email == self.settings.admin_email.strip().lower()
