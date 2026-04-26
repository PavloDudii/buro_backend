import uuid

from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.user import User, UserRole


class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_id(self, user_id: uuid.UUID) -> User | None:
        result = await self.session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()

    async def get_by_email(self, email: str) -> User | None:
        result = await self.session.execute(select(User).where(User.email == email))
        return result.scalar_one_or_none()

    async def create(
        self,
        *,
        email: str,
        full_name: str,
        password_hash: str,
        role: UserRole = UserRole.USER,
    ) -> User:
        user = User(
            email=email,
            full_name=full_name,
            password_hash=password_hash,
            role=role,
        )
        self.session.add(user)
        await self.session.flush()
        return user

    async def email_exists(self, *, email: str, exclude_user_id: uuid.UUID | None = None) -> bool:
        statement = select(User.id).where(User.email == email)
        if exclude_user_id is not None:
            statement = statement.where(User.id != exclude_user_id)
        result = await self.session.execute(statement)
        return result.scalar_one_or_none() is not None

    async def list_users(
        self,
        *,
        limit: int,
        offset: int,
        search: str | None = None,
    ) -> tuple[list[User], int]:
        filters = []
        if search:
            pattern = f"%{search.strip()}%"
            filters.append(
                or_(
                    cast(User.id, String).ilike(pattern),
                    User.email.ilike(pattern),
                    User.full_name.ilike(pattern),
                )
            )

        base_statement = select(User)
        count_statement = select(func.count()).select_from(User)
        if filters:
            base_statement = base_statement.where(*filters)
            count_statement = count_statement.where(*filters)

        total_result = await self.session.execute(count_statement)
        users_result = await self.session.execute(
            base_statement.order_by(User.created_at.desc(), User.id).limit(limit).offset(offset)
        )
        return list(users_result.scalars().all()), total_result.scalar_one()
