from src.core.security import hash_password
from src.core.settings import Settings
from src.models.user import UserRole
from src.repositories.user import UserRepository
from sqlalchemy.ext.asyncio import AsyncSession


async def ensure_configured_admin(session: AsyncSession, settings: Settings) -> None:
    if not settings.admin_email or not settings.admin_password:
        return

    email = settings.admin_email.strip().lower()
    users = UserRepository(session)
    user = await users.get_by_email(email)
    password_hash = hash_password(settings.admin_password)

    if user is None:
        await users.create(
            email=email,
            full_name="Admin",
            password_hash=password_hash,
            role=UserRole.ADMIN,
        )
    else:
        user.role = UserRole.ADMIN
        user.password_hash = password_hash

    await session.commit()
