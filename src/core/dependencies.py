from typing import Annotated
import uuid

from fastapi import BackgroundTasks, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.session import get_db
from src.core.security import decode_token
from src.core.settings import Settings, get_settings
from src.models.user import User, UserRole
from src.repositories.user import UserRepository
from src.services.blob_storage import BlobStorage, VercelBlobStorage
from src.services.document_processing import (
    DocumentProcessingScheduler,
    FastAPIDocumentProcessingScheduler,
)
from src.services.program_import import HttpxProgramImportClient, ProgramImportClient

bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> User:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication credentials were not provided.",
        )

    payload = decode_token(
        token=credentials.credentials,
        settings=settings,
        expected_type="access",
    )

    try:
        user_id = uuid.UUID(payload["sub"])
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token subject is invalid.",
        ) from exc

    user = await UserRepository(db).get_by_id(user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authenticated user was not found.",
        )

    return user


async def get_current_admin_user(
    current_user: Annotated[User, Depends(get_current_user)],
) -> User:
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges are required.",
        )
    return current_user


async def get_blob_storage(
    settings: Annotated[Settings, Depends(get_settings)],
) -> BlobStorage:
    return VercelBlobStorage(settings)


async def get_document_processing_scheduler(
    background_tasks: BackgroundTasks,
) -> DocumentProcessingScheduler:
    return FastAPIDocumentProcessingScheduler(background_tasks)


async def get_program_import_client() -> ProgramImportClient:
    return HttpxProgramImportClient()
