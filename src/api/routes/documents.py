import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Query, Response, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.session import get_db
from src.core.dependencies import get_blob_storage, get_current_admin_user
from src.models.user import User
from src.schemas.document import UploadedDocumentListResponse, UploadedDocumentResponse
from src.services.blob_storage import BlobStorage
from src.services.document_upload import DocumentUploadService

router = APIRouter(prefix="/documents", tags=["documents"])


@router.get("", response_model=UploadedDocumentListResponse)
async def list_documents(
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
    search: Annotated[str | None, Query(min_length=1, max_length=500)] = None,
) -> UploadedDocumentListResponse:
    del current_admin
    return await DocumentUploadService(db).list_documents(
        limit=limit,
        offset=offset,
        search=search,
    )


@router.post(
    "/uploads",
    response_model=UploadedDocumentListResponse,
    status_code=status.HTTP_201_CREATED,
)
async def upload_documents(
    files: Annotated[list[UploadFile], File()],
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    blob_storage: Annotated[BlobStorage, Depends(get_blob_storage)],
) -> UploadedDocumentListResponse:
    return await DocumentUploadService(db, blob_storage=blob_storage).upload_documents(
        files=files,
        uploaded_by=current_admin,
    )


@router.get("/{document_id}", response_model=UploadedDocumentResponse)
async def get_document(
    document_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
) -> UploadedDocumentResponse:
    del current_admin
    return await DocumentUploadService(db).get_document(document_id=document_id)


@router.delete("/{document_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_document(
    document_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
) -> Response:
    del current_admin
    await DocumentUploadService(db).delete_document(document_id=document_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
