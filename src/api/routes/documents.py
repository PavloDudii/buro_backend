import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, Response, UploadFile, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.session import get_db
from src.core.dependencies import (
    bearer_scheme,
    get_blob_storage,
    get_current_admin_user,
    get_document_processing_scheduler,
)
from src.core.security import decode_token
from src.core.settings import Settings, get_settings
from src.models.user import User, UserRole
from src.repositories.user import UserRepository
from src.schemas.document import (
    DirectUploadAuthorizeRequest,
    DirectUploadAuthorizeResponse,
    DirectUploadCompleteRequest,
    DirectUploadInitRequest,
    DirectUploadInitResponse,
    DocumentExtractionItemListResponse,
    DocumentProcessingDetailsResponse,
    UploadedDocumentListResponse,
    UploadedDocumentResponse,
)
from src.services.blob_storage import BlobStorage
from src.services.document_extractions import (
    EXTRACTION_ITEM_TYPES,
    DocumentExtractionItemService,
)
from src.services.document_upload import DocumentUploadService
from src.services.document_processing import (
    DocumentProcessingQueueService,
    DocumentProcessingScheduler,
)

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
    processing_scheduler: Annotated[
        DocumentProcessingScheduler,
        Depends(get_document_processing_scheduler),
    ],
) -> UploadedDocumentListResponse:
    response = await DocumentUploadService(db, blob_storage=blob_storage).upload_documents(
        files=files,
        uploaded_by=current_admin,
    )
    processing_scheduler.schedule_documents([item.id for item in response.items])
    return response


@router.post(
    "/uploads/direct/init",
    response_model=DirectUploadInitResponse,
    status_code=status.HTTP_201_CREATED,
)
async def init_direct_document_upload(
    payload: DirectUploadInitRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> DirectUploadInitResponse:
    return await DocumentUploadService(db, settings=settings).init_direct_upload(
        original_filename=payload.original_filename,
        content_type=payload.content_type,
        size_bytes=payload.size_bytes,
        sha256_hash=payload.sha256_hash,
        uploaded_by=current_admin,
    )


@router.post(
    "/uploads/direct/authorize",
    response_model=DirectUploadAuthorizeResponse,
)
async def authorize_direct_document_upload(
    payload: DirectUploadAuthorizeRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> DirectUploadAuthorizeResponse:
    return await DocumentUploadService(db, settings=settings).authorize_direct_upload(
        intent_id=payload.intent_id,
        pathname=payload.pathname,
        content_type=payload.content_type,
        uploaded_by=current_admin,
    )


@router.post(
    "/uploads/direct/complete",
    response_model=UploadedDocumentResponse,
)
async def complete_direct_document_upload(
    payload: DirectUploadCompleteRequest,
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
    blob_storage: Annotated[BlobStorage, Depends(get_blob_storage)],
    processing_scheduler: Annotated[
        DocumentProcessingScheduler,
        Depends(get_document_processing_scheduler),
    ],
    settings: Annotated[Settings, Depends(get_settings)],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    callback_secret: Annotated[str | None, Header(alias="X-Direct-Upload-Callback-Secret")] = None,
) -> UploadedDocumentResponse:
    completed_by = await direct_upload_completion_user(
        db=db,
        settings=settings,
        credentials=credentials,
        callback_secret=callback_secret,
    )
    service = DocumentUploadService(db, blob_storage=blob_storage, settings=settings)
    completion = await service.complete_direct_upload(
        intent_id=payload.intent_id,
        pathname=payload.pathname,
        url=payload.url,
        download_url=payload.download_url,
        etag=payload.etag,
        completed_by=completed_by,
    )
    if completion.created:
        response.status_code = status.HTTP_201_CREATED
        processing_scheduler.schedule_documents([completion.document.id])
    return await service.document_to_response_async(completion.document)


@router.get(
    "/extraction-items",
    response_model=DocumentExtractionItemListResponse,
)
async def list_document_extraction_items(
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    item_type: Annotated[str | None, Query(alias="type", min_length=1, max_length=100)] = None,
    search: Annotated[str | None, Query(min_length=1, max_length=500)] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> DocumentExtractionItemListResponse:
    del current_admin
    if item_type is not None and item_type not in EXTRACTION_ITEM_TYPES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Unsupported extraction item type.",
        )
    return await DocumentExtractionItemService(db).list_items(
        item_type=item_type,
        search=search,
        limit=limit,
        offset=offset,
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


@router.get(
    "/{document_id}/processing",
    response_model=DocumentProcessingDetailsResponse,
)
async def get_document_processing(
    document_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
) -> DocumentProcessingDetailsResponse:
    del current_admin
    return await DocumentProcessingQueueService(db).get_processing_details(document_id=document_id)


@router.post(
    "/{document_id}/processing",
    response_model=UploadedDocumentResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def queue_document_processing(
    document_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    processing_scheduler: Annotated[
        DocumentProcessingScheduler,
        Depends(get_document_processing_scheduler),
    ],
) -> UploadedDocumentResponse:
    del current_admin
    document = await DocumentProcessingQueueService(db).queue_document(document_id=document_id)
    processing_scheduler.schedule_documents([document.id])
    return DocumentUploadService(db).document_to_response(document)


async def direct_upload_completion_user(
    *,
    db: AsyncSession,
    settings: Settings,
    credentials: HTTPAuthorizationCredentials | None,
    callback_secret: str | None,
) -> User | None:
    if (
        settings.direct_upload_callback_secret
        and callback_secret
        and callback_secret == settings.direct_upload_callback_secret
    ):
        return None

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
    if user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges are required.",
        )
    return user
