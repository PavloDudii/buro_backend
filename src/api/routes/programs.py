from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.session import get_db
from src.core.dependencies import (
    get_blob_storage,
    get_current_admin_user,
    get_document_processing_scheduler,
    get_program_import_client,
)
from src.models.user import User
from src.schemas.program import (
    EducationProgramDetailResponse,
    EducationProgramListResponse,
    EducationProgramResponse,
    ProgramDocumentResponse,
    ProgramDocumentUpdateRequest,
    ProgramDepartmentUpdateRequest,
    ProgramImportRunResponse,
    ProgramUpdateRequest,
)
from src.services.blob_storage import BlobStorage
from src.services.document_processing import DocumentProcessingScheduler
from src.services.program_import import (
    ProgramImportClient,
    ProgramImportService,
    program_import_run_response,
)

router = APIRouter(prefix="/programs", tags=["programs"])


@router.post(
    "/import/nulp/bachelor",
    response_model=ProgramImportRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def import_nulp_bachelor_programs(
    response: Response,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    blob_storage: Annotated[BlobStorage, Depends(get_blob_storage)],
    import_client: Annotated[ProgramImportClient, Depends(get_program_import_client)],
    processing_scheduler: Annotated[
        DocumentProcessingScheduler,
        Depends(get_document_processing_scheduler),
    ],
) -> ProgramImportRunResponse:
    run = await ProgramImportService(
        db,
        blob_storage=blob_storage,
        client=import_client,
    ).import_nulp_bachelor_programs(triggered_by=current_admin)
    document_ids = getattr(run, "scheduled_document_ids", [])
    for index in range(0, len(document_ids), 5):
        processing_scheduler.schedule_documents(document_ids[index : index + 5])
    response.status_code = status.HTTP_202_ACCEPTED
    return program_import_run_response(run)


@router.get("", response_model=EducationProgramListResponse)
async def list_programs(
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    blob_storage: Annotated[BlobStorage, Depends(get_blob_storage)],
    institution_code: Annotated[str | None, Query(min_length=1, max_length=20)] = None,
    department_link_status: Annotated[str | None, Query(min_length=1, max_length=50)] = None,
    search: Annotated[str | None, Query(min_length=1, max_length=500)] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> EducationProgramListResponse:
    del current_admin
    return await ProgramImportService(db, blob_storage=blob_storage).list_programs(
        institution_code=institution_code,
        department_link_status=department_link_status,
        search=search,
        limit=limit,
        offset=offset,
    )


@router.get("/{program_id}", response_model=EducationProgramDetailResponse)
async def get_program(
    program_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    blob_storage: Annotated[BlobStorage, Depends(get_blob_storage)],
) -> EducationProgramDetailResponse:
    del current_admin
    return await ProgramImportService(db, blob_storage=blob_storage).get_program(
        program_id=program_id,
    )


@router.patch("/{program_id}", response_model=EducationProgramResponse)
async def update_program(
    program_id: UUID,
    request: ProgramUpdateRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    blob_storage: Annotated[BlobStorage, Depends(get_blob_storage)],
) -> EducationProgramResponse:
    del current_admin
    return await ProgramImportService(db, blob_storage=blob_storage).update_program(
        program_id=program_id,
        request=request,
    )


@router.patch("/{program_id}/department", response_model=EducationProgramResponse)
async def update_program_department(
    program_id: UUID,
    request: ProgramDepartmentUpdateRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    blob_storage: Annotated[BlobStorage, Depends(get_blob_storage)],
) -> EducationProgramResponse:
    del current_admin
    return await ProgramImportService(db, blob_storage=blob_storage).update_department(
        program_id=program_id,
        department_id=request.department_id,
    )


@router.patch("/{program_id}/documents/{document_link_id}", response_model=ProgramDocumentResponse)
async def update_program_document(
    program_id: UUID,
    document_link_id: UUID,
    request: ProgramDocumentUpdateRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    blob_storage: Annotated[BlobStorage, Depends(get_blob_storage)],
) -> ProgramDocumentResponse:
    del current_admin
    return await ProgramImportService(db, blob_storage=blob_storage).update_program_document(
        program_id=program_id,
        document_id=document_link_id,
        request=request,
    )


@router.delete("/{program_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_program(
    program_id: UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    blob_storage: Annotated[BlobStorage, Depends(get_blob_storage)],
) -> None:
    del current_admin
    await ProgramImportService(db, blob_storage=blob_storage).delete_program(program_id=program_id)
