import hashlib
import re
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field as dataclass_field
from pathlib import PurePosixPath
from typing import Protocol
from urllib.parse import urljoin, urlparse

import structlog
from bs4 import BeautifulSoup
from fastapi import HTTPException, status
from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import utcnow
from src.core.settings import Settings, get_settings
from src.models.document import DocumentProcessingStatus, UploadedDocument
from src.models.institution import Department, Institution
from src.models.program import (
    DepartmentLinkStatus,
    EducationProgram,
    EducationProgramLevel,
    ProgramDirectorySnapshot,
    ProgramDocument,
    ProgramDocumentImportStatus,
    ProgramDocumentKind,
    ProgramImportRun,
    ProgramImportRunStatus,
)
from src.models.user import User
from src.repositories.document import UploadedDocumentRepository
from src.schemas.program import (
    EducationProgramDetailResponse,
    EducationProgramListResponse,
    EducationProgramResponse,
    ProgramDirectorySnapshotResponse,
    ProgramDocumentUpdateRequest,
    ProgramDocumentResponse,
    ProgramImportRunResponse,
    ProgramUpdateRequest,
)
from src.services.blob_storage import BlobStorage
from src.services.document_parsing import DocumentParser
from src.services.document_upload import (
    sanitize_filename,
    validate_content,
    validate_upload_metadata,
)

BACHELOR_PROGRAM_SOURCE_URL = (
    "https://lpnu.ua/osvita/pro-osvitni-programy/pershyi-riven-vyshchoi-osvity"
)
REMOTE_PROGRAM_FILE_MAX_BYTES = 50 * 1024 * 1024
PROGRAM_IMPORT_PROCESSING_BATCH_SIZE = 5

logger = structlog.get_logger(__name__)

INSTITUTION_SLUG_TO_CODE = {
    "iadu": "ІАДУ",
    "iard": "ІАРД",
    "ibib": "ІБІБ",
    "igdg": "ІГДГ",
    "igsn": "ІГСН",
    "inem": "ІНЕМ",
    "iesk": "ІЕСК",
    "ikte": "ІКТЕ",
    "ikni": "ІКНІ",
    "ikta": "ІКТА",
    "imit": "ІМІТ",
    "ipmt": "ІПМТ",
    "ippt": "ІППТ",
    "ippo": "ІППО",
    "imfn": "ІМФН",
    "istr": "ІСТР",
    "ixxt": "ІХХТ",
    "miok": "МІОК",
}


@dataclass(frozen=True)
class ProgramDocumentCandidate:
    source_url: str
    title: str
    kind: str
    source_size_label: str | None = None
    source_size_bytes: int | None = None


@dataclass(frozen=True)
class BachelorProgramCandidate:
    field_code: str
    field_name: str
    specialty_code: str
    specialty_name: str
    program_name: str
    program_url: str | None
    source_page_url: str
    documents: list[ProgramDocumentCandidate] = dataclass_field(default_factory=list)


@dataclass(frozen=True)
class DirectoryProgramMetadata:
    qualification: str | None = None
    admission_year: str | None = None
    institution_text: str | None = None
    study_form: str | None = None
    duration: str | None = None
    credits: str | None = None
    field: str | None = None
    manager: str | None = None
    program_url: str | None = None
    raw_text: str = ""
    structured: dict = dataclass_field(default_factory=dict)
    sections: list[dict] = dataclass_field(default_factory=list)


@dataclass(frozen=True)
class DownloadedRemoteFile:
    content: bytes
    content_type: str | None = None


@dataclass(frozen=True)
class DepartmentMatchResult:
    department: Department | None
    status: str
    confidence: float | None


class RemoteFileTooLargeError(RuntimeError):
    def __init__(self, *, size_bytes: int | None, max_bytes: int) -> None:
        super().__init__("Remote file exceeds configured size limit.")
        self.size_bytes = size_bytes
        self.max_bytes = max_bytes


class ProgramImportClient(Protocol):
    async def get_text(self, url: str) -> str: ...

    async def get_file(self, url: str, *, max_bytes: int) -> DownloadedRemoteFile: ...


class HttpxProgramImportClient:
    async def get_text(self, url: str) -> str:
        import httpx

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text

    async def get_file(self, url: str, *, max_bytes: int) -> DownloadedRemoteFile:
        import httpx

        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                content_length = response.headers.get("content-length")
                if content_length and int(content_length) > max_bytes:
                    raise RemoteFileTooLargeError(
                        size_bytes=int(content_length),
                        max_bytes=max_bytes,
                    )
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > max_bytes:
                        raise RemoteFileTooLargeError(size_bytes=size, max_bytes=max_bytes)
                    chunks.append(chunk)
                return DownloadedRemoteFile(
                    content=b"".join(chunks),
                    content_type=response.headers.get("content-type"),
                )


class ProgramImportService:
    def __init__(
        self,
        session: AsyncSession,
        *,
        blob_storage: BlobStorage,
        client: ProgramImportClient | None = None,
        parser: DocumentParser | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.session = session
        self.blob_storage = blob_storage
        self.client = client or HttpxProgramImportClient()
        self.parser = parser or DocumentParser()
        self.settings = settings or get_settings()
        self.uploaded_documents = UploadedDocumentRepository(session)

    async def import_nulp_bachelor_programs(self, *, triggered_by: User) -> ProgramImportRun:
        started = time.perf_counter()
        source_html = await self.client.get_text(BACHELOR_PROGRAM_SOURCE_URL)
        candidates = parse_bachelor_programs_html(
            source_html,
            source_url=BACHELOR_PROGRAM_SOURCE_URL,
        )
        candidate = await self._next_unimported_bachelor_candidate(candidates)
        institutions = (await self.session.execute(select(Institution))).scalars().all()
        departments = (await self.session.execute(select(Department))).scalars().all()
        department_matches_by_program_id: dict[uuid.UUID, set[uuid.UUID]] = {}
        scheduled_document_ids: list[uuid.UUID] = []
        imported_programs: list[EducationProgram] = []
        created_document_count = 0
        oversized_document_count = 0
        failed_document_count = 0

        if candidate is not None:
            metadata = await self._load_directory_metadata(candidate.program_url)
            institution = match_institution(
                metadata.institution_text,
                program_url=candidate.program_url,
                institutions=institutions,
            )
            program = await self._upsert_program(candidate, metadata, institution)
            await self._upsert_directory_snapshot(program, metadata)
            imported_programs.append(program)
            candidate_documents = deduplicate_document_candidates(candidate.documents)
            for document_candidate in candidate_documents:
                document = await self._upsert_program_document(program, document_candidate)
                if document.uploaded_document_id is not None:
                    continue
                if document.import_status in {
                    ProgramDocumentImportStatus.OVERSIZED,
                    ProgramDocumentImportStatus.FAILED,
                    ProgramDocumentImportStatus.DOWNLOADED,
                    ProgramDocumentImportStatus.PROCESSED,
                }:
                    continue
                try:
                    uploaded_document = await self._download_store_program_document(
                        program=program,
                        program_document=document,
                        triggered_by=triggered_by,
                    )
                except RemoteFileTooLargeError as exc:
                    oversized_document_count += 1
                    document.import_status = ProgramDocumentImportStatus.OVERSIZED
                    document.source_size_bytes = exc.size_bytes
                    document.import_error = None
                    continue
                except Exception as exc:
                    failed_document_count += 1
                    document.import_status = ProgramDocumentImportStatus.FAILED
                    document.import_error = safe_import_error(exc)
                    logger.exception(
                        "program_import.document_failed",
                        source_url=document.source_url,
                        program_id=str(program.id),
                        error_type=exc.__class__.__name__,
                    )
                    continue

                created_document_count += 1
                scheduled_document_ids.append(uploaded_document.id)
                document.uploaded_document_id = uploaded_document.id
                document.import_status = ProgramDocumentImportStatus.DOWNLOADED
                if document.kind == ProgramDocumentKind.OPP:
                    matched_department_ids = await self._department_ids_from_opp(
                        program=program,
                        uploaded_document=uploaded_document,
                        content=await self.blob_storage.get_document_content(
                            uploaded_document.storage_key or ""
                        ),
                        departments=departments,
                    )
                    department_matches_by_program_id.setdefault(program.id, set()).update(
                        matched_department_ids
                    )

            await self.session.flush()
            self._apply_department_match(
                program,
                matched_department_ids=department_matches_by_program_id.get(program.id, set()),
                departments=departments,
            )

        matched_program_count = sum(
            1
            for program in imported_programs
            if program.department_link_status == DepartmentLinkStatus.MATCHED
        )
        pending_program_count = sum(
            1
            for program in imported_programs
            if program.department_link_status == DepartmentLinkStatus.PENDING_REVIEW
        )
        duration_ms = int((time.perf_counter() - started) * 1000)
        run = ProgramImportRun(
            source_url=BACHELOR_PROGRAM_SOURCE_URL,
            status=ProgramImportRunStatus.COMPLETED,
            program_count=len(imported_programs),
            created_document_count=created_document_count,
            oversized_document_count=oversized_document_count,
            failed_document_count=failed_document_count,
            matched_program_count=matched_program_count,
            pending_review_program_count=pending_program_count,
            duration_ms=duration_ms,
        )
        self.session.add(run)
        await self.session.commit()
        await self.session.refresh(run)
        run.scheduled_document_ids = scheduled_document_ids  # type: ignore[attr-defined]
        return run

    async def _next_unimported_bachelor_candidate(
        self,
        candidates: Sequence[BachelorProgramCandidate],
    ) -> BachelorProgramCandidate | None:
        for candidate in candidates:
            existing = await self.session.scalar(
                select(EducationProgram).where(
                    EducationProgram.level == EducationProgramLevel.BACHELOR,
                    EducationProgram.specialty_code == candidate.specialty_code,
                    EducationProgram.program_name == candidate.program_name,
                )
            )
            if existing is None:
                return candidate
            if existing.deleted_at is not None:
                continue
            candidate_documents = deduplicate_document_candidates(candidate.documents)
            if not candidate_documents:
                continue
            result = await self.session.execute(
                select(
                    ProgramDocument.source_url,
                    ProgramDocument.import_status,
                    ProgramDocument.uploaded_document_id,
                ).where(ProgramDocument.program_id == existing.id)
            )
            existing_documents = {
                source_url: (import_status, uploaded_document_id)
                for source_url, import_status, uploaded_document_id in result.all()
            }
            if not all(
                is_terminal_program_document(existing_documents.get(document.source_url))
                for document in candidate_documents
            ):
                return candidate
        return None

    async def list_programs(
        self,
        *,
        institution_code: str | None,
        department_link_status: str | None,
        search: str | None,
        limit: int,
        offset: int,
    ) -> EducationProgramListResponse:
        filters = []
        if institution_code:
            filters.append(Institution.code == institution_code)
        if department_link_status:
            filters.append(EducationProgram.department_link_status == department_link_status)
        if search:
            pattern = f"%{search.strip()}%"
            filters.append(
                or_(
                    EducationProgram.program_name.ilike(pattern),
                    EducationProgram.specialty_name.ilike(pattern),
                    EducationProgram.field_name.ilike(pattern),
                    cast(EducationProgram.id, String).ilike(pattern),
                )
            )
        total = await self.session.scalar(
            select(func.count())
            .select_from(EducationProgram)
            .outerjoin(Institution, Institution.id == EducationProgram.institution_id)
            .where(EducationProgram.deleted_at.is_(None), *filters)
        )
        result = await self.session.execute(
            select(EducationProgram, Institution, Department)
            .outerjoin(Institution, Institution.id == EducationProgram.institution_id)
            .outerjoin(Department, Department.id == EducationProgram.department_id)
            .where(EducationProgram.deleted_at.is_(None), *filters)
            .order_by(EducationProgram.specialty_code.asc(), EducationProgram.program_name.asc())
            .limit(limit)
            .offset(offset)
        )
        programs = result.all()
        stats = await self._document_stats_by_program([program.id for program, _, _ in programs])
        return EducationProgramListResponse(
            items=[
                program_response(
                    program,
                    institution=institution,
                    department=department,
                    stats=stats.get(program.id, {}),
                )
                for program, institution, department in programs
            ],
            total=total or 0,
            limit=limit,
            offset=offset,
        )

    async def get_program(self, *, program_id: uuid.UUID) -> EducationProgramDetailResponse:
        result = await self.session.execute(
            select(EducationProgram, Institution, Department)
            .outerjoin(Institution, Institution.id == EducationProgram.institution_id)
            .outerjoin(Department, Department.id == EducationProgram.department_id)
            .where(EducationProgram.id == program_id, EducationProgram.deleted_at.is_(None))
        )
        row = result.one_or_none()
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Program was not found.")
        program, institution, department = row
        stats = await self._document_stats_by_program([program.id])
        documents = await self._program_document_responses(program.id)
        directory_snapshot = await self._directory_snapshot_response(program.id)
        response = program_response(
            program,
            institution=institution,
            department=department,
            stats=stats.get(program.id, {}),
        )
        return EducationProgramDetailResponse(
            **response.model_dump(),
            documents=documents,
            directory_snapshot=directory_snapshot,
        )

    async def update_department(
        self,
        *,
        program_id: uuid.UUID,
        department_id: uuid.UUID,
    ) -> EducationProgramResponse:
        program = await self.session.get(EducationProgram, program_id)
        if program is None or program.deleted_at is not None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Program was not found.")
        department = await self.session.get(Department, department_id)
        if department is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Department was not found.",
            )
        program.institution_id = department.institution_id
        program.department_id = department.id
        program.department_link_status = DepartmentLinkStatus.MANUAL
        program.department_match_confidence = 1.0
        await self.session.commit()
        detail = await self.get_program(program_id=program_id)
        return EducationProgramResponse(**detail.model_dump(exclude={"documents", "directory_snapshot"}))

    async def update_program(
        self,
        *,
        program_id: uuid.UUID,
        request: ProgramUpdateRequest,
    ) -> EducationProgramResponse:
        program = await self.session.get(EducationProgram, program_id)
        if program is None or program.deleted_at is not None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Program was not found.")
        updated_fields = request.model_fields_set
        nullable_text_fields = {
            "qualification",
            "study_form",
            "duration",
            "credits",
            "manager",
            "program_url",
        }
        for field_name in (
            "field_code",
            "field_name",
            "specialty_code",
            "specialty_name",
            "program_name",
            "qualification",
            "study_form",
            "duration",
            "credits",
            "manager",
            "program_url",
            "source_page_url",
        ):
            if field_name not in updated_fields:
                continue
            value = getattr(request, field_name)
            if isinstance(value, str):
                value = value.strip()
            if not value and field_name not in nullable_text_fields:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"{field_name} cannot be empty.",
                )
            setattr(program, field_name, value or None)
        if "institution_id" in updated_fields:
            if request.institution_id is None:
                program.institution_id = None
                program.department_id = None
                program.department_link_status = DepartmentLinkStatus.PENDING_REVIEW
                program.department_match_confidence = None
            else:
                institution = await self.session.get(Institution, request.institution_id)
                if institution is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Institution was not found.",
                    )
                program.institution_id = institution.id
        if "department_id" in updated_fields:
            if request.department_id is None:
                program.department_id = None
                program.department_link_status = DepartmentLinkStatus.PENDING_REVIEW
                program.department_match_confidence = None
            else:
                department = await self.session.get(Department, request.department_id)
                if department is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Department was not found.",
                    )
                program.institution_id = department.institution_id
                program.department_id = department.id
                program.department_link_status = DepartmentLinkStatus.MANUAL
                program.department_match_confidence = 1.0
        await self.session.commit()
        detail = await self.get_program(program_id=program_id)
        return EducationProgramResponse(**detail.model_dump(exclude={"documents", "directory_snapshot"}))

    async def update_program_document(
        self,
        *,
        program_id: uuid.UUID,
        document_id: uuid.UUID,
        request: ProgramDocumentUpdateRequest,
    ) -> ProgramDocumentResponse:
        program = await self.session.get(EducationProgram, program_id)
        if program is None or program.deleted_at is not None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Program was not found.")
        document = await self.session.scalar(
            select(ProgramDocument).where(
                ProgramDocument.id == document_id,
                ProgramDocument.program_id == program_id,
            )
        )
        if document is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Program document was not found.",
            )
        if request.title is not None:
            document.title = request.title.strip()
        if request.kind is not None:
            try:
                document.kind = ProgramDocumentKind(request.kind)
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Invalid program document kind.",
                ) from exc
        if request.import_status is not None:
            try:
                document.import_status = ProgramDocumentImportStatus(request.import_status)
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Invalid program document import status.",
                ) from exc
        if request.import_error is not None:
            document.import_error = request.import_error.strip() or None
        await self.session.commit()
        responses = await self._program_document_responses(program_id)
        return next(response for response in responses if response.id == document_id)

    async def delete_program(self, *, program_id: uuid.UUID) -> None:
        program = await self.session.get(EducationProgram, program_id)
        if program is None or program.deleted_at is not None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Program was not found.")
        program.deleted_at = utcnow()
        await self.session.commit()

    async def _load_directory_metadata(self, program_url: str | None) -> DirectoryProgramMetadata:
        if not program_url:
            return DirectoryProgramMetadata()
        try:
            html = await self.client.get_text(program_url)
        except Exception:
            logger.exception("program_import.directory_failed", program_url=program_url)
            return DirectoryProgramMetadata(program_url=program_url)
        return parse_directory_program_metadata(html, program_url=program_url)

    async def _upsert_program(
        self,
        candidate: BachelorProgramCandidate,
        metadata: DirectoryProgramMetadata,
        institution: Institution | None,
    ) -> EducationProgram:
        existing = await self.session.scalar(
            select(EducationProgram).where(
                EducationProgram.level == EducationProgramLevel.BACHELOR,
                EducationProgram.specialty_code == candidate.specialty_code,
                EducationProgram.program_name == candidate.program_name,
            )
        )
        if existing is None:
            existing = EducationProgram(
                level=EducationProgramLevel.BACHELOR,
                field_code=candidate.field_code,
                field_name=candidate.field_name,
                specialty_code=candidate.specialty_code,
                specialty_name=candidate.specialty_name,
                program_name=candidate.program_name,
                source_page_url=candidate.source_page_url,
                department_link_status=DepartmentLinkStatus.PENDING_REVIEW,
            )
            self.session.add(existing)
        existing.field_code = candidate.field_code
        existing.field_name = candidate.field_name
        existing.specialty_name = candidate.specialty_name
        existing.qualification = metadata.qualification
        existing.study_form = metadata.study_form
        existing.duration = metadata.duration
        existing.credits = metadata.credits
        existing.manager = metadata.manager
        existing.program_url = metadata.program_url or candidate.program_url
        existing.source_page_url = candidate.source_page_url
        existing.institution_id = institution.id if institution else None
        await self.session.flush()
        return existing

    async def _upsert_directory_snapshot(
        self,
        program: EducationProgram,
        metadata: DirectoryProgramMetadata,
    ) -> None:
        if not metadata.program_url or not metadata.raw_text:
            return
        existing = await self.session.scalar(
            select(ProgramDirectorySnapshot).where(
                ProgramDirectorySnapshot.program_id == program.id,
                ProgramDirectorySnapshot.source_url == metadata.program_url,
            )
        )
        if existing is None:
            existing = ProgramDirectorySnapshot(
                program_id=program.id,
                source_url=metadata.program_url,
                year=directory_url_year(metadata.program_url, metadata.admission_year or ""),
                raw_text=metadata.raw_text,
                structured_json=metadata.structured,
                sections_json=metadata.sections,
                parsed_at=utcnow(),
            )
            self.session.add(existing)
            return
        existing.year = directory_url_year(metadata.program_url, metadata.admission_year or "")
        existing.raw_text = metadata.raw_text
        existing.structured_json = metadata.structured
        existing.sections_json = metadata.sections
        existing.parsed_at = utcnow()

    async def _upsert_program_document(
        self,
        program: EducationProgram,
        candidate: ProgramDocumentCandidate,
    ) -> ProgramDocument:
        existing = await self.session.scalar(
            select(ProgramDocument).where(
                ProgramDocument.program_id == program.id,
                ProgramDocument.source_url == candidate.source_url,
            )
        )
        if existing is None:
            existing = ProgramDocument(
                program_id=program.id,
                source_url=candidate.source_url,
                title=candidate.title,
                kind=ProgramDocumentKind(candidate.kind),
                source_size_label=candidate.source_size_label,
                source_size_bytes=candidate.source_size_bytes,
                import_status=ProgramDocumentImportStatus.QUEUED,
            )
            self.session.add(existing)
            await self.session.flush()
            return existing
        existing.title = candidate.title
        existing.kind = ProgramDocumentKind(candidate.kind)
        existing.source_size_label = candidate.source_size_label
        existing.source_size_bytes = candidate.source_size_bytes
        return existing

    async def _download_store_program_document(
        self,
        *,
        program: EducationProgram,
        program_document: ProgramDocument,
        triggered_by: User,
    ) -> UploadedDocument:
        if (
            program_document.source_size_bytes is not None
            and program_document.source_size_bytes > REMOTE_PROGRAM_FILE_MAX_BYTES
        ):
            raise RemoteFileTooLargeError(
                size_bytes=program_document.source_size_bytes,
                max_bytes=REMOTE_PROGRAM_FILE_MAX_BYTES,
            )
        remote_file = await self.client.get_file(
            program_document.source_url,
            max_bytes=REMOTE_PROGRAM_FILE_MAX_BYTES,
        )
        original_filename = filename_from_url(program_document.source_url, program_document.title)
        validated = validate_upload_metadata(
            original_filename=original_filename,
            content_type=None,
            size_bytes=len(remote_file.content),
            sha256_hash=None,
        )
        validate_content(extension=f".{validated.file_extension}", content=remote_file.content)
        document_id = uuid.uuid4()
        uploaded_at = utcnow()
        stored_blob = await self.blob_storage.put_document(
            user_id=triggered_by.id,
            document_id=document_id,
            safe_filename=validated.safe_filename,
            content=remote_file.content,
            content_type=validated.content_type,
            uploaded_at=uploaded_at,
        )
        return await self.uploaded_documents.create(
            document_id=document_id,
            original_filename=validated.original_filename,
            safe_filename=validated.safe_filename,
            content_type=validated.content_type,
            file_extension=validated.file_extension,
            size_bytes=validated.size_bytes,
            sha256_hash=hashlib.sha256(remote_file.content).hexdigest(),
            storage_key=stored_blob.pathname,
            uploaded_by_id=triggered_by.id,
            processing_status=DocumentProcessingStatus.QUEUED,
        )

    async def _department_ids_from_opp(
        self,
        *,
        program: EducationProgram,
        uploaded_document: UploadedDocument,
        content: bytes,
        departments: Sequence[Department],
    ) -> set[uuid.UUID]:
        if program.institution_id is None:
            return set()
        scoped_departments = [
            department for department in departments if department.institution_id == program.institution_id
        ]
        try:
            parsed = await self.parser.parse(
                filename=uploaded_document.safe_filename,
                file_extension=uploaded_document.file_extension,
                content=content,
            )
            text = parsed.text
        except Exception:
            text = ""
        text = f"{text}\n{content.decode('utf-8', errors='ignore')}"
        match = match_department_for_program(text, departments=scoped_departments)
        return {match.department.id} if match.department is not None else set()

    def _apply_department_match(
        self,
        program: EducationProgram,
        *,
        matched_department_ids: set[uuid.UUID],
        departments: Sequence[Department],
    ) -> None:
        if program.department_link_status == DepartmentLinkStatus.MANUAL:
            return
        if not matched_department_ids and program.department_id is not None:
            return
        if len(matched_department_ids) != 1:
            program.department_id = None
            program.department_link_status = DepartmentLinkStatus.PENDING_REVIEW
            program.department_match_confidence = None
            return
        department_id = next(iter(matched_department_ids))
        department = next((item for item in departments if item.id == department_id), None)
        if department is None:
            return
        program.department_id = department.id
        program.department_link_status = DepartmentLinkStatus.MATCHED
        program.department_match_confidence = 1.0

    async def _program_link_counts(self) -> tuple[int, int]:
        matched = await self.session.scalar(
            select(func.count())
            .select_from(EducationProgram)
            .where(EducationProgram.department_link_status == DepartmentLinkStatus.MATCHED)
        )
        pending = await self.session.scalar(
            select(func.count())
            .select_from(EducationProgram)
            .where(EducationProgram.department_link_status == DepartmentLinkStatus.PENDING_REVIEW)
        )
        return matched or 0, pending or 0

    async def _document_stats_by_program(
        self,
        program_ids: Sequence[uuid.UUID],
    ) -> dict[uuid.UUID, dict[str, int]]:
        if not program_ids:
            return {}
        result = await self.session.execute(
            select(ProgramDocument.program_id, ProgramDocument.import_status, func.count())
            .where(ProgramDocument.program_id.in_(program_ids))
            .group_by(ProgramDocument.program_id, ProgramDocument.import_status)
        )
        stats: dict[uuid.UUID, dict[str, int]] = {}
        for program_id, import_status, count in result.all():
            program_stats = stats.setdefault(
                program_id,
                {
                    "document_count": 0,
                    "downloaded_document_count": 0,
                    "oversized_document_count": 0,
                    "failed_document_count": 0,
                },
            )
            program_stats["document_count"] += count
            if import_status == ProgramDocumentImportStatus.DOWNLOADED:
                program_stats["downloaded_document_count"] += count
            elif import_status == ProgramDocumentImportStatus.OVERSIZED:
                program_stats["oversized_document_count"] += count
            elif import_status == ProgramDocumentImportStatus.FAILED:
                program_stats["failed_document_count"] += count
        return stats

    async def _program_document_responses(self, program_id: uuid.UUID) -> list[ProgramDocumentResponse]:
        result = await self.session.execute(
            select(ProgramDocument, UploadedDocument)
            .outerjoin(UploadedDocument, UploadedDocument.id == ProgramDocument.uploaded_document_id)
            .where(ProgramDocument.program_id == program_id)
            .order_by(ProgramDocument.kind.asc(), ProgramDocument.title.asc())
        )
        return [
            ProgramDocumentResponse(
                id=document.id,
                program_id=document.program_id,
                uploaded_document_id=document.uploaded_document_id,
                source_url=document.source_url,
                title=document.title,
                kind=document.kind,
                source_size_label=document.source_size_label,
                source_size_bytes=document.source_size_bytes,
                import_status=document.import_status,
                import_error=document.import_error,
                document_filename=uploaded.safe_filename if uploaded else None,
                processing_status=uploaded.processing_status if uploaded else None,
                created_at=document.created_at,
                updated_at=document.updated_at,
            )
            for document, uploaded in result.all()
        ]

    async def _directory_snapshot_response(
        self,
        program_id: uuid.UUID,
    ) -> ProgramDirectorySnapshotResponse | None:
        snapshot = await self.session.scalar(
            select(ProgramDirectorySnapshot)
            .where(ProgramDirectorySnapshot.program_id == program_id)
            .order_by(
                ProgramDirectorySnapshot.year.desc().nullslast(),
                ProgramDirectorySnapshot.parsed_at.desc(),
            )
            .limit(1)
        )
        if snapshot is None:
            return None
        return ProgramDirectorySnapshotResponse(
            id=snapshot.id,
            program_id=snapshot.program_id,
            source_url=snapshot.source_url,
            year=snapshot.year,
            raw_text=snapshot.raw_text,
            structured_json=snapshot.structured_json or {},
            sections_json=snapshot.sections_json or [],
            parsed_at=snapshot.parsed_at,
            created_at=snapshot.created_at,
            updated_at=snapshot.updated_at,
        )


def parse_bachelor_programs_html(
    html: str,
    *,
    source_url: str,
) -> list[BachelorProgramCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    current_field_code = ""
    current_field_name = ""
    programs: list[BachelorProgramCandidate] = []
    tables = soup.select("table")
    row_containers = tables or [soup]
    for container in row_containers:
        caption = container.find("caption") if container.name == "table" else None
        caption_field = parse_field_label(caption.get_text(" ", strip=True)) if caption else None
        if caption_field is not None:
            current_field_code, current_field_name = caption_field
        for row in container.select("tr"):
            cells = row.find_all(["td", "th"])
            row_text = normalize_spaces(row.get_text(" ", strip=True))
            field_match = parse_field_label(row_text)
            if field_match:
                current_field_code, current_field_name = field_match
                continue
            if len(cells) < 2 or not current_field_code:
                continue
            specialty = parse_specialty_cell(cells[0].get_text(" ", strip=True))
            if specialty is None:
                continue
            program_name = strip_quotes(cells[1].get_text(" ", strip=True))
            if not program_name:
                continue
            directory_url = first_directory_url(row, source_url=source_url)
            documents = linked_document_candidates(row, source_url=source_url)
            programs.append(
                BachelorProgramCandidate(
                    field_code=current_field_code,
                    field_name=current_field_name,
                    specialty_code=specialty[0],
                    specialty_name=specialty[1],
                    program_name=program_name,
                    program_url=directory_url,
                    source_page_url=source_url,
                    documents=documents,
                )
            )
    return programs


def parse_field_label(value: str) -> tuple[str, str] | None:
    normalized = normalize_spaces(value)
    field_match = re.search(r"Галузь знань:?\s*([0-9A-ZА-ЯІЇЄҐ]+)\s+(.+)", normalized)
    if field_match:
        return field_match.group(1).strip(), strip_quotes(field_match.group(2).strip())
    return None


def parse_directory_program_metadata(
    html: str,
    *,
    program_url: str,
) -> DirectoryProgramMetadata:
    soup = BeautifulSoup(html, "html.parser")
    pairs: dict[str, str] = {}
    for term in soup.find_all("dt"):
        definition = term.find_next_sibling("dd")
        if definition is not None:
            pairs[normalize_label(term.get_text(" ", strip=True))] = normalize_spaces(
                definition.get_text(" ", strip=True)
            )
    if not pairs:
        text = soup.get_text("\n", strip=True)
        lines = [normalize_spaces(line) for line in text.splitlines() if normalize_spaces(line)]
        metadata_labels = (
            "Кваліфікація",
            "Інститут",
            "Форма навчання",
            "Тривалість програми",
            "Строк навчання",
            "Кількість кредитів",
            "Обсяг програми",
            "Керівник освітньої програми",
            "Гарант",
            "Керівник освітньої програми, контактна особа",
        )
        for line in lines:
            normalized_line = normalize_label(line)
            for label in metadata_labels:
                normalized_label = normalize_label(label)
                if normalized_line.startswith(f"{normalized_label}:"):
                    pairs.setdefault(normalized_label, normalize_spaces(line.split(":", 1)[1]))
                    break
        for index, line in enumerate(lines[:-1]):
            if line.endswith(":"):
                pairs.setdefault(normalize_label(line[:-1]), lines[index + 1])
    raw_text = normalize_directory_text(soup)
    structured = directory_structured_values(raw_text, pairs)
    sections = directory_sections(raw_text)
    return DirectoryProgramMetadata(
        qualification=first_labeled_value(pairs, "кваліфікація"),
        admission_year=first_labeled_value(pairs, "рік вступу"),
        institution_text=first_labeled_value(pairs, "інститут"),
        study_form=first_labeled_value(pairs, "форма навчання"),
        duration=first_labeled_value(pairs, "тривалість програми", "строк навчання"),
        credits=first_labeled_value(pairs, "кількість кредитів", "обсяг програми"),
        field=first_labeled_value(pairs, "галузь знань"),
        manager=first_labeled_value(
            pairs,
            "керівник освітньої програми",
            "керівник освітньої програми, контактна особа",
            "гарант",
        ),
        program_url=program_url,
        raw_text=raw_text,
        structured=structured,
        sections=sections,
    )


def parse_specialty_cell(value: str) -> tuple[str, str] | None:
    normalized = normalize_spaces(value)
    match = re.match(r"([0-9A-ZА-ЯІЇЄҐ.]+)\s+[«\"]?(.+?)[»\"]?$", normalized)
    if not match:
        return None
    return match.group(1), strip_quotes(match.group(2))


DIRECTORY_SECTION_LABELS = (
    "Рівень кваліфікації відповідно до Національної рамки кваліфікацій, Європейської рамки кваліфікацій для навчання впродовж життя",
    "Особливі умови вступу",
    "Конкретні механізми визнання попереднього навчання",
    "Вимоги та правила щодо отримання кваліфікації, вимоги щодо виконання навчальної програми",
    "Характеристика освітньої програми",
    "Програмні результати навчання",
    "Академічна мобільність",
    "Практика/стажування",
    "Професійні профілі випускників",
    "Доступ до подальшого навчання",
    "Інші особливості програми",
    "Зауваження та пропозиції стейкхолдерів",
    "Освітні компоненти",
)


def normalize_directory_text(soup: BeautifulSoup) -> str:
    for element in soup.select("script, style, noscript"):
        element.decompose()
    lines = [normalize_spaces(line) for line in soup.get_text("\n", strip=True).splitlines()]
    return "\n".join(line for line in lines if line)


def directory_structured_values(raw_text: str, pairs: dict[str, str]) -> dict:
    structured = dict(pairs)
    for section in directory_sections(raw_text):
        label = normalize_label(section["title"])
        if section["body"]:
            structured.setdefault(label, section["body"])
    components = education_components(raw_text)
    if components:
        structured["освітні компоненти"] = components
    return structured


def directory_sections(raw_text: str) -> list[dict]:
    lines = [line for line in raw_text.splitlines() if line.strip()]
    sections: list[dict] = []
    current_title: str | None = None
    current_lines: list[str] = []
    labels = {normalize_label(label): label for label in DIRECTORY_SECTION_LABELS}
    for line in lines:
        inline_title, inline_body = split_directory_label_line(line, labels)
        if inline_title is not None:
            if current_title is not None:
                sections.append({"title": current_title, "body": "\n".join(current_lines).strip()})
            current_title = inline_title
            current_lines = [inline_body] if inline_body else []
            continue
        if current_title is not None:
            current_lines.append(line)
    if current_title is not None:
        sections.append({"title": current_title, "body": "\n".join(current_lines).strip()})
    return sections


def split_directory_label_line(
    line: str,
    labels: dict[str, str],
) -> tuple[str | None, str]:
    normalized_line = normalize_label(line)
    for normalized_label, original_label in labels.items():
        if normalized_line == normalized_label:
            return original_label, ""
        if normalized_line.startswith(f"{normalized_label}:"):
            return original_label, normalize_spaces(line.split(":", 1)[1])
    return None, ""


def education_components(raw_text: str) -> list[dict]:
    marker = "Освітні компоненти"
    if marker not in raw_text:
        return []
    lines = raw_text.split(marker, 1)[1].splitlines()
    semesters: list[dict] = []
    current: dict | None = None
    current_group = "items"
    for line in lines:
        value = normalize_spaces(line)
        if not value:
            continue
        if re.match(r"^\d+\s+семестр$", value, re.IGNORECASE):
            current = {"semester": value, "groups": {}}
            semesters.append(current)
            current_group = "items"
            continue
        if current is None:
            continue
        if "дисципл" in value.lower() or value.lower().startswith("вибірков"):
            current_group = value
            current["groups"].setdefault(current_group, [])
            continue
        current["groups"].setdefault(current_group, []).append(value)
    return semesters


def linked_document_candidates(row, *, source_url: str) -> list[ProgramDocumentCandidate]:
    candidates = []
    for link in row.find_all("a", href=True):
        href = str(link["href"]).strip()
        absolute_url = urljoin(source_url, href)
        if is_directory_url(absolute_url):
            continue
        if not looks_like_downloadable_url(absolute_url):
            continue
        title = normalize_spaces(link.get_text(" ", strip=True)) or filename_from_url(absolute_url, "file")
        size_label = link_size_label(link)
        candidates.append(
            ProgramDocumentCandidate(
                source_url=absolute_url,
                title=title,
                kind=classify_program_document_kind(title, absolute_url),
                source_size_label=size_label,
                source_size_bytes=size_label_to_bytes(size_label),
            )
        )
    return candidates


def first_directory_url(row, *, source_url: str) -> str | None:
    directory_urls: list[tuple[int, int, str]] = []
    for link in row.find_all("a", href=True):
        absolute_url = urljoin(source_url, str(link["href"]).strip())
        if is_directory_url(absolute_url):
            directory_urls.append(
                (
                    directory_url_year(absolute_url, link.get_text(" ", strip=True)),
                    len(directory_urls),
                    absolute_url,
                )
            )
    if not directory_urls:
        return None
    return max(directory_urls)[2]


def deduplicate_document_candidates(
    candidates: Sequence[ProgramDocumentCandidate],
) -> list[ProgramDocumentCandidate]:
    deduplicated: dict[str, ProgramDocumentCandidate] = {}
    for candidate in candidates:
        deduplicated.setdefault(candidate.source_url, candidate)
    return list(deduplicated.values())


def is_terminal_program_document(
    value: tuple[str, uuid.UUID | None] | None,
) -> bool:
    if value is None:
        return False
    import_status, uploaded_document_id = value
    return uploaded_document_id is not None or import_status in {
        ProgramDocumentImportStatus.DOWNLOADED,
        ProgramDocumentImportStatus.OVERSIZED,
        ProgramDocumentImportStatus.FAILED,
        ProgramDocumentImportStatus.PROCESSED,
    }


def match_institution(
    institution_text: str | None,
    *,
    program_url: str | None,
    institutions: Sequence[Institution],
) -> Institution | None:
    if program_url:
        parts = [part.lower() for part in urlparse(program_url).path.split("/") if part]
        for part in parts:
            code = INSTITUTION_SLUG_TO_CODE.get(part)
            if code:
                match = next((institution for institution in institutions if institution.code == code), None)
                if match is not None:
                    return match
    if not institution_text:
        return None
    normalized_text = normalize_key(institution_text)
    for institution in institutions:
        if institution.code in institution_text:
            return institution
        normalized_name = normalize_key(institution.name)
        if normalized_name in normalized_text or normalized_text in normalized_name:
            return institution
    return None


def match_department_for_program(
    text: str,
    *,
    departments: Sequence[Department],
) -> DepartmentMatchResult:
    normalized_text = normalize_key(text)
    matches: dict[uuid.UUID, Department] = {}
    for department in departments:
        full_name = normalize_key(department.name)
        short_name = normalize_key(department.name.replace("Кафедра", "", 1))
        if full_name and full_name in normalized_text:
            matches[department.id] = department
        elif short_name and len(short_name) > 12 and short_name in normalized_text:
            matches[department.id] = department
    if len(matches) == 1:
        return DepartmentMatchResult(
            department=next(iter(matches.values())),
            status=DepartmentLinkStatus.MATCHED,
            confidence=1.0,
        )
    return DepartmentMatchResult(
        department=None,
        status=DepartmentLinkStatus.PENDING_REVIEW,
        confidence=None,
    )


def program_response(
    program: EducationProgram,
    *,
    institution: Institution | None,
    department: Department | None,
    stats: dict[str, int],
) -> EducationProgramResponse:
    return EducationProgramResponse(
        id=program.id,
        level=program.level,
        field_code=program.field_code,
        field_name=program.field_name,
        specialty_code=program.specialty_code,
        specialty_name=program.specialty_name,
        program_name=program.program_name,
        qualification=program.qualification,
        study_form=program.study_form,
        duration=program.duration,
        credits=program.credits,
        manager=program.manager,
        program_url=program.program_url,
        source_page_url=program.source_page_url,
        institution_id=program.institution_id,
        institution_code=institution.code if institution else None,
        institution_name=institution.name if institution else None,
        department_id=program.department_id,
        department_name=department.name if department else None,
        department_link_status=program.department_link_status,
        department_match_confidence=program.department_match_confidence,
        deleted_at=program.deleted_at,
        document_count=stats.get("document_count", 0),
        downloaded_document_count=stats.get("downloaded_document_count", 0),
        oversized_document_count=stats.get("oversized_document_count", 0),
        failed_document_count=stats.get("failed_document_count", 0),
        created_at=program.created_at,
        updated_at=program.updated_at,
    )


def program_import_run_response(run: ProgramImportRun) -> ProgramImportRunResponse:
    return ProgramImportRunResponse(
        id=run.id,
        source_url=run.source_url,
        status=run.status,
        program_count=run.program_count,
        created_document_count=run.created_document_count,
        oversized_document_count=run.oversized_document_count,
        failed_document_count=run.failed_document_count,
        matched_program_count=run.matched_program_count,
        pending_review_program_count=run.pending_review_program_count,
        duration_ms=run.duration_ms,
        error_message=run.error_message,
        created_at=run.created_at,
        updated_at=run.updated_at,
    )


def classify_program_document_kind(title: str, source_url: str) -> str:
    value = f"{title} {source_url}".lower()
    if "самооцін" in value:
        return ProgramDocumentKind.SELF_EVALUATION
    if "сертиф" in value:
        return ProgramDocumentKind.CERTIFICATE
    if "звіт" in value or "експерт" in value and "програм" not in value:
        return ProgramDocumentKind.ACCREDITATION_REPORT
    if "розклад" in value or "візит" in value or "відкрита зустріч" in value:
        return ProgramDocumentKind.VISIT_SCHEDULE
    if "проєкт" in value or "проект" in value:
        return ProgramDocumentKind.PROJECT
    if "зауважен" in value or "стейкхолдер" in value:
        return ProgramDocumentKind.STAKEHOLDER_FEEDBACK
    if "опп" in value or "освіт" in value:
        return ProgramDocumentKind.OPP
    return ProgramDocumentKind.OTHER


def is_directory_url(url: str) -> bool:
    return urlparse(url).netloc.lower() in {
        "directory.lpnu.ua",
        "directory-new.lpnu.ua",
    }


def directory_url_year(url: str, label: str) -> int:
    for value in (label, url):
        matches = re.findall(r"(20\d{2})", value)
        if matches:
            return int(matches[-1])
    return 0


def looks_like_downloadable_url(url: str) -> bool:
    extension = PurePosixPath(urlparse(url).path).suffix.lower()
    return extension in {".pdf", ".docx", ".txt", ".md", ".rtf", ".csv", ".xlsx", ".pptx", ".odt"}


def filename_from_url(url: str, fallback_title: str) -> str:
    filename = PurePosixPath(urlparse(url).path).name
    if filename:
        return sanitize_filename(filename)
    return sanitize_filename(fallback_title)


def link_size_label(link) -> str | None:
    file_wrapper = link.find_parent(class_="file")
    if file_wrapper is not None:
        size_node = file_wrapper.select_one(".file-size")
        if size_node is not None:
            size_text = normalize_spaces(size_node.get_text(" ", strip=True))
            if size_text:
                return size_text
    text = ""
    for sibling in link.next_siblings:
        if getattr(sibling, "name", None) == "a":
            break
        text += str(sibling)
    match = re.search(r"(\d+(?:[,.]\d+)?)\s*(МБ|MB|КБ|KB)", text, re.IGNORECASE)
    return normalize_spaces(match.group(0)) if match else None


def size_label_to_bytes(value: str | None) -> int | None:
    if value is None:
        return None
    match = re.search(r"(\d+(?:[,.]\d+)?)\s*(МБ|MB|КБ|KB)", value, re.IGNORECASE)
    if not match:
        return None
    amount = float(match.group(1).replace(",", "."))
    unit = match.group(2).lower()
    multiplier = 1024 * 1024 if unit in {"мб", "mb"} else 1024
    return int(amount * multiplier)


def first_labeled_value(pairs: dict[str, str], *labels: str) -> str | None:
    for label in labels:
        normalized = normalize_label(label)
        if normalized in pairs and pairs[normalized]:
            return pairs[normalized]
    return None


def strip_quotes(value: str) -> str:
    return normalize_spaces(value).strip(" «»\"")


def normalize_label(value: str) -> str:
    return normalize_spaces(value).strip(":").lower()


def normalize_spaces(value: str) -> str:
    return " ".join(value.split())


def normalize_key(value: str) -> str:
    normalized = value.lower().replace("’", "'").replace("ʼ", "'").replace("`", "'")
    normalized = normalized.replace("інституту", "").replace("інститут", "")
    normalized = re.sub(r"[^\w\s']", " ", normalized, flags=re.UNICODE)
    return normalize_spaces(normalized)


def safe_import_error(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        return str(exc.detail)
    return "Program document import failed."
