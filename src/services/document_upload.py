import hashlib
import re
import uuid
from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO
from pathlib import PurePath
from zipfile import BadZipFile, ZipFile

import structlog
from fastapi import HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import utcnow
from src.core.settings import Settings, get_settings
from src.models.document import (
    DocumentProcessingStatus,
    DocumentUploadIntent,
    DocumentUploadIntentStatus,
    UploadedDocument,
)
from src.models.program import EducationProgram, ProgramDocument
from src.models.user import User
from src.repositories.document import UploadedDocumentRepository
from src.schemas.document import (
    DirectUploadAuthorizeResponse,
    DirectUploadInitResponse,
    UploadedDocumentListResponse,
    UploadedDocumentResponse,
)
from src.services.blob_storage import BlobStorage, build_document_blob_path

logger = structlog.get_logger(__name__)

MAX_UPLOAD_FILE_SIZE_MB = 50
MAX_UPLOAD_FILE_SIZE_BYTES = MAX_UPLOAD_FILE_SIZE_MB * 1024 * 1024
MAX_UPLOAD_FILES = 10
MAX_ZIP_UNCOMPRESSED_BYTES = 50 * 1024 * 1024
MAX_ZIP_MEMBERS = 1000

ALLOWED_EXTENSIONS = {
    ".csv": "text/csv",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".md": "text/markdown",
    ".odt": "application/vnd.oasis.opendocument.text",
    ".pdf": "application/pdf",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".rtf": "application/rtf",
    ".txt": "text/plain",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

PDF_ACTIVE_CONTENT_PATTERNS = (
    re.compile(rb"/AA\b"),
    re.compile(rb"/EmbeddedFile\b"),
    re.compile(rb"/JavaScript\b"),
    re.compile(rb"/JS\b"),
    re.compile(rb"/Launch\b"),
    re.compile(rb"/OpenAction\b"),
)


@dataclass(frozen=True)
class ValidatedUpload:
    original_filename: str
    safe_filename: str
    content_type: str
    file_extension: str
    size_bytes: int
    sha256_hash: str


@dataclass(frozen=True)
class ValidatedUploadMetadata:
    original_filename: str
    safe_filename: str
    content_type: str
    file_extension: str
    size_bytes: int
    sha256_hash: str | None = None


@dataclass(frozen=True)
class PreparedUpload:
    content: bytes
    validated: ValidatedUpload


@dataclass(frozen=True)
class DirectUploadCompletion:
    document: UploadedDocument
    created: bool


@dataclass(frozen=True)
class DocumentProgramSource:
    program_id: uuid.UUID
    program_name: str


class DocumentUploadService:
    def __init__(
        self,
        session: AsyncSession,
        *,
        blob_storage: BlobStorage | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.session = session
        self.uploaded_documents = UploadedDocumentRepository(session)
        self.blob_storage = blob_storage
        self.settings = settings or get_settings()

    async def upload_documents(
        self,
        *,
        files: list[UploadFile],
        uploaded_by: User,
    ) -> UploadedDocumentListResponse:
        if not files:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="At least one file must be uploaded.",
            )
        if len(files) > MAX_UPLOAD_FILES:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"At most {MAX_UPLOAD_FILES} files can be uploaded at once.",
            )

        prepared_uploads: list[PreparedUpload] = []
        for file in files:
            content = await self._read_file(file)
            validated = self._validate_file(file, content)
            prepared_uploads.append(PreparedUpload(content=content, validated=validated))

        if self.blob_storage is None:
            raise RuntimeError("Blob storage must be configured for document uploads.")

        documents: list[UploadedDocument] = []
        uploaded_pathnames: list[str] = []
        try:
            for prepared in prepared_uploads:
                document_id = uuid.uuid4()
                uploaded_at = utcnow()
                stored_blob = await self.blob_storage.put_document(
                    user_id=uploaded_by.id,
                    document_id=document_id,
                    safe_filename=prepared.validated.safe_filename,
                    content=prepared.content,
                    content_type=prepared.validated.content_type,
                    uploaded_at=uploaded_at,
                )
                uploaded_pathnames.append(stored_blob.pathname)
                documents.append(
                    await self.uploaded_documents.create(
                        document_id=document_id,
                        original_filename=prepared.validated.original_filename,
                        safe_filename=prepared.validated.safe_filename,
                        content_type=prepared.validated.content_type,
                        file_extension=prepared.validated.file_extension,
                        size_bytes=prepared.validated.size_bytes,
                        sha256_hash=prepared.validated.sha256_hash,
                        storage_key=stored_blob.pathname,
                        uploaded_by_id=uploaded_by.id,
                        processing_status=DocumentProcessingStatus.QUEUED,
                    )
                )

            await self.session.commit()
        except Exception as exc:
            await self.session.rollback()
            if uploaded_pathnames:
                try:
                    await self.blob_storage.delete_documents(uploaded_pathnames)
                except Exception as cleanup_exc:
                    logger.exception(
                        "document_upload.cleanup_failed",
                        error_type=cleanup_exc.__class__.__name__,
                        uploaded_pathname_count=len(uploaded_pathnames),
                    )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Document storage is temporarily unavailable.",
            ) from exc

        for document in documents:
            await self.session.refresh(document)

        return UploadedDocumentListResponse(
            items=[document_response(document, uploaded_by) for document in documents],
            total=len(documents),
        )

    async def list_documents(
        self,
        *,
        limit: int,
        offset: int,
        search: str | None,
    ) -> UploadedDocumentListResponse:
        normalized_search = search.strip() if search else None
        if normalized_search == "":
            normalized_search = None

        documents, total = await self.uploaded_documents.list_active(
            limit=limit,
            offset=offset,
            search=normalized_search,
        )
        program_sources = await self._program_sources_for_documents(
            [document.id for document in documents]
        )
        return UploadedDocumentListResponse(
            items=[
                document_response(
                    document,
                    document.uploaded_by,
                    program_source=program_sources.get(document.id),
                )
                for document in documents
            ],
            total=total,
            limit=limit,
            offset=offset,
        )

    async def get_document(self, *, document_id: uuid.UUID) -> UploadedDocumentResponse:
        document = await self.uploaded_documents.get_active_by_id(document_id)
        if document is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document was not found.",
            )
        return document_response(
            document,
            document.uploaded_by,
            program_source=await self._program_source_for_document(document.id),
        )

    async def delete_document(self, *, document_id: uuid.UUID) -> None:
        document = await self.uploaded_documents.get_active_by_id(document_id)
        if document is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document was not found.",
            )
        await self.uploaded_documents.soft_delete(document, deleted_at=utcnow())
        await self.session.commit()

    def document_to_response(self, document: UploadedDocument) -> UploadedDocumentResponse:
        return document_response(document, document.uploaded_by)

    async def document_to_response_async(
        self,
        document: UploadedDocument,
    ) -> UploadedDocumentResponse:
        uploaded_by = await self.session.get(User, document.uploaded_by_id)
        if uploaded_by is None:
            raise RuntimeError("Document uploader was not found.")
        return document_response(
            document,
            uploaded_by,
            program_source=await self._program_source_for_document(document.id),
        )

    async def _program_source_for_document(
        self,
        document_id: uuid.UUID,
    ) -> DocumentProgramSource | None:
        sources = await self._program_sources_for_documents([document_id])
        return sources.get(document_id)

    async def _program_sources_for_documents(
        self,
        document_ids: list[uuid.UUID],
    ) -> dict[uuid.UUID, DocumentProgramSource]:
        if not document_ids:
            return {}
        result = await self.session.execute(
            select(ProgramDocument.uploaded_document_id, EducationProgram.id, EducationProgram.program_name)
            .join(EducationProgram, EducationProgram.id == ProgramDocument.program_id)
            .where(ProgramDocument.uploaded_document_id.in_(document_ids))
        )
        return {
            document_id: DocumentProgramSource(program_id=program_id, program_name=program_name)
            for document_id, program_id, program_name in result.all()
            if document_id is not None
        }

    async def init_direct_upload(
        self,
        *,
        original_filename: str,
        content_type: str,
        size_bytes: int,
        uploaded_by: User,
        sha256_hash: str | None = None,
    ) -> DirectUploadInitResponse:
        validated = validate_upload_metadata(
            original_filename=original_filename,
            content_type=content_type,
            size_bytes=size_bytes,
            sha256_hash=sha256_hash,
        )
        document_id = uuid.uuid4()
        created_at = utcnow()
        pathname = build_document_blob_path(
            user_id=uploaded_by.id,
            document_id=document_id,
            safe_filename=validated.safe_filename,
            uploaded_at=created_at,
            prefix=self.settings.blob_prefix,
        )
        intent = DocumentUploadIntent(
            document_id=document_id,
            uploaded_by_id=uploaded_by.id,
            original_filename=validated.original_filename,
            safe_filename=validated.safe_filename,
            content_type=validated.content_type,
            file_extension=validated.file_extension,
            size_bytes=validated.size_bytes,
            sha256_hash=validated.sha256_hash,
            planned_pathname=pathname,
            status=DocumentUploadIntentStatus.PENDING,
            expires_at=created_at + timedelta(minutes=self.settings.direct_upload_intent_ttl_minutes),
        )
        self.session.add(intent)
        await self.session.commit()
        await self.session.refresh(intent)
        return direct_upload_init_response(intent)

    async def authorize_direct_upload(
        self,
        *,
        intent_id: uuid.UUID,
        pathname: str,
        content_type: str,
        uploaded_by: User,
    ) -> DirectUploadAuthorizeResponse:
        intent = await self._get_upload_intent(intent_id)
        self._ensure_intent_owner(intent, uploaded_by)
        await self._ensure_intent_active(intent)
        if intent.status == DocumentUploadIntentStatus.COMPLETED:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Upload intent has already completed.",
            )
        if pathname != intent.planned_pathname:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Upload pathname does not match the initialized intent.",
            )
        if content_type != intent.content_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Upload content type does not match the initialized intent.",
            )

        if intent.status == DocumentUploadIntentStatus.PENDING:
            intent.status = DocumentUploadIntentStatus.UPLOADING
            await self.session.commit()

        return DirectUploadAuthorizeResponse(
            pathname=intent.planned_pathname,
            allowed_content_types=[intent.content_type],
            add_random_suffix=False,
            allow_overwrite=False,
            token_payload={"intentId": str(intent.id), "documentId": str(intent.document_id)},
        )

    async def complete_direct_upload(
        self,
        *,
        intent_id: uuid.UUID,
        pathname: str,
        url: str | None,
        download_url: str | None,
        etag: str | None,
        completed_by: User | None = None,
    ) -> DirectUploadCompletion:
        if self.blob_storage is None:
            raise RuntimeError("Blob storage must be configured for direct document uploads.")

        intent = await self._get_upload_intent(intent_id)
        if completed_by is not None:
            self._ensure_intent_owner(intent, completed_by)

        existing = await self.session.get(UploadedDocument, intent.document_id)
        if existing is not None:
            await self._mark_intent_completed(
                intent,
                url=url,
                download_url=download_url,
                etag=etag,
            )
            return DirectUploadCompletion(document=existing, created=False)

        await self._ensure_intent_active(intent)
        if pathname != intent.planned_pathname:
            await self._fail_intent(
                intent,
                code="pathname_mismatch",
                message="Upload pathname does not match the initialized intent.",
                cleanup_pathname=pathname,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Upload pathname does not match the initialized intent.",
            )

        intent.status = DocumentUploadIntentStatus.VALIDATING
        await self.session.commit()

        try:
            content = await self.blob_storage.get_document_content(intent.planned_pathname)
        except Exception as exc:
            await self._fail_intent(
                intent,
                code="blob_read_failed",
                message="Uploaded file could not be read from storage.",
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Uploaded file could not be read from storage.",
            ) from exc

        calculated_hash = hashlib.sha256(content).hexdigest()
        if len(content) != intent.size_bytes:
            await self._fail_intent(
                intent,
                code="size_mismatch",
                message="Uploaded file size does not match the initialized intent.",
                cleanup_pathname=intent.planned_pathname,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Uploaded file size does not match the initialized intent.",
            )
        if intent.sha256_hash and calculated_hash != intent.sha256_hash:
            await self._fail_intent(
                intent,
                code="hash_mismatch",
                message="Uploaded file hash does not match the initialized intent.",
                cleanup_pathname=intent.planned_pathname,
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Uploaded file hash does not match the initialized intent.",
            )

        try:
            validate_content(extension=f".{intent.file_extension}", content=content)
        except HTTPException as exc:
            await self._fail_intent(
                intent,
                code="invalid_content",
                message=str(exc.detail),
                cleanup_pathname=intent.planned_pathname,
            )
            raise

        document = await self.uploaded_documents.create(
            document_id=intent.document_id,
            original_filename=intent.original_filename,
            safe_filename=intent.safe_filename,
            content_type=intent.content_type,
            file_extension=intent.file_extension,
            size_bytes=intent.size_bytes,
            sha256_hash=calculated_hash,
            storage_key=intent.planned_pathname,
            uploaded_by_id=intent.uploaded_by_id,
            processing_status=DocumentProcessingStatus.QUEUED,
        )
        await self._mark_intent_completed(
            intent,
            url=url,
            download_url=download_url,
            etag=etag,
            commit=False,
        )
        await self.session.commit()
        await self.session.refresh(document)
        return DirectUploadCompletion(document=document, created=True)

    async def _read_file(self, file: UploadFile) -> bytes:
        content = await file.read(MAX_UPLOAD_FILE_SIZE_BYTES + 1)
        if len(content) > MAX_UPLOAD_FILE_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"File '{file.filename}' exceeds the {MAX_UPLOAD_FILE_SIZE_MB}MB limit.",
            )
        if not content:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File '{file.filename}' is empty.",
            )
        return content

    def _validate_file(self, file: UploadFile, content: bytes) -> ValidatedUpload:
        metadata = validate_upload_metadata(
            original_filename=file.filename or "",
            content_type=None,
            size_bytes=len(content),
        )
        validate_content(extension=f".{metadata.file_extension}", content=content)
        return ValidatedUpload(
            original_filename=metadata.original_filename,
            safe_filename=metadata.safe_filename,
            content_type=metadata.content_type,
            file_extension=metadata.file_extension,
            size_bytes=len(content),
            sha256_hash=hashlib.sha256(content).hexdigest(),
        )

    async def _get_upload_intent(self, intent_id: uuid.UUID) -> DocumentUploadIntent:
        result = await self.session.execute(
            select(DocumentUploadIntent).where(DocumentUploadIntent.id == intent_id)
        )
        intent = result.scalar_one_or_none()
        if intent is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Upload intent was not found.",
            )
        return intent

    @staticmethod
    def _ensure_intent_owner(intent: DocumentUploadIntent, uploaded_by: User) -> None:
        if intent.uploaded_by_id != uploaded_by.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Upload intent belongs to a different user.",
            )

    async def _ensure_intent_active(self, intent: DocumentUploadIntent) -> None:
        if intent.expires_at <= utcnow():
            intent.status = DocumentUploadIntentStatus.EXPIRED
            intent.error_code = "intent_expired"
            intent.error_message = "Upload intent has expired."
            await self.session.commit()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Upload intent has expired.",
            )
        if intent.status == DocumentUploadIntentStatus.FAILED:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Upload intent has failed.",
            )

    async def _mark_intent_completed(
        self,
        intent: DocumentUploadIntent,
        *,
        url: str | None,
        download_url: str | None,
        etag: str | None,
        commit: bool = True,
    ) -> None:
        intent.status = DocumentUploadIntentStatus.COMPLETED
        intent.error_code = None
        intent.error_message = None
        intent.completed_at = intent.completed_at or utcnow()
        intent.blob_url = url or intent.blob_url
        intent.blob_download_url = download_url or intent.blob_download_url
        intent.blob_etag = etag or intent.blob_etag
        if commit:
            await self.session.commit()

    async def _fail_intent(
        self,
        intent: DocumentUploadIntent,
        *,
        code: str,
        message: str,
        cleanup_pathname: str | None = None,
    ) -> None:
        intent.status = DocumentUploadIntentStatus.FAILED
        intent.error_code = code
        intent.error_message = message
        await self.session.commit()
        if cleanup_pathname and self.blob_storage is not None:
            try:
                await self.blob_storage.delete_documents([cleanup_pathname])
            except Exception as cleanup_exc:
                logger.exception(
                    "document_upload.direct_cleanup_failed",
                    intent_id=str(intent.id),
                    document_id=str(intent.document_id),
                    pathname=cleanup_pathname,
                    error_type=cleanup_exc.__class__.__name__,
                )


def sanitize_filename(filename: str) -> str:
    basename = PurePath(filename).name.strip()
    if not basename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file must have a filename.",
        )

    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", basename)
    safe_name = re.sub(r"_+", "_", safe_name).strip("._")
    if not safe_name or not PurePath(safe_name).suffix:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Filename '{filename}' is not valid.",
        )
    return safe_name


def validate_upload_metadata(
    *,
    original_filename: str,
    content_type: str | None,
    size_bytes: int,
    sha256_hash: str | None = None,
) -> ValidatedUploadMetadata:
    if size_bytes <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file is empty.",
        )
    if size_bytes > MAX_UPLOAD_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=f"File exceeds the {MAX_UPLOAD_FILE_SIZE_MB}MB limit.",
        )

    safe_filename = sanitize_filename(original_filename)
    extension = PurePath(safe_filename).suffix.lower()
    expected_content_type = ALLOWED_EXTENSIONS.get(extension)
    if expected_content_type is None:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"File type '{extension or 'unknown'}' is not allowed.",
        )
    if content_type is not None and content_type != expected_content_type:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File content type does not match the file extension.",
        )
    if sha256_hash is not None and not re.fullmatch(r"[a-fA-F0-9]{64}", sha256_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="SHA-256 hash must be a 64-character hexadecimal string.",
        )

    return ValidatedUploadMetadata(
        original_filename=original_filename,
        safe_filename=safe_filename,
        content_type=expected_content_type,
        file_extension=extension.removeprefix("."),
        size_bytes=size_bytes,
        sha256_hash=sha256_hash.lower() if sha256_hash else None,
    )


def validate_content(*, extension: str, content: bytes) -> None:
    if extension == ".pdf":
        validate_pdf(content)
    elif extension in {".docx", ".xlsx", ".pptx"}:
        validate_ooxml_archive(extension=extension, content=content)
    elif extension == ".odt":
        validate_odt_archive(content)
    elif extension == ".rtf":
        validate_rtf(content)
    else:
        validate_text(content)


def validate_pdf(content: bytes) -> None:
    if not content.startswith(b"%PDF-"):
        raise invalid_content("PDF file signature is invalid.")

    if any(pattern.search(content) for pattern in PDF_ACTIVE_CONTENT_PATTERNS):
        raise invalid_content("PDF contains active or embedded content and was rejected.")


def validate_ooxml_archive(*, extension: str, content: bytes) -> None:
    required_member_by_extension = {
        ".docx": "word/document.xml",
        ".pptx": "ppt/presentation.xml",
        ".xlsx": "xl/workbook.xml",
    }
    validate_zip_archive(content)
    with ZipFile(BytesIO(content)) as archive:
        names = set(archive.namelist())
        if "[Content_Types].xml" not in names or required_member_by_extension[extension] not in names:
            raise invalid_content(f"{extension} archive structure is invalid.")


def validate_odt_archive(content: bytes) -> None:
    validate_zip_archive(content)
    with ZipFile(BytesIO(content)) as archive:
        if "mimetype" not in archive.namelist():
            raise invalid_content("ODT archive structure is invalid.")
        if archive.read("mimetype") != b"application/vnd.oasis.opendocument.text":
            raise invalid_content("ODT mimetype is invalid.")


def validate_zip_archive(content: bytes) -> None:
    try:
        with ZipFile(BytesIO(content)) as archive:
            members = archive.infolist()
            if len(members) > MAX_ZIP_MEMBERS:
                raise invalid_content("Archive contains too many files.")
            total_size = 0
            for member in members:
                if member.flag_bits & 0x1:
                    raise invalid_content("Encrypted archives are not allowed.")
                if member.filename.startswith("/") or ".." in PurePath(member.filename).parts:
                    raise invalid_content("Archive contains unsafe paths.")
                total_size += member.file_size
            if total_size > MAX_ZIP_UNCOMPRESSED_BYTES:
                raise invalid_content("Archive uncompressed size is too large.")
    except BadZipFile as exc:
        raise invalid_content("Archive file signature is invalid.") from exc


def validate_rtf(content: bytes) -> None:
    if not content.startswith(b"{\\rtf"):
        raise invalid_content("RTF file signature is invalid.")


def validate_text(content: bytes) -> None:
    if b"\x00" in content:
        raise invalid_content("Text files cannot contain null bytes.")
    try:
        content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise invalid_content("Text file must be valid UTF-8.") from exc


def invalid_content(detail: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


def document_response(
    document: UploadedDocument,
    uploaded_by: User,
    *,
    program_source: DocumentProgramSource | None = None,
) -> UploadedDocumentResponse:
    return UploadedDocumentResponse(
        id=document.id,
        original_filename=document.original_filename,
        safe_filename=document.safe_filename,
        content_type=document.content_type,
        file_extension=document.file_extension,
        size_bytes=document.size_bytes,
        sha256_hash=document.sha256_hash,
        storage_key=document.storage_key,
        deleted_at=document.deleted_at,
        processing_status=document.processing_status,
        processing_error=document.processing_error,
        processing_error_code=document.processing_error_code,
        processing_error_stage=document.processing_error_stage,
        processing_started_at=document.processing_started_at,
        processing_completed_at=document.processing_completed_at,
        parser_version=document.parser_version,
        extraction_version=document.extraction_version,
        source_type="program" if program_source else "uploaded",
        program_id=program_source.program_id if program_source else None,
        program_name=program_source.program_name if program_source else None,
        uploaded_by_id=document.uploaded_by_id,
        uploaded_by_email=uploaded_by.email,
        created_at=document.created_at,
        updated_at=document.updated_at,
    )


def direct_upload_init_response(intent: DocumentUploadIntent) -> DirectUploadInitResponse:
    return DirectUploadInitResponse(
        intent_id=intent.id,
        document_id=intent.document_id,
        pathname=intent.planned_pathname,
        original_filename=intent.original_filename,
        safe_filename=intent.safe_filename,
        content_type=intent.content_type,
        file_extension=intent.file_extension,
        size_bytes=intent.size_bytes,
        sha256_hash=intent.sha256_hash,
        status=intent.status,
        expires_at=intent.expires_at,
    )
