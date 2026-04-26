import hashlib
import re
import uuid
from dataclasses import dataclass
from io import BytesIO
from pathlib import PurePath
from zipfile import BadZipFile, ZipFile

from fastapi import HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.security import utcnow
from src.models.document import UploadedDocument
from src.models.user import User
from src.repositories.document import UploadedDocumentRepository
from src.schemas.document import UploadedDocumentListResponse, UploadedDocumentResponse

MAX_UPLOAD_FILE_SIZE_BYTES = 10 * 1024 * 1024
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


class DocumentUploadService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.uploaded_documents = UploadedDocumentRepository(session)

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

        documents: list[UploadedDocument] = []
        for file in files:
            content = await self._read_file(file)
            validated = self._validate_file(file, content)
            documents.append(
                await self.uploaded_documents.create(
                    original_filename=validated.original_filename,
                    safe_filename=validated.safe_filename,
                    content_type=validated.content_type,
                    file_extension=validated.file_extension,
                    size_bytes=validated.size_bytes,
                    sha256_hash=validated.sha256_hash,
                    uploaded_by_id=uploaded_by.id,
                )
            )

        await self.session.commit()
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
        return UploadedDocumentListResponse(
            items=[document_response(document, document.uploaded_by) for document in documents],
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
        return document_response(document, document.uploaded_by)

    async def delete_document(self, *, document_id: uuid.UUID) -> None:
        document = await self.uploaded_documents.get_active_by_id(document_id)
        if document is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Document was not found.",
            )
        await self.uploaded_documents.soft_delete(document, deleted_at=utcnow())
        await self.session.commit()

    async def _read_file(self, file: UploadFile) -> bytes:
        content = await file.read(MAX_UPLOAD_FILE_SIZE_BYTES + 1)
        if len(content) > MAX_UPLOAD_FILE_SIZE_BYTES:
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"File '{file.filename}' exceeds the 10MB limit.",
            )
        if not content:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File '{file.filename}' is empty.",
            )
        return content

    def _validate_file(self, file: UploadFile, content: bytes) -> ValidatedUpload:
        original_filename = file.filename or ""
        safe_filename = sanitize_filename(original_filename)
        extension = PurePath(safe_filename).suffix.lower()
        content_type = ALLOWED_EXTENSIONS.get(extension)
        if content_type is None:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=f"File type '{extension or 'unknown'}' is not allowed.",
            )

        validate_content(extension=extension, content=content)
        return ValidatedUpload(
            original_filename=original_filename,
            safe_filename=safe_filename,
            content_type=content_type,
            file_extension=extension.removeprefix("."),
            size_bytes=len(content),
            sha256_hash=hashlib.sha256(content).hexdigest(),
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


def document_response(document: UploadedDocument, uploaded_by: User) -> UploadedDocumentResponse:
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
        uploaded_by_id=document.uploaded_by_id,
        uploaded_by_email=uploaded_by.email,
        created_at=document.created_at,
        updated_at=document.updated_at,
    )
