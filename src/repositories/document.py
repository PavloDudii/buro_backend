import uuid
from datetime import datetime

from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.document import UploadedDocument
from src.models.user import User


class UploadedDocumentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self,
        *,
        document_id: uuid.UUID | None = None,
        original_filename: str,
        safe_filename: str,
        content_type: str,
        file_extension: str,
        size_bytes: int,
        sha256_hash: str,
        storage_key: str,
        uploaded_by_id: uuid.UUID,
    ) -> UploadedDocument:
        document = UploadedDocument(
            original_filename=original_filename,
            safe_filename=safe_filename,
            content_type=content_type,
            file_extension=file_extension,
            size_bytes=size_bytes,
            sha256_hash=sha256_hash,
            uploaded_by_id=uploaded_by_id,
            storage_key=storage_key,
        )
        if document_id is not None:
            document.id = document_id
        self.session.add(document)
        await self.session.flush()
        return document

    async def get_active_by_id(self, document_id: uuid.UUID) -> UploadedDocument | None:
        result = await self.session.execute(
            select(UploadedDocument).where(
                UploadedDocument.id == document_id,
                UploadedDocument.deleted_at.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def list_active(
        self,
        *,
        limit: int,
        offset: int,
        search: str | None = None,
    ) -> tuple[list[UploadedDocument], int]:
        filters = [UploadedDocument.deleted_at.is_(None)]
        if search:
            pattern = f"%{search.strip()}%"
            filters.append(
                or_(
                    UploadedDocument.original_filename.ilike(pattern),
                    UploadedDocument.safe_filename.ilike(pattern),
                    UploadedDocument.sha256_hash.ilike(pattern),
                    cast(UploadedDocument.id, String).ilike(pattern),
                    User.email.ilike(pattern),
                )
            )

        base_statement = select(UploadedDocument).join(User).where(*filters)
        count_statement = select(func.count()).select_from(UploadedDocument).join(User).where(*filters)
        total_result = await self.session.execute(count_statement)
        documents_result = await self.session.execute(
            base_statement.order_by(UploadedDocument.created_at.desc(), UploadedDocument.id)
            .limit(limit)
            .offset(offset)
        )
        return list(documents_result.scalars().all()), total_result.scalar_one()

    async def soft_delete(self, document: UploadedDocument, *, deleted_at: datetime) -> None:
        document.deleted_at = deleted_at
        await self.session.flush()
