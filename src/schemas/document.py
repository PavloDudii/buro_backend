from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr


class UploadedDocumentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    original_filename: str
    safe_filename: str
    content_type: str
    file_extension: str
    size_bytes: int
    sha256_hash: str
    storage_key: str | None
    deleted_at: datetime | None
    uploaded_by_id: UUID
    uploaded_by_email: EmailStr
    created_at: datetime
    updated_at: datetime


class UploadedDocumentListResponse(BaseModel):
    items: list[UploadedDocumentResponse]
    total: int
    limit: int | None = None
    offset: int | None = None
