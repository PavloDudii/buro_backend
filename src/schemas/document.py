from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from src.models.document import DocumentProcessingStatus
from src.models.document import DocumentUploadIntentStatus


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
    processing_status: DocumentProcessingStatus
    processing_error: str | None
    processing_error_code: str | None
    processing_error_stage: str | None
    processing_started_at: datetime | None
    processing_completed_at: datetime | None
    parser_version: str | None
    extraction_version: str | None
    source_type: str = "uploaded"
    program_id: UUID | None = None
    program_name: str | None = None
    uploaded_by_id: UUID
    uploaded_by_email: EmailStr
    created_at: datetime
    updated_at: datetime


class UploadedDocumentListResponse(BaseModel):
    items: list[UploadedDocumentResponse]
    total: int
    limit: int | None = None
    offset: int | None = None


class DocumentProcessingRunResponse(BaseModel):
    id: UUID
    document_id: UUID
    status: DocumentProcessingStatus
    started_at: datetime
    completed_at: datetime | None
    total_duration_ms: int | None
    error_code: str | None
    error_stage: str | None
    error_message: str | None
    stage_metrics: dict[str, dict[str, Any]]
    summary_metrics: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class DocumentProcessingDetailsResponse(BaseModel):
    document_id: UUID
    processing_status: DocumentProcessingStatus
    processing_error: str | None
    processing_error_code: str | None
    processing_error_stage: str | None
    processing_started_at: datetime | None
    processing_completed_at: datetime | None
    latest_run: DocumentProcessingRunResponse | None


class DocumentExtractionItemResponse(BaseModel):
    id: UUID
    document_id: UUID
    document_filename: str
    type: str
    value_json: dict[str, Any]
    confidence: float | None
    source: str
    evidence_text: str | None
    page_start: int | None
    page_end: int | None
    char_start: int | None
    char_end: int | None
    created_at: datetime


class DocumentExtractionItemListResponse(BaseModel):
    items: list[DocumentExtractionItemResponse]
    total: int
    limit: int
    offset: int


class DirectUploadInitRequest(BaseModel):
    original_filename: str = Field(min_length=1, max_length=500)
    content_type: str = Field(min_length=1, max_length=255)
    size_bytes: int = Field(gt=0)
    sha256_hash: str | None = Field(default=None, min_length=64, max_length=64)


class DirectUploadInitResponse(BaseModel):
    intent_id: UUID
    document_id: UUID
    pathname: str
    original_filename: str
    safe_filename: str
    content_type: str
    file_extension: str
    size_bytes: int
    sha256_hash: str | None
    status: DocumentUploadIntentStatus
    expires_at: datetime


class DirectUploadAuthorizeRequest(BaseModel):
    intent_id: UUID
    pathname: str = Field(min_length=1, max_length=1000)
    content_type: str = Field(min_length=1, max_length=255)


class DirectUploadAuthorizeResponse(BaseModel):
    pathname: str
    allowed_content_types: list[str]
    add_random_suffix: bool
    allow_overwrite: bool
    token_payload: dict[str, str]


class DirectUploadCompleteRequest(BaseModel):
    intent_id: UUID
    pathname: str = Field(min_length=1, max_length=1000)
    url: str | None = Field(default=None, max_length=2000)
    download_url: str | None = Field(default=None, max_length=2000)
    etag: str | None = Field(default=None, max_length=255)
