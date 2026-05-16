import uuid
from datetime import datetime
from enum import StrEnum

from pgvector.sqlalchemy import Vector  # type: ignore[import-untyped]
from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class DocumentProcessingStatus(StrEnum):
    NOT_STARTED = "not_started"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    UNSUPPORTED = "unsupported"
    NEEDS_OCR = "needs_ocr"


class DocumentUploadIntentStatus(StrEnum):
    PENDING = "pending"
    UPLOADING = "uploading"
    VALIDATING = "validating"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


class ParsedDocument(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "parsed_documents"

    document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("uploaded_documents.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    raw_text: Mapped[str] = mapped_column(Text, nullable=False)
    language: Mapped[str] = mapped_column(String(50), nullable=False, default="ukrainian")
    parser_version: Mapped[str] = mapped_column(String(100), nullable=False)
    page_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    outline_json: Mapped[list] = mapped_column(JSONB, default=list)

    document = relationship("UploadedDocument")


class DocumentChunk(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "document_chunks"

    document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("uploaded_documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    chunk_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
        unique=True,
        default=uuid.uuid4,
    )
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    title: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    section_path: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    fts_text: Mapped[str] = mapped_column(Text, nullable=False)
    search_vector: Mapped[str | None] = mapped_column(TSVECTOR, nullable=True)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1536), nullable=True)
    embedding_model: Mapped[str | None] = mapped_column(String(100), nullable=True)
    token_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    page_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    page_end: Mapped[int | None] = mapped_column(Integer, nullable=True)
    char_start: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    char_end: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    document = relationship("UploadedDocument")


class DocumentExtractionItem(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "document_extraction_items"

    document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("uploaded_documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    chunk_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("document_chunks.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    value_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    confidence: Mapped[float | None] = mapped_column(nullable=True)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    evidence_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    page_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    page_end: Mapped[int | None] = mapped_column(Integer, nullable=True)
    char_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    char_end: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSONB, default=dict)

    document = relationship("UploadedDocument")
    chunk = relationship("DocumentChunk")


class DocumentProcessingRun(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "document_processing_runs"

    document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("uploaded_documents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status: Mapped[DocumentProcessingStatus] = mapped_column(
        String(30),
        nullable=False,
        index=True,
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    total_duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_stage: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    stage_metrics_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    summary_metrics_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    document = relationship("UploadedDocument", back_populates="processing_runs")


class DocumentUploadIntent(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "document_upload_intents"

    document_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True)
    uploaded_by_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    original_filename: Mapped[str] = mapped_column(String(500), nullable=False)
    safe_filename: Mapped[str] = mapped_column(String(500), nullable=False)
    content_type: Mapped[str] = mapped_column(String(255), nullable=False)
    file_extension: Mapped[str] = mapped_column(String(20), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sha256_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    planned_pathname: Mapped[str] = mapped_column(String(1000), nullable=False, unique=True)
    status: Mapped[DocumentUploadIntentStatus] = mapped_column(
        String(30),
        nullable=False,
        default=DocumentUploadIntentStatus.PENDING,
        index=True,
    )
    error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    blob_url: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    blob_download_url: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    blob_etag: Mapped[str | None] = mapped_column(String(255), nullable=True)

    uploaded_by = relationship("User")


class UploadedDocument(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "uploaded_documents"

    original_filename: Mapped[str] = mapped_column(String(500), nullable=False)
    safe_filename: Mapped[str] = mapped_column(String(500), nullable=False)
    content_type: Mapped[str] = mapped_column(String(255), nullable=False)
    file_extension: Mapped[str] = mapped_column(String(20), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sha256_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    storage_key: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    processing_status: Mapped[DocumentProcessingStatus] = mapped_column(
        String(30),
        nullable=False,
        default=DocumentProcessingStatus.NOT_STARTED,
        index=True,
    )
    processing_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    processing_error_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    processing_error_stage: Mapped[str | None] = mapped_column(String(100), nullable=True)
    processing_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    processing_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    parser_version: Mapped[str | None] = mapped_column(String(100), nullable=True)
    extraction_version: Mapped[str | None] = mapped_column(String(100), nullable=True)
    uploaded_by_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )

    uploaded_by = relationship("User")
    processing_runs = relationship("DocumentProcessingRun", back_populates="document")
