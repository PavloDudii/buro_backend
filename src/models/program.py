import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class EducationProgramLevel(StrEnum):
    BACHELOR = "bachelor"


class DepartmentLinkStatus(StrEnum):
    MATCHED = "matched"
    PENDING_REVIEW = "pending_review"
    MANUAL = "manual"


class ProgramDocumentKind(StrEnum):
    OPP = "opp"
    PROJECT = "project"
    SELF_EVALUATION = "self_evaluation"
    VISIT_SCHEDULE = "visit_schedule"
    ACCREDITATION_REPORT = "accreditation_report"
    CERTIFICATE = "certificate"
    STAKEHOLDER_FEEDBACK = "stakeholder_feedback"
    OTHER = "other"


class ProgramDocumentImportStatus(StrEnum):
    QUEUED = "queued"
    DOWNLOADED = "downloaded"
    OVERSIZED = "oversized"
    FAILED = "failed"
    PROCESSED = "processed"


class ProgramImportRunStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


class EducationProgram(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "education_programs"
    __table_args__ = (UniqueConstraint("level", "specialty_code", "program_name"),)

    level: Mapped[EducationProgramLevel] = mapped_column(String(30), nullable=False, index=True)
    field_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    field_name: Mapped[str] = mapped_column(String(500), nullable=False)
    specialty_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    specialty_name: Mapped[str] = mapped_column(String(500), nullable=False)
    program_name: Mapped[str] = mapped_column(String(1000), nullable=False, index=True)
    qualification: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    study_form: Mapped[str | None] = mapped_column(String(255), nullable=True)
    duration: Mapped[str | None] = mapped_column(String(255), nullable=True)
    credits: Mapped[str | None] = mapped_column(String(255), nullable=True)
    manager: Mapped[str | None] = mapped_column(String(500), nullable=True)
    program_url: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    source_page_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    institution_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("institutions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    department_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("departments.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    department_link_status: Mapped[DepartmentLinkStatus] = mapped_column(
        String(30),
        nullable=False,
        default=DepartmentLinkStatus.PENDING_REVIEW,
        index=True,
    )
    department_match_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    institution = relationship("Institution", lazy="joined")
    department = relationship("Department", lazy="joined")
    documents = relationship(
        "ProgramDocument",
        back_populates="program",
        cascade="all, delete-orphan",
    )


class ProgramDocument(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "program_documents"
    __table_args__ = (UniqueConstraint("program_id", "source_url"),)

    program_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("education_programs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    uploaded_document_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("uploaded_documents.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    title: Mapped[str] = mapped_column(String(1000), nullable=False)
    kind: Mapped[ProgramDocumentKind] = mapped_column(String(100), nullable=False, index=True)
    source_size_label: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source_size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    import_status: Mapped[ProgramDocumentImportStatus] = mapped_column(
        String(30),
        nullable=False,
        default=ProgramDocumentImportStatus.QUEUED,
        index=True,
    )
    import_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    program = relationship("EducationProgram", back_populates="documents")
    uploaded_document = relationship("UploadedDocument")


class ProgramDirectorySnapshot(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "program_directory_snapshots"
    __table_args__ = (UniqueConstraint("program_id", "source_url"),)

    program_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("education_programs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    year: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    raw_text: Mapped[str] = mapped_column(Text, nullable=False)
    structured_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    sections_json: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    parsed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    program = relationship("EducationProgram")


class ProgramImportRun(TimestampMixin, UUIDPrimaryKeyMixin, Base):
    __tablename__ = "program_import_runs"

    source_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    status: Mapped[ProgramImportRunStatus] = mapped_column(String(30), nullable=False, index=True)
    program_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    oversized_document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    matched_program_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pending_review_program_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
