from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ProgramImportRunResponse(BaseModel):
    id: UUID
    source_url: str
    status: str
    program_count: int
    created_document_count: int
    oversized_document_count: int
    failed_document_count: int
    matched_program_count: int
    pending_review_program_count: int
    duration_ms: int | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime


class ProgramDirectorySnapshotResponse(BaseModel):
    id: UUID
    program_id: UUID
    source_url: str
    year: int | None
    raw_text: str
    structured_json: dict
    sections_json: list
    parsed_at: datetime
    created_at: datetime
    updated_at: datetime


class EducationProgramResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    level: str
    field_code: str
    field_name: str
    specialty_code: str
    specialty_name: str
    program_name: str
    qualification: str | None
    study_form: str | None
    duration: str | None
    credits: str | None
    manager: str | None
    program_url: str | None
    source_page_url: str
    institution_id: UUID | None
    institution_code: str | None
    institution_name: str | None
    department_id: UUID | None
    department_name: str | None
    department_link_status: str
    department_match_confidence: float | None
    deleted_at: datetime | None
    document_count: int
    downloaded_document_count: int
    oversized_document_count: int
    failed_document_count: int
    created_at: datetime
    updated_at: datetime


class EducationProgramListResponse(BaseModel):
    items: list[EducationProgramResponse]
    total: int
    limit: int
    offset: int


class ProgramDocumentResponse(BaseModel):
    id: UUID
    program_id: UUID
    uploaded_document_id: UUID | None
    source_url: str
    title: str
    kind: str
    source_size_label: str | None
    source_size_bytes: int | None
    import_status: str
    import_error: str | None
    document_filename: str | None
    processing_status: str | None
    created_at: datetime
    updated_at: datetime


class EducationProgramDetailResponse(EducationProgramResponse):
    documents: list[ProgramDocumentResponse]
    directory_snapshot: ProgramDirectorySnapshotResponse | None


class ProgramDepartmentUpdateRequest(BaseModel):
    department_id: UUID


class ProgramUpdateRequest(BaseModel):
    field_code: str | None = Field(default=None, min_length=1, max_length=50)
    field_name: str | None = Field(default=None, min_length=1, max_length=500)
    specialty_code: str | None = Field(default=None, min_length=1, max_length=50)
    specialty_name: str | None = Field(default=None, min_length=1, max_length=500)
    program_name: str | None = Field(default=None, min_length=1, max_length=1000)
    qualification: str | None = Field(default=None, max_length=1000)
    study_form: str | None = Field(default=None, max_length=255)
    duration: str | None = Field(default=None, max_length=255)
    credits: str | None = Field(default=None, max_length=255)
    manager: str | None = Field(default=None, max_length=500)
    program_url: str | None = Field(default=None, max_length=2000)
    source_page_url: str | None = Field(default=None, min_length=1, max_length=2000)
    institution_id: UUID | None = None
    department_id: UUID | None = None


class ProgramDocumentUpdateRequest(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=1000)
    kind: str | None = Field(default=None, min_length=1, max_length=100)
    import_status: str | None = Field(default=None, min_length=1, max_length=30)
    import_error: str | None = None
