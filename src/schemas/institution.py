from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class InstitutionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    code: str
    name: str
    sort_order: int
    is_active: bool
    created_at: datetime
    updated_at: datetime


class InstitutionListResponse(BaseModel):
    items: list[InstitutionResponse]
    total: int


class DepartmentResponse(BaseModel):
    id: UUID
    institution_id: UUID
    institution_code: str
    institution_name: str
    name: str
    sort_order: int
    is_active: bool
    created_at: datetime
    updated_at: datetime


class DepartmentListResponse(BaseModel):
    items: list[DepartmentResponse]
    total: int
