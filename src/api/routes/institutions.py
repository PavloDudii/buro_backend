from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.session import get_db
from src.core.dependencies import get_current_admin_user
from src.models.user import User
from src.schemas.institution import DepartmentListResponse, InstitutionListResponse
from src.services.institution import InstitutionService

router = APIRouter(prefix="/institutions", tags=["institutions"])


@router.get("", response_model=InstitutionListResponse)
async def list_institutions(
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
) -> InstitutionListResponse:
    del current_admin
    return await InstitutionService(db).list_institutions()


@router.get("/departments", response_model=DepartmentListResponse)
async def list_departments(
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    institution_code: Annotated[str | None, Query(min_length=1, max_length=20)] = None,
    search: Annotated[str | None, Query(min_length=1, max_length=500)] = None,
) -> DepartmentListResponse:
    del current_admin
    return await InstitutionService(db).list_departments(
        institution_code=institution_code,
        search=search,
    )
