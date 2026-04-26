import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.session import get_db
from src.core.dependencies import get_current_admin_user, get_current_user
from src.core.settings import Settings, get_settings
from src.models.user import User
from src.schemas.user import (
    CurrentUserResponse,
    UpdateCurrentUserRequest,
    UpdateUserRoleRequest,
    UserListResponse,
)
from src.services.user import UserService

router = APIRouter(prefix="/users", tags=["users"])


@router.get("", response_model=UserListResponse)
async def list_users(
    db: Annotated[AsyncSession, Depends(get_db)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
    search: Annotated[str | None, Query(min_length=1, max_length=320)] = None,
) -> UserListResponse:
    del current_admin
    return await UserService(db).list_users(limit=limit, offset=offset, search=search)


@router.get("/me", response_model=CurrentUserResponse)
async def get_me(current_user: Annotated[User, Depends(get_current_user)]) -> CurrentUserResponse:
    return CurrentUserResponse.model_validate(current_user)


@router.patch("/me", response_model=CurrentUserResponse)
async def update_me(
    payload: UpdateCurrentUserRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> CurrentUserResponse:
    user = await UserService(db).update_current_user(
        user=current_user,
        full_name=payload.full_name,
    )
    return CurrentUserResponse.model_validate(user)


@router.patch("/role", response_model=CurrentUserResponse)
async def update_user_role(
    payload: UpdateUserRoleRequest,
    db: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
    current_admin: Annotated[User, Depends(get_current_admin_user)],
) -> CurrentUserResponse:
    del current_admin
    user = await UserService(db, settings).update_user_role(email=str(payload.email), role=payload.role)
    return CurrentUserResponse.model_validate(user)


@router.get("/{user_id}", response_model=CurrentUserResponse)
async def get_user_by_id(
    user_id: uuid.UUID,
    db: Annotated[AsyncSession, Depends(get_db)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> CurrentUserResponse:
    user = await UserService(db).get_user_by_id_for_requester(
        user_id=user_id,
        requester=current_user,
    )
    return CurrentUserResponse.model_validate(user)
