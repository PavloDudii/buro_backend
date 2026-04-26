from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator, model_validator

from src.models.user import UserRole
from src.schemas.auth import normalize_email
from src.schemas.auth import UserResponse


class UpdateCurrentUserRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    full_name: str | None = Field(default=None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def validate_non_empty_payload(self) -> "UpdateCurrentUserRequest":
        if self.full_name is None:
            raise ValueError("At least one field must be provided.")
        return self

    @model_validator(mode="after")
    def normalize_fields(self) -> "UpdateCurrentUserRequest":
        if self.full_name is not None:
            self.full_name = self.full_name.strip()
        return self


class CurrentUserResponse(UserResponse):
    pass


class UserListResponse(BaseModel):
    items: list[UserResponse]
    total: int
    limit: int
    offset: int


class UpdateUserRoleRequest(BaseModel):
    email: EmailStr
    role: UserRole

    @field_validator("email", mode="before")
    @classmethod
    def validate_email(cls, value: str) -> str:
        return normalize_email(value)
