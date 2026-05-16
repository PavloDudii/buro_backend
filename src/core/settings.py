from functools import lru_cache
import json

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.core.logging import LogFormat


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = Field(default="bur_backend", alias="APP_NAME")
    app_env: str = Field(default="development", alias="APP_ENV")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8000, alias="APP_PORT")
    app_debug: bool = Field(default=True, alias="APP_DEBUG")
    database_url: str = Field(
        default="postgresql+psycopg://buro_user:buro_password@localhost:5433/buro_database",
        alias="DATABASE_URL",
    )
    sql_echo: bool = Field(default=False, alias="SQL_ECHO")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: LogFormat = Field(default="json", alias="LOG_FORMAT")
    jwt_secret_key: str = Field(
        default="change-me-in-production-32-byte-secret",
        alias="JWT_SECRET_KEY",
    )
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")
    access_token_ttl_minutes: int = Field(default=30, alias="ACCESS_TOKEN_TTL_MINUTES")
    refresh_token_ttl_days: int = Field(default=7, alias="REFRESH_TOKEN_TTL_DAYS")
    admin_email: str | None = Field(default=None, alias="ADMIN_EMAIL")
    admin_password: str | None = Field(default=None, alias="ADMIN_PASSWORD")
    blob_read_write_token: str | None = Field(default=None, alias="BLOB_READ_WRITE_TOKEN")
    blob_prefix: str = Field(default="documents", alias="BLOB_PREFIX")
    direct_upload_intent_ttl_minutes: int = Field(
        default=30,
        alias="DIRECT_UPLOAD_INTENT_TTL_MINUTES",
    )
    direct_upload_callback_secret: str | None = Field(
        default=None,
        alias="DIRECT_UPLOAD_CALLBACK_SECRET",
    )
    allowed_origins_env: str = Field(default="", alias="ALLOWED_ORIGINS")
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_extraction_model: str = Field(default="gpt-5-mini", alias="OPENAI_EXTRACTION_MODEL")
    openai_extraction_concurrency: int = Field(
        default=4,
        ge=1,
        le=10,
        alias="OPENAI_EXTRACTION_CONCURRENCY",
    )
    openai_embedding_model: str = Field(
        default="text-embedding-3-small",
        alias="OPENAI_EMBEDDING_MODEL",
    )
    openai_embedding_dimensions: int = Field(default=1536, alias="OPENAI_EMBEDDING_DIMENSIONS")
    ocr_enabled: bool = Field(default=True, alias="OCR_ENABLED")
    ocr_languages: str = Field(default="ukr+eng", alias="OCR_LANGUAGES")
    ocr_timeout_seconds: int = Field(default=300, ge=1, alias="OCR_TIMEOUT_SECONDS")
    ocr_max_pages: int = Field(default=80, ge=1, alias="OCR_MAX_PAGES")

    @computed_field
    @property
    def allowed_origins(self) -> list[str]:
        raw = self.allowed_origins_env.strip()
        if not raw:
            return []
        if raw.startswith("["):
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                raise ValueError("ALLOWED_ORIGINS must be a JSON array or comma-separated string.")
            return parsed
        return [o.strip() for o in raw.split(",") if o.strip()]


@lru_cache()
def get_settings() -> Settings:
    return Settings()
