from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from src.api.router import api_router
from src.core.db.session import AsyncSessionLocal
from src.core.logging import configure_logging
from src.core.settings import get_settings
from src.schemas.health import HealthCheckResponse
from src.services.bootstrap import ensure_configured_admin
from src.services.institution import ensure_institutions_seeded

settings = get_settings()
configure_logging(settings)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    del app
    async with AsyncSessionLocal() as session:
        await ensure_configured_admin(session, settings)
        await ensure_institutions_seeded(session)
    yield


app = FastAPI(title=settings.app_name, debug=settings.app_debug, lifespan=lifespan)
cors_origins = settings.allowed_origins
if not cors_origins and settings.app_env == "development":
    cors_origins = ["http://localhost:3000", "http://127.0.0.1:3000"]

if cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
app.include_router(api_router, prefix="/api/v1")


def custom_openapi() -> dict:
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=settings.app_name,
        version="0.1.0",
        routes=app.routes,
    )
    for schema in openapi_schema.get("components", {}).get("schemas", {}).values():
        for property_schema in schema.get("properties", {}).values():
            items_schema = property_schema.get("items")
            if items_schema and items_schema.get("contentMediaType") == "application/octet-stream":
                items_schema.pop("contentMediaType", None)
                items_schema["format"] = "binary"

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi  # type: ignore[method-assign]


@app.get("/health", response_model=HealthCheckResponse, tags=["health"])
async def healthcheck() -> HealthCheckResponse:
    return HealthCheckResponse(status="ok", app_name=settings.app_name)
