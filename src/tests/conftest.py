import os
from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from testcontainers.postgres import PostgresContainer

_pg = PostgresContainer("pgvector/pgvector:pg17", driver="psycopg")
_pg.start()

os.environ["DATABASE_URL"] = _pg.get_connection_url()
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-with-32-plus-bytes")
os.environ.setdefault("APP_DEBUG", "false")
os.environ.setdefault("ADMIN_EMAIL", "admin@buro.com")
os.environ.setdefault("ADMIN_PASSWORD", "AdminPassword123!")
os.environ.setdefault("OCR_ENABLED", "false")

from src.core.db.session import engine  # noqa: E402
from src.core.settings import get_settings  # noqa: E402
from src.main import app  # noqa: E402
from src.models.base import Base  # noqa: E402
from src.models.institution import Department, Institution  # noqa: E402,F401
from src.models.program import (  # noqa: E402,F401
    EducationProgram,
    ProgramDirectorySnapshot,
    ProgramDocument,
    ProgramImportRun,
)
from src.models.refresh_session import RefreshSession  # noqa: E402,F401
from src.models.user import User  # noqa: E402,F401
from src.services.bootstrap import ensure_configured_admin  # noqa: E402
from src.services.institution import ensure_institutions_seeded  # noqa: E402


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:  # noqa: ARG001
    _pg.stop()


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
async def reset_database() -> AsyncIterator[None]:
    async with engine.begin() as connection:
        await connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        await connection.execute(text("CREATE EXTENSION IF NOT EXISTS unaccent"))
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    from src.core.db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        await ensure_configured_admin(session, get_settings())
        await ensure_institutions_seeded(session)
    yield


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as async_client:
        yield async_client
