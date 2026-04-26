import os
from collections.abc import AsyncIterator

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient
from psycopg import sql
from sqlalchemy.engine import make_url

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+psycopg://buro_user:buro_password@localhost:5433/buro_test_database",
)
os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key-with-32-plus-bytes")
os.environ.setdefault("APP_DEBUG", "false")
os.environ.setdefault("ADMIN_EMAIL", "admin@buro.com")
os.environ.setdefault("ADMIN_PASSWORD", "AdminPassword123!")


def ensure_test_database(database_url: str) -> None:
    url = make_url(database_url)
    if not url.database:
        return

    connection_kwargs = {
        "host": url.host,
        "dbname": "postgres",
        "user": url.username,
        "password": url.password,
        "autocommit": True,
    }
    if url.port is not None:
        connection_kwargs["port"] = url.port

    with psycopg.connect(**connection_kwargs) as connection:
        exists = connection.execute(
            "select 1 from pg_database where datname = %s",
            (url.database,),
        ).fetchone()
        if exists is None:
            connection.execute(sql.SQL("create database {}").format(sql.Identifier(url.database)))


ensure_test_database(TEST_DATABASE_URL)

from src.core.db.session import engine  # noqa: E402
from src.core.settings import get_settings  # noqa: E402
from src.main import app  # noqa: E402
from src.models.base import Base  # noqa: E402
from src.models.refresh_session import RefreshSession  # noqa: E402,F401
from src.models.user import User  # noqa: E402,F401
from src.services.bootstrap import ensure_configured_admin  # noqa: E402


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
async def reset_database() -> AsyncIterator[None]:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    from src.core.db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        await ensure_configured_admin(session, get_settings())
    yield


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as async_client:
        yield async_client
