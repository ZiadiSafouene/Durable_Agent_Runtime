from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.container import AppContainer, build_container
from app.main import create_app
from app.models.tables import Base
from app.runtime.engine import Engine
from app.storage.database import create_engine_from_url, to_sync_url
from app.workers.worker import Worker

TEST_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "mysql+asyncmy://durable:durable@127.0.0.1:3306/durable_agent_test",
)


def mysql_reachable() -> bool:
    try:
        import pymysql

        sync = to_sync_url(TEST_URL)
        # mysql+pymysql://user:pass@host:port/db
        rest = sync.split("://", 1)[1]
        creds, hostdb = rest.split("@", 1)
        user, password = creds.split(":", 1)
        hostport, dbname = hostdb.split("/", 1)
        if ":" in hostport:
            host, port_s = hostport.split(":")
            port = int(port_s)
        else:
            host, port = hostport, 3306
        conn = pymysql.connect(
            host=host, port=port, user=user, password=password, database=dbname, connect_timeout=3
        )
        conn.close()
        return True
    except Exception:
        return False


def _ensure_database() -> None:
    import pymysql

    sync = to_sync_url(TEST_URL)
    rest = sync.split("://", 1)[1]
    creds, hostdb = rest.split("@", 1)
    user, password = creds.split(":", 1)
    hostport, dbname = hostdb.split("/", 1)
    if ":" in hostport:
        host, port = hostport.split(":")
        port = int(port)
    else:
        host, port = hostport, 3306
    conn = pymysql.connect(host=host, port=port, user=user, password=password, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{dbname}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
    finally:
        conn.close()


@pytest.fixture(scope="session")
def mysql_enabled() -> bool:
    try:
        _ensure_database()
        return mysql_reachable()
    except Exception:
        return False


@pytest_asyncio.fixture(scope="session")
async def db_engine(mysql_enabled: bool) -> AsyncIterator[AsyncEngine]:
    if not mysql_enabled:
        pytest.skip("MySQL test database is not reachable")
    engine = create_engine_from_url(TEST_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def session_factory(db_engine: AsyncEngine) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    factory = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)
    async with db_engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(text(f"DELETE FROM `{table.name}`"))
    yield factory


@pytest.fixture
def settings() -> Settings:
    return Settings(
        database_url=TEST_URL,
        embedded_worker=False,
        lease_seconds=15,
        worker_poll_interval_seconds=0.05,
        retry_base_delay_seconds=0.01,
        max_tool_attempts=3,
        snapshot_every_events=4,
        sse_poll_interval_seconds=0.05,
        sse_heartbeat_seconds=30,
    )


@pytest.fixture
def container(settings: Settings, session_factory: async_sessionmaker[AsyncSession]) -> AppContainer:
    c = build_container(settings, database_url=TEST_URL)
    c.session_factory = session_factory
    c.engine.session_factory = session_factory
    c.tool_executor.session_factory = session_factory
    return c


@pytest.fixture
def engine(container: AppContainer) -> Engine:
    return container.engine


@pytest_asyncio.fixture
async def client(container: AppContainer, settings: Settings) -> AsyncIterator[AsyncClient]:
    app = create_app(settings)

    async def lifespan_override():
        app.state.settings = settings
        app.state.container = container
        yield

    app.state.settings = settings
    app.state.container = container
    transport = ASGITransport(app=app, lifespan="off")
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def worker(container: AppContainer, settings: Settings) -> Worker:
    return Worker(container.session_factory, container.engine, settings, worker_id="test-worker")
