"""Shared pytest setup for optional integration-test environments."""

import os
from collections.abc import AsyncIterator

import pytest
from postgres_host import PostgresHost, open_postgres_host


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Skip PostgreSQL integration cases unless a test database is configured."""
    if os.environ.get("ALETORA_TEST_POSTGRES_DSN"):
        return

    skip_postgres = pytest.mark.skip(
        reason="ALETORA_TEST_POSTGRES_DSN is not configured",
    )
    for item in items:
        if "postgres" in item.keywords:
            item.add_marker(skip_postgres)


@pytest.fixture(params=["sqlalchemy", "tortoise"])
async def postgres_host(request: pytest.FixtureRequest) -> AsyncIterator[PostgresHost]:
    """Run shared host guarantees against each real PostgreSQL transaction adapter."""
    dsn = os.environ.get("ALETORA_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("ALETORA_TEST_POSTGRES_DSN is not configured")
    pytest.importorskip("asyncpg")
    pytest.importorskip(request.param)
    async with open_postgres_host(dsn, request.param) as host:
        yield host
