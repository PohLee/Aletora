"""Shared pytest setup for optional integration-test environments."""

import os

import pytest


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
