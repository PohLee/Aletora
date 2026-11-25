"""Exercise the example worker against a real PostgreSQL transaction."""

import os
from importlib import import_module
from pathlib import Path
from unittest.mock import Mock

import pytest

from aletora import IndexEvent, Memory, Scope

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.postgres
@pytest.mark.skipif(not (ROOT / "examples/outbox.sql").is_file(), reason="requires source examples")
async def test_outbox_worker_isolates_failed_events_and_stops_retrying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    monkeypatch.syspath_prepend(str(ROOT))
    worker = import_module("examples.outbox_worker")
    dsn = os.environ["ALETORA_TEST_POSTGRES_DSN"].replace(
        "postgresql://", "postgresql+asyncpg://", 1
    )
    engine = create_async_engine(dsn)
    scope = Scope("tenant", "notes")
    handled: list[str] = []

    async def reconcile(memory: Memory, index: object, embed: object, event: IndexEvent) -> None:
        if event.id == "2-sql-failure":
            # An actual PostgreSQL statement failure aborts the current savepoint.
            await session.execute(text("SELECT 1 / 0"))
        handled.append(event.id)

    monkeypatch.setattr(worker, "reconcile_event", reconcile)

    try:
        async with engine.connect() as connection:
            async with connection.begin():
                ddl = (ROOT / "examples/outbox.sql").read_text()
                await connection.execute(
                    text(ddl.replace("CREATE TABLE", "CREATE TEMP TABLE", 1).split(";")[0])
                )
                await connection.execute(
                    text("""
                    INSERT INTO host_outbox
                        (id, tenant_id, namespace, action, kind, record_id, revision, queued_at)
                    VALUES
                        ('1-bad-action', 'tenant', 'notes', 'unknown',
                         'candidate', 'one', 1, now()),
                        ('2-sql-failure', 'tenant', 'notes', 'delete_candidate',
                         'candidate', 'two', 1, now()),
                        ('3-good', 'tenant', 'notes', 'delete_candidate',
                         'candidate', 'three', 1, now())
                """)
                )
            async with AsyncSession(connection) as session:
                sink, index, embed = Mock(), Mock(), Mock()
                assert await worker.run_batch(session, scope, sink, index, embed) == 1
                assert handled == ["3-good"]
                for _ in range(2):
                    assert await worker.run_batch(session, scope, sink, index, embed) == 0
                assert await worker.run_batch(session, scope, sink, index, embed) == 0
                rows = await session.execute(
                    text("""
                    SELECT id, attempts, last_error, dead_lettered_at IS NOT NULL,
                        processed_at IS NOT NULL
                    FROM host_outbox ORDER BY id
                """)
                )
                assert rows.all() == [
                    ("1-bad-action", 3, "ValueError", True, False),
                    ("2-sql-failure", 3, "DBAPIError", True, False),
                    ("3-good", 0, None, False, True),
                ]
    finally:
        await engine.dispose()
