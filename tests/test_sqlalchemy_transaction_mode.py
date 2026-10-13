"""Real PostgreSQL checks for host-owned SQLAlchemy transaction modes."""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import pytest

from aletora import (
    EpistemicKind,
    GovernanceState,
    Memory,
    Proposal,
    Scope,
    SourceKind,
    SourceRef,
)
from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore
from aletora.migrations import render_migration

pytestmark = [pytest.mark.postgres, pytest.mark.asyncio]


@pytest.fixture
async def transaction_schema() -> AsyncIterator[tuple[str, str]]:
    dsn = os.environ.get("ALETORA_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("ALETORA_TEST_POSTGRES_DSN is not configured")
    asyncpg = pytest.importorskip("asyncpg")
    pytest.importorskip("sqlalchemy")
    dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)
    dsn = dsn.replace("postgres://", "postgresql://", 1)
    schema = f"aletora_transaction_{uuid.uuid4().hex}"
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(render_migration("dedicated.sql", schema=schema))
        await connection.execute(f'CREATE TABLE "{schema}".host_outbox (id text primary key)')
        yield dsn.replace("postgresql://", "postgresql+asyncpg://", 1), schema
    finally:
        await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await connection.close()


class DiscardEvents:
    async def append(self, event: Any) -> None:
        pass


class FailingOutbox:
    def __init__(self, session: Any, schema: str) -> None:
        self.session = session
        self.schema = schema

    async def append(self, event: Any) -> None:
        from sqlalchemy import text

        await self.session.execute(
            text(f'INSERT INTO "{self.schema}".host_outbox VALUES (:id)'),
            {"id": event.id},
        )
        raise RuntimeError("outbox failed after write")


async def seed_candidate(sessions: Any, schema: str) -> tuple[Scope, Any]:
    scope = Scope("tenant", "transaction-mode")
    async with sessions.begin() as session:
        memory = Memory(SQLAlchemyRecordStore(session, schema=schema), DiscardEvents())
        evidence = await memory.record_evidence(
            scope,
            SourceRef(SourceKind.DOCUMENT, "document", "v1", "page-1"),
            "A documented assertion",
            idempotency_key="evidence",
        )
        candidate = await memory.propose(
            scope,
            evidence.id,
            Proposal(
                "A candidate assertion",
                EpistemicKind.ASSERTED,
                "proposer",
                "extractor",
                "run-1",
                datetime(2026, 10, 13, tzinfo=timezone.utc),
            ),
            idempotency_key="candidate",
        )
    return scope, candidate


@asynccontextmanager
async def configured_session(url: str, mode: str) -> AsyncIterator[Any]:
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.elements import TextClause

    defaults = {"engine-default", "bound-default", "transactional-override"}
    options = {"isolation_level": "AUTOCOMMIT"} if mode in defaults else {}
    engine = create_async_engine(url, **options)
    if mode in {"engine-options", "transactional-option-override"}:
        engine = engine.execution_options(isolation_level="AUTOCOMMIT")
    try:
        async with AsyncExitStack() as stack:
            bind: Any = engine
            session_type: type[Session] = Session
            if mode == "routed-autocommit":
                routed_engine = create_async_engine(url, isolation_level="AUTOCOMMIT")
                stack.push_async_callback(routed_engine.dispose)

                class HostRoutingSession(Session):
                    def get_bind(
                        self, mapper: Any = None, *, clause: Any = None, **kwargs: Any
                    ) -> Any:
                        if isinstance(clause, TextClause):
                            return routed_engine.sync_engine
                        return super().get_bind(mapper=mapper, clause=clause, **kwargs)

                session_type = HostRoutingSession
            if mode in {
                "bound-default",
                "connection-options",
                "transactional-override",
                "transactional-option-override",
            }:
                bind = await stack.enter_async_context(engine.connect())
                if mode == "connection-options":
                    await bind.execution_options(isolation_level="AUTOCOMMIT")
                elif mode.startswith("transactional-"):
                    await bind.execution_options(isolation_level="READ COMMITTED")
            session = await stack.enter_async_context(
                AsyncSession(bind, sync_session_class=session_type)
            )
            await stack.enter_async_context(session.begin())
            if mode == "session-options":
                await session.connection(execution_options={"isolation_level": "AUTOCOMMIT"})
            yield session
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "mode",
    [
        "engine-default",
        "engine-options",
        "bound-default",
        "connection-options",
        "session-options",
        "routed-autocommit",
        "transactional-override",
        "transactional-option-override",
        "transactional-default",
    ],
)
async def test_sqlalchemy_preserves_review_and_outbox_atomicity_in_each_transaction_mode(
    transaction_schema: tuple[str, str],
    mode: str,
) -> None:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    url, schema = transaction_schema
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        scope, candidate = await seed_candidate(sessions, schema)
        with pytest.raises(RuntimeError) as failure:
            async with configured_session(url, mode) as session:
                assert session.in_transaction()
                memory = Memory(
                    SQLAlchemyRecordStore(session, schema=schema), FailingOutbox(session, schema)
                )
                try:
                    await memory.review(
                        scope,
                        candidate.id,
                        GovernanceState.ACCEPTED,
                        "reviewer",
                        "verified",
                        expected_revision=candidate.revision,
                    )
                except RuntimeError:
                    # The host still owns a usable transaction after rejection.
                    assert session.in_transaction()
                    assert (await session.execute(text("SELECT 1"))).scalar_one() == 1
                    raise
        async with sessions.begin() as session:
            memory = Memory(SQLAlchemyRecordStore(session, schema=schema), DiscardEvents())
            persisted = await memory.get_candidate(scope, candidate.id)
            outbox_rows = (
                await session.execute(text(f'SELECT count(*) FROM "{schema}".host_outbox'))
            ).scalar_one()
        assert (persisted.state, persisted.revision, outbox_rows) == (
            GovernanceState.PROPOSED,
            1,
            0,
        )
        if mode.startswith("transactional-"):
            assert str(failure.value) == "outbox failed after write"
        else:
            assert "AUTOCOMMIT" in str(failure.value)
    finally:
        await engine.dispose()
