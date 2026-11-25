"""Composed PostgreSQL lifecycle, Qdrant projection, and authorized retrieval tests."""

from __future__ import annotations

import os
import re
import uuid
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest

from aletora import (
    Candidate,
    EmbeddingProfile,
    EpistemicKind,
    FormalRef,
    GovernanceState,
    Memory,
    Proposal,
    ResolvedRecord,
    Scope,
    SourceKind,
    SourceRef,
)

NOW = datetime(2026, 10, 13, tzinfo=UTC)
PROFILE = EmbeddingProfile("golden-embed", 3, "golden-preprocess-v1", "golden-chunks-v1")
POSTGRES_DSN = os.environ.get("ALETORA_TEST_POSTGRES_DSN")


def _asyncpg_dsn(dsn: str) -> str:
    return re.sub(r"^postgres(?:ql)?(?:\+asyncpg)?://", "postgresql://", dsn)


def _sqlalchemy_dsn(dsn: str) -> str:
    if dsn.startswith(("postgresql://", "postgres://")):
        return re.sub(r"^postgres(?:ql)?://", "postgresql+asyncpg://", dsn)
    return dsn


def _tortoise_dsn(dsn: str) -> str:
    dsn = re.sub(r"^postgresql\+asyncpg://", "postgres://", dsn)
    return re.sub(r"^postgresql://", "postgres://", dsn)


@pytest.fixture
async def composition_schema():
    if POSTGRES_DSN is None:
        pytest.skip("ALETORA_TEST_POSTGRES_DSN is not configured")
    asyncpg = pytest.importorskip("asyncpg")
    from aletora.migrations import render_migration

    schema = f"aletora_e2e_{uuid.uuid4().hex}"
    connection = await asyncpg.connect(_asyncpg_dsn(POSTGRES_DSN))
    created = False
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        created = True
        await connection.execute(render_migration("dedicated.sql", schema=schema))
        yield schema
    finally:
        if created:
            await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await connection.close()


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("adapter", ["sqlalchemy", "tortoise"])
async def test_postgres_memory_qdrant_and_request_bound_host_resolution_compose(
    composition_schema: str, adapter: str
) -> None:
    if POSTGRES_DSN is None:
        pytest.skip("ALETORA_TEST_POSTGRES_DSN is not configured")
    pytest.importorskip(adapter)
    pytest.importorskip("qdrant_client")
    from qdrant_client import AsyncQdrantClient

    from aletora.adapters.qdrant import QdrantIndex, reconcile_event

    schema = composition_schema
    qdrant = AsyncQdrantClient(":memory:")
    host_formal = f'"{schema}"."host_formal"'
    host_outbox = f'"{schema}"."host_outbox"'
    scope = Scope("tenant-e2e", "policy")
    source_text = "confidential evidence text never returned as formal content"
    formal_text = "Only the host-authorized, versioned policy is returned."
    ref = FormalRef("host-system", "policy", "policy-42", "v1")

    async with AsyncExitStack() as stack:
        stack.push_async_callback(qdrant.close)
        if adapter == "sqlalchemy":
            from sqlalchemy import text
            from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

            from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore

            engine = create_async_engine(_sqlalchemy_dsn(POSTGRES_DSN))
            stack.push_async_callback(engine.dispose)
            sessions = async_sessionmaker(engine, expire_on_commit=False)

            async def run_sql(
                session: Any, statement: str, params: dict[str, Any] | None = None
            ) -> list[dict[str, Any]]:
                res = await session.execute(text(statement), params or {})
                if res.returns_rows:
                    return [dict(row) for row in res.mappings().all()]
                return []

            def make_store(session: Any) -> Any:
                return SQLAlchemyRecordStore(session, schema=schema)

            @asynccontextmanager
            async def host_tx():
                async with sessions.begin() as session:
                    yield session

        else:
            from tortoise import Tortoise
            from tortoise.transactions import in_transaction

            from aletora.adapters.tortoise import TortoiseRecordStore

            await Tortoise.init(db_url=_tortoise_dsn(POSTGRES_DSN), modules={"models": []})
            stack.push_async_callback(Tortoise.close_connections)

            async def run_sql(
                session: Any, statement: str, params: dict[str, Any] | None = None
            ) -> list[dict[str, Any]]:
                values: list[Any] = []
                bound = params or {}

                def positional(match: re.Match[str]) -> str:
                    values.append(bound[match.group(1)])
                    return f"${len(values)}"

                query = re.sub(r"(?<!:):([a-z_][a-z0-9_]*)", positional, statement)
                rows: list[dict[str, Any]] = await session.execute_query_dict(query, values)
                return rows

            def make_store(session: Any) -> Any:
                return TortoiseRecordStore(session, schema=schema)

            @asynccontextmanager
            async def host_tx():
                async with in_transaction() as connection:
                    yield connection

        class HostOutbox:
            """Host durability fixture writing through the caller's transaction."""

            def __init__(self, session: Any) -> None:
                self.session = session

            async def append(self, event: Any) -> None:
                await run_sql(
                    self.session,
                    f"INSERT INTO {host_outbox} (event_id, action) VALUES (:id, :action)",
                    {"id": event.id, "action": event.action.value},
                )

        class RequestFormalResolver:
            """A request-bound host resolver enforcing scope, exact version, and role."""

            def __init__(self, session: Any, role: str) -> None:
                self.session = session
                self.role = role

            async def resolve(
                self, request_scope: Scope, formal_ref: FormalRef
            ) -> ResolvedRecord | None:
                if self.role != "policy-reader":
                    return None
                rows = await run_sql(
                    self.session,
                    f"SELECT content FROM {host_formal} "
                    "WHERE tenant_id = :tenant AND namespace = :namespace "
                    "AND owner = :owner AND object_type = :object_type "
                    "AND object_id = :object_id AND version = :version "
                    "AND allowed_role = :role",
                    {
                        "tenant": request_scope.tenant_id,
                        "namespace": request_scope.namespace,
                        "owner": formal_ref.owner,
                        "object_type": formal_ref.object_type,
                        "object_id": formal_ref.object_id,
                        "version": formal_ref.version,
                        "role": self.role,
                    },
                )
                if not rows:
                    return None
                return ResolvedRecord(formal_ref, rows[0]["content"])

        async def embed(_query: str) -> list[float]:
            return [1.0, 0.0, 0.0]

        async def embed_candidate(_candidate: Candidate) -> list[float]:
            return [1.0, 0.0, 0.0]

        index = QdrantIndex(qdrant, f"aletora-e2e-{adapter}", PROFILE, embed)
        await index.ensure_collection()
        async with host_tx() as session:
            await run_sql(
                session,
                f"CREATE TABLE {host_formal} ("
                "tenant_id text NOT NULL, namespace text NOT NULL, owner text NOT NULL, "
                "object_type text NOT NULL, object_id text NOT NULL, version text NOT NULL, "
                "content text NOT NULL, allowed_role text NOT NULL, "
                "PRIMARY KEY (tenant_id, namespace, owner, object_type, object_id, version))",
            )
            await run_sql(
                session,
                f"CREATE TABLE {host_outbox} (event_id text PRIMARY KEY, action text NOT NULL)",
            )

        async with host_tx() as session:
            memory = Memory(make_store(session), HostOutbox(session), clock=lambda: NOW)
            evidence = await memory.record_evidence(
                scope,
                SourceRef(SourceKind.DOCUMENT, "approved-policy-source", "v3", "section:4"),
                source_text,
                idempotency_key="approved-policy-evidence",
                captured_at=NOW,
            )
            candidate = await memory.propose(
                scope,
                evidence.id,
                Proposal(
                    "The policy requires approval.",
                    EpistemicKind.ASSERTED,
                    "policy-author",
                    "deterministic-extractor",
                    "golden-run-1",
                    NOW,
                ),
                idempotency_key="approved-policy-proposal",
            )
            candidate = await memory.review(
                scope,
                candidate.id,
                GovernanceState.ACCEPTED,
                "independent-reviewer",
                "checked against the source",
                expected_revision=candidate.revision,
            )
            await run_sql(
                session,
                f"INSERT INTO {host_formal} "
                "(tenant_id, namespace, owner, object_type, object_id, version, content, "
                "allowed_role) VALUES (:tenant, :namespace, :owner, :object_type, "
                ":object_id, :version, :content, :role)",
                {
                    "tenant": scope.tenant_id,
                    "namespace": scope.namespace,
                    "owner": ref.owner,
                    "object_type": ref.object_type,
                    "object_id": ref.object_id,
                    "version": ref.version,
                    "content": formal_text,
                    "role": "policy-reader",
                },
            )
            applied = await memory.record_application(
                scope,
                candidate.id,
                ref,
                "host-writer",
                expected_revision=candidate.revision,
            )

        # Independent host-side read verifies formal write and outbox entries committed.
        async with host_tx() as session:
            formal_rows = await run_sql(session, f"SELECT content FROM {host_formal}")
            count_rows = await run_sql(session, f"SELECT count(*) AS cnt FROM {host_outbox}")
        assert formal_rows[0]["content"] == formal_text
        assert count_rows[0]["cnt"] == 2

        # Worker enumerates authoritative records and projects via reconcile_event.
        async with host_tx() as session:
            worker = Memory(make_store(session), HostOutbox(session), clock=lambda: NOW)
            listed = await worker.list_candidates(scope, limit=10)
            assert [c.id for c in listed] == [applied.id]
            await index.upsert(
                scope,
                listed[0].id,
                listed[0].revision,
                await embed_candidate(listed[0]),
                evidence_id=evidence.id,
            )

        async def retrieve(request_scope: Scope, role: str):
            async with host_tx() as session:
                request_memory = Memory(
                    make_store(session),
                    HostOutbox(session),
                    index=index,
                    resolver=RequestFormalResolver(session, role),
                    profile=PROFILE,
                    clock=lambda: NOW,
                )
                return await request_memory.retrieve(request_scope, "approval policy", limit=1)

        result = await retrieve(scope, "policy-reader")
        assert len(result) == 1
        hit = result[0]
        assert hit.content == formal_text
        assert hit.ref == ref
        assert hit.candidate_id == applied.id
        assert hit.epistemic_kind is EpistemicKind.ASSERTED
        assert hit.governance_state is GovernanceState.ACCEPTED
        assert hit.profile_id == PROFILE.profile_id
        assert hit.citations[0].evidence_id == evidence.id
        assert hit.citations[0].source == evidence.source
        assert hit.citations[0].content_hash == evidence.content_hash
        assert source_text not in hit.content

        assert await retrieve(Scope(scope.tenant_id, "other-namespace"), "policy-reader") == ()
        assert await retrieve(scope, "unauthorized-role") == ()

        async with host_tx() as session:
            await run_sql(
                session,
                f"UPDATE {host_formal} SET version = 'v2' WHERE object_id = :id",
                {"id": ref.object_id},
            )
        assert await retrieve(scope, "policy-reader") == ()
        async with host_tx() as session:
            await run_sql(
                session,
                f"DELETE FROM {host_formal} WHERE object_id = :id",
                {"id": ref.object_id},
            )
        assert await retrieve(scope, "policy-reader") == ()

        async with host_tx() as session:
            await run_sql(
                session,
                f"INSERT INTO {host_formal} "
                "(tenant_id, namespace, owner, object_type, object_id, version, content, "
                "allowed_role) VALUES (:tenant, :namespace, :owner, :object_type, "
                ":object_id, :version, :content, :role)",
                {
                    "tenant": scope.tenant_id,
                    "namespace": scope.namespace,
                    "owner": ref.owner,
                    "object_type": ref.object_type,
                    "object_id": ref.object_id,
                    "version": ref.version,
                    "content": formal_text,
                    "role": "policy-reader",
                },
            )
            withdrawal_memory = Memory(make_store(session), HostOutbox(session), clock=lambda: NOW)
            await withdrawal_memory.withdraw_evidence(
                scope,
                evidence.id,
                "source-owner",
                "source withdrawn",
                expected_revision=evidence.revision,
            )

        assert await retrieve(scope, "policy-reader") == ()
        async with host_tx() as session:
            worker = Memory(make_store(session), HostOutbox(session), clock=lambda: NOW)
            from aletora.ports import IndexAction, IndexEvent

            await reconcile_event(
                worker,
                index,
                embed_candidate,
                IndexEvent(
                    "invalidate-e2e",
                    scope,
                    IndexAction.INVALIDATE_EVIDENCE,
                    "evidence",
                    evidence.id,
                    evidence.revision + 1,
                ),
            )
        async with index.scan(scope, "approval policy", budget=5) as discovery:
            assert [hit async for hit in discovery] == []
        assert await retrieve(scope, "policy-reader") == ()
