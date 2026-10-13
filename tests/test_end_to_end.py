"""One composed PostgreSQL lifecycle, Qdrant projection, and authorized retrieval case."""

from __future__ import annotations

import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest

from aletora import (
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

NOW = datetime(2026, 10, 13, tzinfo=timezone.utc)
PROFILE = EmbeddingProfile("golden-embed", 3, "golden-preprocess-v1", "golden-chunks-v1")
POSTGRES_DSN = os.environ.get("ALETORA_TEST_POSTGRES_DSN")


def _asyncpg_dsn(dsn: str) -> str:
    return re.sub(r"^postgres(?:ql)?(?:\+asyncpg)?://", "postgresql://", dsn)


def _sqlalchemy_dsn(dsn: str) -> str:
    if dsn.startswith(("postgresql://", "postgres://")):
        return re.sub(r"^postgres(?:ql)?://", "postgresql+asyncpg://", dsn)
    return dsn


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
async def test_postgres_memory_qdrant_and_request_bound_host_resolution_compose(
    composition_schema: str,
):
    if POSTGRES_DSN is None:
        pytest.skip("ALETORA_TEST_POSTGRES_DSN is not configured")
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("qdrant_client")
    from qdrant_client import AsyncQdrantClient
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from aletora.adapters.qdrant import QdrantIndex
    from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore

    schema = composition_schema
    engine = create_async_engine(_sqlalchemy_dsn(POSTGRES_DSN))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    qdrant = AsyncQdrantClient(":memory:")
    host_formal = f'"{schema}"."host_formal"'
    host_outbox = f'"{schema}"."host_outbox"'
    scope = Scope("tenant-e2e", "policy")
    source_text = "confidential evidence text never returned as formal content"
    formal_text = "Only the host-authorized, versioned policy is returned."
    ref = FormalRef("host-system", "policy", "policy-42", "v1")

    class HostOutbox:
        """Host durability fixture writing through the caller's session."""

        def __init__(self, session: Any) -> None:
            self.session = session

        async def append(self, event: Any) -> None:
            await self.session.execute(
                text(f"INSERT INTO {host_outbox} (event_id, action) VALUES (:id, :action)"),
                {"id": event.id, "action": event.action.value},
            )

    class RequestFormalResolver:
        """A request-bound host resolver enforcing scope, exact version, and role."""

        def __init__(self, session: Any, role: str) -> None:
            self.session = session
            self.role = role

        async def resolve(self, request_scope: Scope, formal_ref: FormalRef):
            if self.role != "policy-reader":
                return None
            row = (
                await self.session.execute(
                    text(
                        f"SELECT content FROM {host_formal} "
                        "WHERE tenant_id = :tenant AND namespace = :namespace "
                        "AND owner = :owner AND object_type = :object_type "
                        "AND object_id = :object_id AND version = :version "
                        "AND allowed_role = :role"
                    ),
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
            ).first()
            if row is None:
                return None
            return ResolvedRecord(formal_ref, row.content)

    async def embed(_query: str) -> list[float]:
        return [1.0, 0.0, 0.0]

    index = QdrantIndex(qdrant, "aletora-e2e", PROFILE, embed)
    try:
        await index.ensure_collection()
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"CREATE TABLE {host_formal} ("
                    "tenant_id text NOT NULL, namespace text NOT NULL, owner text NOT NULL, "
                    "object_type text NOT NULL, object_id text NOT NULL, version text NOT NULL, "
                    "content text NOT NULL, allowed_role text NOT NULL, "
                    "PRIMARY KEY (tenant_id, namespace, owner, object_type, object_id, version))"
                )
            )
            await connection.execute(
                text(
                    f"CREATE TABLE {host_outbox} (event_id text PRIMARY KEY, action text NOT NULL)"
                )
            )

        async with sessions.begin() as session:
            memory = Memory(
                SQLAlchemyRecordStore(session, schema=schema),
                HostOutbox(session),
                clock=lambda: NOW,
            )
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
            await session.execute(
                text(
                    f"INSERT INTO {host_formal} "
                    "(tenant_id, namespace, owner, object_type, object_id, version, content, "
                    "allowed_role) VALUES (:tenant, :namespace, :owner, :object_type, "
                    ":object_id, :version, :content, :role)"
                ),
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

        # Independent host-side read verifies that the formal write and outbox
        # entries were committed alongside the Memory records.
        async with sessions.begin() as session:
            host_record = (await session.execute(text(f"SELECT content FROM {host_formal}"))).one()
            event_count = (
                await session.execute(text(f"SELECT count(*) FROM {host_outbox}"))
            ).scalar_one()
        assert host_record.content == formal_text
        assert event_count == 2

        # A worker uses the public Memory snapshots as source of truth before
        # projecting the accepted/applied revision into the real local Qdrant.
        async with sessions.begin() as session:
            worker = Memory(
                SQLAlchemyRecordStore(session, schema=schema),
                HostOutbox(session),
                clock=lambda: NOW,
            )
            projected_candidate = await worker.get_candidate(scope, applied.id)
            projected_evidence = await worker.get_evidence(scope, evidence.id)
            assert projected_candidate.state is GovernanceState.ACCEPTED
            assert projected_candidate.application.ref == ref
            assert projected_evidence.state.value == "active"
            await index.upsert(
                scope,
                projected_candidate.id,
                projected_candidate.revision,
                [1.0, 0.0, 0.0],
                evidence_id=projected_evidence.id,
            )

        async def retrieve(request_scope: Scope, role: str):
            async with sessions.begin() as session:
                request_memory = Memory(
                    SQLAlchemyRecordStore(session, schema=schema),
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

        # A projection is only a hint: a changed formal version and then a
        # deleted formal object both fail the host's exact-reference check.
        async with sessions.begin() as session:
            await session.execute(
                text(f"UPDATE {host_formal} SET version = 'v2' WHERE object_id = :id"),
                {"id": ref.object_id},
            )
        assert await retrieve(scope, "policy-reader") == ()
        async with sessions.begin() as session:
            await session.execute(
                text(f"DELETE FROM {host_formal} WHERE object_id = :id"),
                {"id": ref.object_id},
            )
        assert await retrieve(scope, "policy-reader") == ()

        # Restore only the host fixture so evidence withdrawal is independently
        # exercised against a still-resolvable formal record and stale Qdrant point.
        async with sessions.begin() as session:
            await session.execute(
                text(
                    f"INSERT INTO {host_formal} "
                    "(tenant_id, namespace, owner, object_type, object_id, version, content, "
                    "allowed_role) VALUES (:tenant, :namespace, :owner, :object_type, "
                    ":object_id, :version, :content, :role)"
                ),
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
            withdrawal_memory = Memory(
                SQLAlchemyRecordStore(session, schema=schema),
                HostOutbox(session),
                clock=lambda: NOW,
            )
            await withdrawal_memory.withdraw_evidence(
                scope,
                evidence.id,
                "source-owner",
                "source withdrawn",
                expected_revision=evidence.revision,
            )

        assert await retrieve(scope, "policy-reader") == ()
        await index.delete_evidence(scope, evidence.id)
        assert (await index.search(scope, "approval policy", limit=5, offset=0)).hits == ()
        assert await retrieve(scope, "policy-reader") == ()
    finally:
        await qdrant.close()
        await engine.dispose()
