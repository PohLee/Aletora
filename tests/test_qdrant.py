import array
import asyncio
import math
import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from qdrant_client import AsyncQdrantClient, models

from aletora.adapters.memory import InMemoryDatabase, InMemoryEventSink, InMemoryRecordStore
from aletora.adapters.qdrant import QdrantIndex, reconcile_event
from aletora.memory import Memory
from aletora.models import (
    Candidate,
    EmbeddingProfile,
    EpistemicKind,
    FormalRef,
    GovernanceState,
    Proposal,
    ResolvedRecord,
    Scope,
    SourceKind,
    SourceRef,
)
from aletora.ports import IndexAction, IndexEvent


@pytest.fixture
async def qdrant():
    client = AsyncQdrantClient(":memory:")
    try:
        yield client
    finally:
        await client.close()


def profile(
    model: str = "test-model",
    dimension: int = 3,
    preprocessing: str = "normalize-v1",
    chunking: str = "chunks-v1",
) -> EmbeddingProfile:
    return EmbeddingProfile(model, dimension, preprocessing, chunking)


def embedder(vector):
    async def embed(_query: str):
        return vector

    return embed


async def make_index(
    qdrant: AsyncQdrantClient,
    *,
    embedding_profile: EmbeddingProfile | None = None,
    vector: tuple[float, ...] = (1.0, 0.0, 0.0),
) -> QdrantIndex:
    index = QdrantIndex(
        qdrant,
        "aletora",
        embedding_profile or profile(),
        embedder(vector),
    )
    await index.ensure_collection()
    return index


async def scan(index: QdrantIndex, scope: Scope, query: str, *, budget: int):
    async with index.scan(scope, query, budget=budget) as discovery:
        return tuple([hit async for hit in discovery])


async def test_scan_returns_only_scoped_profile_hits_without_projected_text(
    qdrant: AsyncQdrantClient,
) -> None:
    index = await make_index(qdrant)
    scope = Scope("tenant-1", "main")
    await index.upsert(scope, "candidate-1", 4, [1.0, 0.0, 0.0], evidence_id="evidence-1")
    await index.upsert(
        Scope("tenant-2", "main"), "candidate-2", 2, [1.0, 0.0, 0.0], evidence_id="evidence-2"
    )
    await index.upsert(
        Scope("tenant-1", "other"), "candidate-3", 1, [1.0, 0.0, 0.0], evidence_id="evidence-3"
    )

    hits = await scan(index, scope, "query", budget=10)

    assert [(hit.candidate_id, hit.revision, hit.profile_id) for hit in hits] == [
        ("candidate-1", 4, index.profile.profile_id)
    ]
    assert not hasattr(hits[0], "text")


async def test_upsert_is_repeatable_and_rebuild_delete_are_scope_isolated(
    qdrant: AsyncQdrantClient,
) -> None:
    index = await make_index(qdrant)
    same_scope = Scope("tenant-1", "main")
    other_namespace = Scope("tenant-1", "other")
    other_tenant = Scope("tenant-2", "main")
    await index.upsert(same_scope, "same-id", 1, [1.0, 0.0, 0.0], evidence_id="e1")
    await index.upsert(same_scope, "same-id", 2, [0.0, 1.0, 0.0], evidence_id="e2")
    await index.upsert(other_namespace, "same-id", 1, [1.0, 0.0, 0.0], evidence_id="e1")
    await index.upsert(other_tenant, "same-id", 1, [1.0, 0.0, 0.0], evidence_id="e1")
    await index.delete(same_scope, "same-id", 2)

    assert (await scan(index, same_scope, "query", budget=5)) == ()
    assert len(await scan(index, other_namespace, "query", budget=5)) == 1
    assert len(await scan(index, other_tenant, "query", budget=5)) == 1


async def test_retry_from_a_rebuilt_adapter_replaces_the_same_projection(
    qdrant: AsyncQdrantClient,
) -> None:
    original = await make_index(qdrant)
    rebuilt = QdrantIndex(qdrant, "aletora", profile(), embedder([1, 0, 0]))
    scope = Scope("tenant", "main")
    await original.upsert(scope, "candidate", 2, [0, 1, 0], evidence_id="e1")
    await rebuilt.ensure_collection()
    await rebuilt.upsert(scope, "candidate", 2, [1, 0, 0], evidence_id="e1")

    hits = await scan(rebuilt, scope, "query", budget=10)

    assert [(hit.candidate_id, hit.revision) for hit in hits] == [("candidate", 2)]


async def test_late_old_upsert_and_delete_cannot_replace_or_remove_newer_revision(
    qdrant: AsyncQdrantClient,
) -> None:
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "candidate", 2, [0, 1, 0], evidence_id="e1")
    await index.upsert(scope, "candidate", 1, [1, 0, 0], evidence_id="e1")

    hits = await scan(index, scope, "query", budget=10)
    assert {(hit.candidate_id, hit.revision) for hit in hits} == {
        ("candidate", 1),
        ("candidate", 2),
    }
    await index.delete(scope, "candidate", 1)
    hits = await scan(index, scope, "query", budget=10)
    assert [(hit.candidate_id, hit.revision) for hit in hits] == [("candidate", 2)]

    await index.delete(scope, "candidate", 2)
    assert (await scan(index, scope, "query", budget=10)) == ()


async def test_upsert_prunes_prior_revisions_when_enabled(qdrant: AsyncQdrantClient) -> None:
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "candidate", 1, [1.0, 0.0, 0.0], evidence_id="e1")
    await index.upsert(scope, "candidate", 2, [0.5, 0.5, 0.0], evidence_id="e1")
    await index.upsert(
        scope,
        "candidate",
        3,
        [0.0, 1.0, 0.0],
        evidence_id="e1",
        prune_prior_revisions=True,
    )

    hits = await scan(index, scope, "query", budget=10)
    assert [(hit.candidate_id, hit.revision) for hit in hits] == [("candidate", 3)]


async def test_stale_projection_does_not_hide_current_candidate_from_memory(
    qdrant: AsyncQdrantClient,
) -> None:
    now = datetime(2026, 10, 13, tzinfo=UTC)
    scope = Scope("tenant", "main")
    embedding_profile = profile()
    index = await make_index(qdrant, vector=(1.0, 0.0, 0.0))
    database = InMemoryDatabase()
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: now)
    async with database.transaction():
        evidence = await lifecycle.record_evidence(
            scope,
            SourceRef(SourceKind.DOCUMENT, "doc-1", "v1", "section:1"),
            "evidence text",
            idempotency_key="evidence-1",
            captured_at=now,
        )
        proposed = await lifecycle.propose(
            scope,
            evidence.id,
            Proposal(
                "candidate text",
                EpistemicKind.ASSERTED,
                "author",
                "extractor",
                "run-1",
                now,
            ),
            idempotency_key="candidate-1",
        )
        accepted = await lifecycle.review(
            scope,
            proposed.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=proposed.revision,
        )
        ref = FormalRef("host", "Policy", "policy-1", "v-current")
        applied = await lifecycle.record_application(
            scope, accepted.id, ref, "host", expected_revision=accepted.revision
        )

    await index.upsert(scope, applied.id, applied.revision, [0, 1, 0], evidence_id=evidence.id)
    await index.upsert(scope, applied.id, applied.revision - 1, [1, 0, 0], evidence_id=evidence.id)

    class AuthorizedResolver:
        async def resolve(
            self, request_scope: Scope, request_ref: FormalRef
        ) -> ResolvedRecord | None:
            assert request_scope == scope
            if request_ref == ref:
                return ResolvedRecord(ref, "current authorized policy")
            return None

    retrieval = Memory(
        store,
        sink,
        index=index,
        resolver=AuthorizedResolver(),
        profile=embedding_profile,
        clock=lambda: now,
    )
    async with database.transaction():
        hits = await retrieval.retrieve(scope, "query", limit=1)

    assert len(hits) == 1
    assert hits[0].revision == applied.revision
    assert hits[0].ref == ref
    assert hits[0].content == "current authorized policy"


async def test_profiles_are_isolated_and_collection_dimension_is_checked(
    qdrant: AsyncQdrantClient,
) -> None:
    first = await make_index(qdrant)
    second = QdrantIndex(qdrant, "aletora", profile(model="other-model"), embedder([1, 0, 0]))
    scope = Scope("tenant", "main")
    await first.upsert(scope, "same-id", 1, [1, 0, 0], evidence_id="e1")
    await second.upsert(scope, "same-id", 1, [1, 0, 0], evidence_id="e1")

    hits = await scan(second, scope, "q", budget=10)
    assert [hit.candidate_id for hit in hits] == ["same-id"]
    await first.delete(scope, "same-id", 1)
    assert (await scan(first, scope, "q", budget=10)) == ()
    assert [hit.candidate_id for hit in (await scan(second, scope, "q", budget=10))] == ["same-id"]
    incompatible = QdrantIndex(qdrant, "aletora", profile(dimension=4), embedder([1, 0, 0, 0]))
    with pytest.raises(ValueError, match="dimension"):
        await incompatible.ensure_collection()


async def test_existing_collection_with_incompatible_distance_is_rejected(
    qdrant: AsyncQdrantClient,
) -> None:
    await qdrant.create_collection(
        collection_name="wrong-distance",
        vectors_config=models.VectorParams(size=3, distance=models.Distance.DOT),
    )
    index = QdrantIndex(qdrant, "wrong-distance", profile(), embedder([1, 0, 0]))

    with pytest.raises(ValueError, match="cosine"):
        await index.ensure_collection()


async def test_delete_evidence_invalidates_only_matching_scope_and_profile_points(
    qdrant: AsyncQdrantClient,
) -> None:
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "one", 1, [1, 0, 0], evidence_id="evidence-1")
    await index.upsert(scope, "two", 1, [0, 1, 0], evidence_id="evidence-2")
    await index.upsert(
        Scope("tenant", "other-namespace"),
        "same-evidence-other-namespace",
        1,
        [1, 0, 0],
        evidence_id="evidence-1",
    )
    await index.upsert(Scope("other", "main"), "three", 1, [1, 0, 0], evidence_id="evidence-1")

    await index.delete_evidence(scope, "evidence-1")

    hits = await scan(index, scope, "q", budget=10)
    assert [hit.candidate_id for hit in hits] == ["two"]
    assert len(await scan(index, Scope("tenant", "other-namespace"), "q", budget=10)) == 1
    assert len(await scan(index, Scope("other", "main"), "q", budget=10)) == 1


async def test_ensure_collection_propagates_unrelated_client_error_without_creating() -> None:
    client = AsyncMock(spec=AsyncQdrantClient)
    client.collection_exists.side_effect = RuntimeError("transport is unavailable")
    index = QdrantIndex(client, "aletora", profile(), embedder([1, 0, 0]))

    with pytest.raises(RuntimeError, match="transport is unavailable"):
        await index.ensure_collection()

    client.create_collection.assert_not_awaited()
    client.create_payload_index.assert_not_awaited()


async def test_ensure_collection_creates_payload_indexes_and_handles_concurrent_create_race() -> (
    None
):
    client = AsyncMock(spec=AsyncQdrantClient)
    client.collection_exists.side_effect = [False, True]
    client.create_collection.side_effect = RuntimeError("409 collection already exists")
    client.get_collection.return_value = SimpleNamespace(
        config=SimpleNamespace(
            params=SimpleNamespace(
                vectors=models.VectorParams(size=3, distance=models.Distance.COSINE)
            )
        )
    )
    index = QdrantIndex(client, "aletora", profile(), embedder([1, 0, 0]))

    await index.ensure_collection()

    indexed_fields = {
        call.kwargs["field_name"]: call.kwargs["field_schema"]
        for call in client.create_payload_index.await_args_list
    }
    assert set(indexed_fields) == {
        "tenant_id",
        "namespace",
        "profile_id",
        "candidate_id",
        "evidence_id",
        "revision",
    }
    assert isinstance(indexed_fields["tenant_id"], models.KeywordIndexParams)
    assert indexed_fields["tenant_id"].is_tenant is True
    assert indexed_fields["revision"] == models.PayloadSchemaType.INTEGER


async def test_ensure_collection_does_not_create_indexes_on_incompatible_collection() -> None:
    client = AsyncMock(spec=AsyncQdrantClient)
    client.collection_exists.return_value = True
    client.get_collection.return_value = SimpleNamespace(
        config=SimpleNamespace(
            params=SimpleNamespace(
                vectors=models.VectorParams(size=99, distance=models.Distance.COSINE)
            )
        )
    )
    index = QdrantIndex(client, "aletora", profile(), embedder([1, 0, 0]))

    with pytest.raises(ValueError, match="dimension"):
        await index.ensure_collection()
    client.create_payload_index.assert_not_awaited()


async def test_ensure_collection_provisions_missing_local_collection(
    qdrant: AsyncQdrantClient,
) -> None:
    index = QdrantIndex(qdrant, "fresh-collection", profile(), embedder([1, 0, 0]))

    await index.ensure_collection()

    assert await qdrant.collection_exists("fresh-collection")


@pytest.mark.parametrize(
    "vector",
    [
        [1, 2],
        [1, math.inf, 0],
        [1, math.nan, 0],
        [True, 0, 0],
        bytearray(b"abc"),
        memoryview(b"abc"),
        {"a": 1.0, "b": 0.0, "c": 0.0},
        {1.0, 2.0, 3.0},
    ],
)
async def test_upsert_rejects_invalid_vectors(qdrant: AsyncQdrantClient, vector: Any) -> None:
    index = await make_index(qdrant)
    with pytest.raises(ValueError):
        await index.upsert(Scope("tenant", "main"), "candidate", 1, vector, evidence_id="e1")


async def test_upsert_accepts_array_and_custom_vector_like_inputs(
    qdrant: AsyncQdrantClient,
) -> None:
    class CustomVector:
        def __len__(self) -> int:
            return 3

        def __iter__(self) -> Iterator[float]:
            return iter((0.0, 1.0, 0.0))

    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "arr-cand", 1, array.array("f", [1.0, 0.0, 0.0]), evidence_id="e1")
    await index.upsert(scope, "custom-cand", 1, CustomVector(), evidence_id="e2")

    hits = await scan(index, scope, "query", budget=10)
    assert {hit.candidate_id for hit in hits} == {"arr-cand", "custom-cand"}


async def test_upsert_rejects_empty_or_nul_evidence_id(qdrant: AsyncQdrantClient) -> None:
    index = await make_index(qdrant)
    for bad in (" ", "ev\x001"):
        with pytest.raises(ValueError, match="evidence_id"):
            await index.upsert(
                Scope("tenant", "main"), "candidate", 1, [1.0, 0.0, 0.0], evidence_id=bad
            )


async def test_scan_rejects_invalid_query_vectors(qdrant: AsyncQdrantClient) -> None:
    index = await make_index(qdrant, vector=(True, 0, 0))
    with pytest.raises(ValueError, match="vector"):
        await scan(index, Scope("tenant", "main"), "query", budget=3)


async def test_scan_discards_malformed_candidate_payload(qdrant: AsyncQdrantClient) -> None:
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "good", 1, [0, 1, 0], evidence_id="e1")
    await qdrant.upsert(
        collection_name="aletora",
        points=[
            models.PointStruct(
                id="2cc77cf3-e8ca-4fda-a4a4-8731b12501a1",
                vector=[1.0, 0.0, 0.0],
                payload={
                    "tenant_id": scope.tenant_id,
                    "namespace": scope.namespace,
                    "profile_id": index.profile.profile_id,
                    "candidate_id": 42,
                    "revision": 1,
                    "text": "never exposed",
                },
            ),
            models.PointStruct(
                id="6d34e4cf-4900-4a83-90e1-965f2bb80a5e",
                vector=[1.0, 0.0, 0.0],
                payload={
                    "tenant_id": scope.tenant_id,
                    "namespace": scope.namespace,
                    "profile_id": index.profile.profile_id,
                    "candidate_id": "bad\x00id",
                    "revision": 1,
                },
            ),
        ],
        wait=True,
    )

    assert await scan(index, scope, "query", budget=2) == ()
    page = await scan(index, scope, "query", budget=3)

    assert [hit.candidate_id for hit in page] == ["good"]


@pytest.mark.parametrize("revision", [0, -1, True, 1.5])
async def test_upsert_rejects_invalid_revision(qdrant: AsyncQdrantClient, revision: Any) -> None:
    index = await make_index(qdrant)
    with pytest.raises(ValueError, match="revision"):
        await index.upsert(
            Scope("tenant", "main"), "candidate", revision, [1, 0, 0], evidence_id="e1"
        )


@pytest.mark.parametrize("revision", [0, -1, True, 1.5])
async def test_delete_rejects_invalid_revision(qdrant: AsyncQdrantClient, revision: Any) -> None:
    index = await make_index(qdrant)
    with pytest.raises(ValueError, match="revision"):
        await index.delete(Scope("tenant", "main"), "candidate", revision)


async def test_reconcile_event_and_list_candidates_rebuild(qdrant: AsyncQdrantClient) -> None:
    now = datetime(2026, 10, 13, tzinfo=UTC)
    scope = Scope("tenant", "reconcile")
    database = InMemoryDatabase()
    store, sink = InMemoryRecordStore(database), InMemoryEventSink(database)
    index = await make_index(qdrant)
    memory = Memory(store, sink, clock=lambda: now)

    async def embed_candidate(_candidate: Candidate) -> list[float]:
        return [1.0, 0.0, 0.0]

    async with database.transaction():
        ev = await memory.record_evidence(
            scope,
            SourceRef(SourceKind.DOCUMENT, "doc", "1", "p1"),
            "source",
            idempotency_key="ev",
            captured_at=now,
        )
        c1 = await memory.propose(
            scope,
            ev.id,
            Proposal("c1", EpistemicKind.ASSERTED, "a1", "ext", "run", now),
            idempotency_key="c1",
        )
        c1 = await memory.review(
            scope, c1.id, GovernanceState.ACCEPTED, "rev", "ok", expected_revision=c1.revision
        )
        c1 = await memory.record_application(
            scope,
            c1.id,
            FormalRef("host", "Policy", "p1", "v1"),
            "host",
            expected_revision=c1.revision,
        )

    # Reconcile accepted+applied candidate -> projected with evidence_id.
    async with database.transaction():
        await reconcile_event(
            memory,
            index,
            embed_candidate,
            IndexEvent(
                "evt-1", scope, IndexAction.REFRESH_CANDIDATE, "candidate", c1.id, c1.revision
            ),
        )
    assert [(h.candidate_id, h.revision) for h in await scan(index, scope, "q", budget=10)] == [
        (c1.id, c1.revision)
    ]

    # Withdraw candidate -> reconcile deletes point.
    async with database.transaction():
        withdrawn = await memory.withdraw(
            scope, c1.id, "rev", "retired", expected_revision=c1.revision
        )
        await reconcile_event(
            memory,
            index,
            embed_candidate,
            IndexEvent(
                "evt-2",
                scope,
                IndexAction.DELETE_CANDIDATE,
                "candidate",
                withdrawn.id,
                withdrawn.revision,
            ),
        )
    assert await scan(index, scope, "q", budget=10) == ()

    # Rebuild projects an accepted, applied candidate even after its validity window ends.
    async with database.transaction():
        c2 = await memory.propose(
            scope,
            ev.id,
            Proposal("c2", EpistemicKind.ASSERTED, "a2", "ext", "run", now, valid_until=now),
            idempotency_key="c2",
        )
        c2 = await memory.review(
            scope, c2.id, GovernanceState.ACCEPTED, "rev", "ok", expected_revision=c2.revision
        )
        c2 = await memory.record_application(
            scope,
            c2.id,
            FormalRef("host", "Policy", "p2", "v1"),
            "host",
            expected_revision=c2.revision,
        )
        for listed in await memory.list_candidates(scope, limit=10):
            await reconcile_event(
                memory,
                index,
                embed_candidate,
                IndexEvent(
                    f"rebuild-{listed.id}",
                    scope,
                    IndexAction.REFRESH_CANDIDATE,
                    "candidate",
                    listed.id,
                    listed.revision,
                ),
            )
    assert [(h.candidate_id, h.revision) for h in await scan(index, scope, "q", budget=10)] == [
        (c2.id, c2.revision)
    ]

    async def embed_during_invalidation(_candidate: Candidate) -> list[float]:
        ev_w = await memory.withdraw_evidence(
            scope, ev.id, "owner", "revoked", expected_revision=ev.revision
        )
        await reconcile_event(
            memory,
            index,
            embed_candidate,
            IndexEvent(
                "evt-3",
                scope,
                IndexAction.INVALIDATE_EVIDENCE,
                "evidence",
                ev_w.id,
                ev_w.revision,
            ),
        )
        return await embed_candidate(_candidate)

    # The invalidation worker deletes the existing point before the refresh upserts it again.
    async with database.transaction():
        await reconcile_event(
            memory,
            index,
            embed_during_invalidation,
            IndexEvent(
                "evt-race", scope, IndexAction.REFRESH_CANDIDATE, "candidate", c2.id, c2.revision
            ),
        )
    assert await scan(index, scope, "q", budget=10) == ()

    for action, kind, record_id in (
        (IndexAction.REFRESH_CANDIDATE, "evidence", ev.id),
        (IndexAction.DELETE_CANDIDATE, "evidence", ev.id),
        (IndexAction.INVALIDATE_EVIDENCE, "candidate", c2.id),
    ):
        with pytest.raises(ValueError, match="requires kind"):
            await reconcile_event(
                memory, index, embed_candidate, IndexEvent("bad", scope, action, kind, record_id, 1)
            )


@pytest.mark.parametrize(
    ("budget", "requests", "allowed"),
    [
        (32, [(32, 0)], False),
        (33, [(32, 0), (1, 32)], False),
        (34, [(32, 0), (2, 32)], False),
        (35, [(32, 0), (3, 32)], True),
        (100, [(32, 0), (32, 32)], True),
    ],
)
async def test_memory_traverses_raw_pages_once_and_stops_at_budget_or_success(
    qdrant: AsyncQdrantClient,
    monkeypatch: pytest.MonkeyPatch,
    budget: int,
    requests: list[tuple[int, int]],
    allowed: bool,
) -> None:
    now = datetime(2026, 10, 13, tzinfo=UTC)
    scope = Scope("tenant", "main")
    embedding = AsyncMock(return_value=[1.0, 0.0, 0.0])
    index = QdrantIndex(qdrant, "aletora", profile(), embedding)
    await index.ensure_collection()
    database = InMemoryDatabase()
    store, sink = InMemoryRecordStore(database), InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: now)

    async def applied(key: str) -> Candidate:
        evidence = await lifecycle.record_evidence(
            scope,
            SourceRef(SourceKind.DOCUMENT, key, "v1", "section:1"),
            "private source",
            idempotency_key=f"e-{key}",
            captured_at=now,
        )
        candidate = await lifecycle.propose(
            scope,
            evidence.id,
            Proposal("private candidate", EpistemicKind.ASSERTED, "author", "extractor", key, now),
            idempotency_key=key,
        )
        candidate = await lifecycle.review(
            scope,
            candidate.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=candidate.revision,
        )
        return await lifecycle.record_application(
            scope,
            candidate.id,
            FormalRef("host", "Policy", key, "v1"),
            "host",
            expected_revision=candidate.revision,
        )

    async with database.transaction():
        safe, denied = await applied("safe"), await applied("denied")
    # The first complete raw page has no usable hits. The next starts with an
    # obsolete revision and a denied record before reaching authorized content.
    await qdrant.upsert(
        collection_name="aletora",
        points=[
            models.PointStruct(
                id=str(uuid4()),
                vector=[1.0, 0.0, 0.0],
                payload={
                    "tenant_id": scope.tenant_id,
                    "namespace": scope.namespace,
                    "profile_id": index.profile.profile_id,
                    "candidate_id": 42,
                    "revision": 1,
                },
            )
            for _ in range(32)
        ],
        wait=True,
    )
    await index.upsert(
        scope, safe.id, safe.revision - 1, [0.9, math.sqrt(0.19), 0.0], evidence_id=safe.evidence_id
    )
    await index.upsert(
        scope, denied.id, denied.revision, [0.8, 0.6, 0.0], evidence_id=denied.evidence_id
    )
    await index.upsert(
        scope, safe.id, safe.revision, [0.7, math.sqrt(0.51), 0.0], evidence_id=safe.evidence_id
    )
    for number in range(33):
        await index.upsert(
            scope, f"later-{number}", 1, [0.0, 1.0, 0.0], evidence_id=f"later-ev-{number}"
        )
    query_points = AsyncMock(wraps=qdrant.query_points)
    monkeypatch.setattr(qdrant, "query_points", query_points)
    resolved: list[FormalRef] = []
    assert safe.application is not None
    assert denied.application is not None
    safe_ref = safe.application.ref
    denied_ref = denied.application.ref

    class Resolver:
        async def resolve(self, request_scope: Scope, ref: FormalRef) -> ResolvedRecord | None:
            assert request_scope == scope
            resolved.append(ref)
            if ref == safe_ref:
                return ResolvedRecord(ref, "authorized formal content")
            return None

    memory = Memory(
        store,
        sink,
        index=index,
        resolver=Resolver(),
        profile=index.profile,
        clock=lambda: now,
        scan_budget=budget,
    )
    async with database.transaction():
        hits = await memory.retrieve(scope, "fixed query", limit=1)

    assert [hit.candidate_id for hit in hits] == ([safe.id] if allowed else [])
    assert all(hit.content == "authorized formal content" for hit in hits)
    embedding.assert_awaited_once_with("fixed query")
    calls = [call.kwargs for call in query_points.await_args_list]
    assert [(call["limit"], call["offset"]) for call in calls] == requests
    assert sum(call["limit"] for call in calls) <= budget
    assert all(call["query"] == [1.0, 0.0, 0.0] for call in calls)
    assert all(call["query_filter"] == calls[0]["query_filter"] for call in calls)
    expected_resolved = [denied_ref] if budget >= 34 else []
    if allowed:
        expected_resolved.append(safe_ref)
    assert resolved == expected_resolved
    assert await qdrant.collection_exists("aletora")


@pytest.mark.parametrize("count", [0, 32, 33])
async def test_scan_exhausts_empty_full_and_short_raw_pages(
    qdrant: AsyncQdrantClient, monkeypatch: pytest.MonkeyPatch, count: int
) -> None:
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    for number in range(count):
        await index.upsert(
            scope, f"candidate-{number}", 1, [1.0, 0.0, 0.0], evidence_id=f"ev-{number}"
        )
    query_points = AsyncMock(wraps=qdrant.query_points)
    monkeypatch.setattr(qdrant, "query_points", query_points)

    hits = await scan(index, scope, "query", budget=100)

    assert len(hits) == count
    assert [call.kwargs["offset"] for call in query_points.await_args_list] == (
        [0] if count == 0 else [0, 32]
    )


async def test_concurrent_scans_prepare_distinct_queries_and_have_independent_offsets(
    qdrant: AsyncQdrantClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    scope = Scope("tenant", "main")
    embedding = AsyncMock(side_effect=lambda query: [1, 0, 0] if query == "x" else [0, 1, 0])
    index = QdrantIndex(qdrant, "aletora", profile(), embedding)
    await index.ensure_collection()
    for number in range(40):
        await index.upsert(scope, f"x-{number}", 1, [1.0, 0.0, 0.0], evidence_id=f"ex-{number}")
        await index.upsert(scope, f"y-{number}", 1, [0.0, 1.0, 0.0], evidence_id=f"ey-{number}")
    query_points = AsyncMock(wraps=qdrant.query_points)
    monkeypatch.setattr(qdrant, "query_points", query_points)
    both_entered = asyncio.Event()
    entered = 0

    async def collect(query: str):
        nonlocal entered
        async with index.scan(scope, query, budget=35) as discovery:
            entered += 1
            if entered == 2:
                both_entered.set()
            await both_entered.wait()
            hits = []
            async for hit in discovery:
                hits.append(hit)
                # The local backend can complete awaits synchronously.
                await asyncio.sleep(0)
            return hits

    tasks = [asyncio.create_task(collect(query)) for query in ("x", "y")]
    try:
        async with asyncio.timeout(10):
            x_hits, y_hits = await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    assert len(x_hits) == len(y_hits) == 35
    assert all(hit.candidate_id.startswith("x-") for hit in x_hits)
    assert all(hit.candidate_id.startswith("y-") for hit in y_hits)
    assert sorted(call.args[0] for call in embedding.await_args_list) == ["x", "y"]
    for vector in ([1.0, 0.0, 0.0], [0.0, 1.0, 0.0]):
        calls = [
            call.kwargs for call in query_points.await_args_list if call.kwargs["query"] == vector
        ]
        assert [(call["limit"], call["offset"]) for call in calls] == [(32, 0), (3, 32)]


async def test_scan_context_closes_abandoned_iterator_without_closing_client(
    qdrant: AsyncQdrantClient,
) -> None:
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "one", 1, [1.0, 0.0, 0.0], evidence_id="e1")
    await index.upsert(scope, "two", 1, [1.0, 0.0, 0.0], evidence_id="e2")
    async with index.scan(scope, "query", budget=10) as discovery:
        await anext(discovery)
    with pytest.raises(StopAsyncIteration):
        await anext(discovery)
    assert await qdrant.collection_exists("aletora")


async def test_scan_cancellation_closes_local_iteration_and_leaves_client_open(
    qdrant: AsyncQdrantClient,
) -> None:
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "one", 1, [1.0, 0.0, 0.0], evidence_id="e1")
    await index.upsert(scope, "two", 1, [1.0, 0.0, 0.0], evidence_id="e2")
    paused = asyncio.Event()
    traversals = []

    async def consume() -> None:
        async with index.scan(scope, "query", budget=10) as discovery:
            traversals.append(discovery)
            await anext(discovery)
            paused.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(consume())
    try:
        async with asyncio.timeout(10):
            await paused.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    with pytest.raises(StopAsyncIteration):
        await anext(traversals[0])
    assert await qdrant.collection_exists("aletora")


@pytest.mark.parametrize("budget", [0, -1, True, 1.5])
async def test_scan_rejects_invalid_budget_before_embedding(budget: Any) -> None:
    embedding = AsyncMock(return_value=[1.0, 0.0, 0.0])
    client = AsyncMock(spec=AsyncQdrantClient)
    index = QdrantIndex(client, "aletora", profile(), embedding)
    with pytest.raises(ValueError, match="budget"):
        await scan(index, Scope("tenant", "main"), "query", budget=budget)
    embedding.assert_not_awaited()
    client.query_points.assert_not_awaited()


async def test_scan_rejects_backend_overproduction_before_yielding() -> None:
    client = AsyncMock(spec=AsyncQdrantClient)
    client.query_points.return_value = SimpleNamespace(points=[object(), object()])
    index = QdrantIndex(client, "aletora", profile(), embedder([1.0, 0.0, 0.0]))
    with pytest.raises(ValueError, match="more raw points"):
        await scan(index, Scope("tenant", "main"), "query", budget=1)
    client.close.assert_not_awaited()


async def test_scan_keeps_invalid_score_failure_explicit_and_propagates_backend_errors() -> None:
    client = AsyncMock(spec=AsyncQdrantClient)
    index = QdrantIndex(client, "aletora", profile(), embedder([1.0, 0.0, 0.0]))
    client.query_points.return_value = SimpleNamespace(
        points=[
            SimpleNamespace(
                payload={
                    "tenant_id": "tenant",
                    "namespace": "main",
                    "profile_id": index.profile.profile_id,
                    "candidate_id": "candidate",
                    "revision": 1,
                },
                score=math.nan,
            )
        ]
    )
    with pytest.raises(ValueError, match="score"):
        await scan(index, Scope("tenant", "main"), "query", budget=1)
    client.query_points.side_effect = RuntimeError("backend unavailable")
    with pytest.raises(RuntimeError, match="backend unavailable"):
        await scan(index, Scope("tenant", "main"), "query", budget=1)
    client.close.assert_not_awaited()


@pytest.mark.qdrant_server
@pytest.mark.asyncio
async def test_qdrant_server_creates_payload_indexes_and_filters_accurately() -> None:
    url = os.environ.get("ALETORA_TEST_QDRANT_URL")
    if not url:
        pytest.skip("ALETORA_TEST_QDRANT_URL is not configured")
    collection = f"aletora_srv_{uuid.uuid4().hex}"
    client = AsyncQdrantClient(url=url, check_compatibility=False)
    index = QdrantIndex(client, collection, profile(), embedder([1.0, 0.0, 0.0]))
    scope = Scope("tenant-srv", "main")
    try:
        await index.ensure_collection()
        info = await client.get_collection(collection)
        assert set(info.payload_schema) >= {
            "tenant_id",
            "namespace",
            "profile_id",
            "candidate_id",
            "evidence_id",
            "revision",
        }
        await index.upsert(scope, "c1", 1, [1.0, 0.0, 0.0], evidence_id="ev1")
        await index.upsert(
            scope,
            "c1",
            2,
            [1.0, 0.0, 0.0],
            evidence_id="ev1",
            prune_prior_revisions=True,
        )
        hits = await scan(index, scope, "query", budget=10)
        assert [(h.candidate_id, h.revision) for h in hits] == [("c1", 2)]
        await index.delete_evidence(scope, "ev1")
        assert await scan(index, scope, "query", budget=10) == ()
    finally:
        if await client.collection_exists(collection):
            await client.delete_collection(collection)
        await client.close()
