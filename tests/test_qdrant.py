import asyncio
import math
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from qdrant_client import AsyncQdrantClient, models

from aletora.adapters.memory import InMemoryDatabase, InMemoryEventSink, InMemoryRecordStore
from aletora.adapters.qdrant import QdrantIndex
from aletora.memory import Memory
from aletora.models import (
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


@pytest.fixture
async def qdrant():
    client = AsyncQdrantClient(":memory:")
    try:
        yield client
    finally:
        await client.close()


def profile(model="test-model", dimension=3, preprocessing="normalize-v1", chunking="chunks-v1"):
    return EmbeddingProfile(model, dimension, preprocessing, chunking)


def embedder(vector):
    async def embed(_query):
        return vector

    return embed


async def make_index(qdrant, *, embedding_profile=None, vector=(1.0, 0.0, 0.0)):
    index = QdrantIndex(
        qdrant,
        "aletora",
        embedding_profile or profile(),
        embedder(vector),
    )
    await index.ensure_collection()
    return index


async def scan(index, scope, query, *, budget):
    async with index.scan(scope, query, budget=budget) as discovery:
        return tuple([hit async for hit in discovery])


async def test_scan_returns_only_scoped_profile_hits_without_projected_text(qdrant):
    index = await make_index(qdrant)
    scope = Scope("tenant-1", "main")
    await index.upsert(scope, "candidate-1", 4, [1.0, 0.0, 0.0], evidence_id="evidence-1")
    await index.upsert(Scope("tenant-2", "main"), "candidate-2", 2, [1.0, 0.0, 0.0])
    await index.upsert(Scope("tenant-1", "other"), "candidate-3", 1, [1.0, 0.0, 0.0])

    hits = await scan(index, scope, "query", budget=10)

    assert [(hit.candidate_id, hit.revision, hit.profile_id) for hit in hits] == [
        ("candidate-1", 4, index.profile.profile_id)
    ]
    assert not hasattr(hits[0], "text")


async def test_upsert_is_repeatable_and_rebuild_delete_are_scope_isolated(qdrant):
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
    assert len((await scan(index, other_namespace, "query", budget=5))) == 1
    assert len((await scan(index, other_tenant, "query", budget=5))) == 1


async def test_retry_from_a_rebuilt_adapter_replaces_the_same_projection(qdrant):
    original = await make_index(qdrant)
    rebuilt = QdrantIndex(qdrant, "aletora", profile(), embedder([1, 0, 0]))
    scope = Scope("tenant", "main")
    await original.upsert(scope, "candidate", 2, [0, 1, 0])
    await rebuilt.ensure_collection()
    await rebuilt.upsert(scope, "candidate", 2, [1, 0, 0])

    hits = await scan(rebuilt, scope, "query", budget=10)

    assert [(hit.candidate_id, hit.revision) for hit in hits] == [("candidate", 2)]


async def test_late_old_upsert_and_delete_cannot_replace_or_remove_newer_revision(qdrant):
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "candidate", 2, [0, 1, 0])
    await index.upsert(scope, "candidate", 1, [1, 0, 0])

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


async def test_stale_projection_does_not_hide_current_candidate_from_memory(qdrant):
    now = datetime(2026, 10, 13, tzinfo=timezone.utc)
    scope = Scope("tenant", "main")
    embedding_profile = profile()
    index = await make_index(qdrant, vector=[1, 0, 0])
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
        async def resolve(self, request_scope, request_ref):
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


async def test_profiles_are_isolated_and_collection_dimension_is_checked(qdrant):
    first = await make_index(qdrant)
    second = QdrantIndex(qdrant, "aletora", profile(model="other-model"), embedder([1, 0, 0]))
    scope = Scope("tenant", "main")
    await first.upsert(scope, "same-id", 1, [1, 0, 0])
    await second.upsert(scope, "same-id", 1, [1, 0, 0])

    hits = await scan(second, scope, "q", budget=10)
    assert [hit.candidate_id for hit in hits] == ["same-id"]
    await first.delete(scope, "same-id", 1)
    assert (await scan(first, scope, "q", budget=10)) == ()
    assert [hit.candidate_id for hit in (await scan(second, scope, "q", budget=10))] == ["same-id"]
    incompatible = QdrantIndex(qdrant, "aletora", profile(dimension=4), embedder([1, 0, 0, 0]))
    with pytest.raises(ValueError, match="dimension"):
        await incompatible.ensure_collection()


async def test_existing_collection_with_incompatible_distance_is_rejected(qdrant):
    await qdrant.create_collection(
        collection_name="wrong-distance",
        vectors_config=models.VectorParams(size=3, distance=models.Distance.DOT),
    )
    index = QdrantIndex(qdrant, "wrong-distance", profile(), embedder([1, 0, 0]))

    with pytest.raises(ValueError, match="cosine"):
        await index.ensure_collection()


async def test_delete_evidence_invalidates_only_matching_scope_and_profile_points(qdrant):
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
    assert len((await scan(index, Scope("tenant", "other-namespace"), "q", budget=10))) == 1
    assert len((await scan(index, Scope("other", "main"), "q", budget=10))) == 1


async def test_ensure_collection_propagates_unrelated_client_error_without_creating():
    client = AsyncMock(spec=AsyncQdrantClient)
    client.collection_exists.side_effect = RuntimeError("transport is unavailable")
    index = QdrantIndex(client, "aletora", profile(), embedder([1, 0, 0]))

    with pytest.raises(RuntimeError, match="transport is unavailable"):
        await index.ensure_collection()

    client.create_collection.assert_not_awaited()


async def test_ensure_collection_provisions_missing_local_collection(qdrant):
    index = QdrantIndex(qdrant, "fresh-collection", profile(), embedder([1, 0, 0]))

    await index.ensure_collection()

    assert await qdrant.collection_exists("fresh-collection")


@pytest.mark.parametrize("vector", [[1, 2], [1, math.inf, 0], [1, math.nan, 0], [True, 0, 0]])
async def test_upsert_rejects_invalid_vectors(qdrant, vector):
    index = await make_index(qdrant)
    with pytest.raises(ValueError):
        await index.upsert(Scope("tenant", "main"), "candidate", 1, vector)


async def test_scan_rejects_invalid_query_vectors(qdrant):
    index = await make_index(qdrant, vector=[True, 0, 0])
    with pytest.raises(ValueError, match="vector"):
        await scan(index, Scope("tenant", "main"), "query", budget=3)


async def test_scan_discards_malformed_candidate_payload(qdrant):
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "good", 1, [1, 0, 0])
    await qdrant.upsert(
        collection_name="aletora",
        points=[
            models.PointStruct(
                id="2cc77cf3-e8ca-4fda-a4a4-8731b12501a1",
                vector=[1, 0, 0],
                payload={
                    "tenant_id": scope.tenant_id,
                    "namespace": scope.namespace,
                    "profile_id": index.profile.profile_id,
                    "candidate_id": 42,
                    "revision": 1,
                    "text": "never exposed",
                },
            )
        ],
        wait=True,
    )

    page = await scan(index, scope, "query", budget=10)

    assert [hit.candidate_id for hit in page] == ["good"]


@pytest.mark.parametrize("revision", [0, -1, True, 1.5])
async def test_upsert_rejects_invalid_revision(qdrant, revision):
    index = await make_index(qdrant)
    with pytest.raises(ValueError, match="revision"):
        await index.upsert(Scope("tenant", "main"), "candidate", revision, [1, 0, 0])


@pytest.mark.parametrize("revision", [0, -1, True, 1.5])
async def test_delete_rejects_invalid_revision(qdrant, revision):
    index = await make_index(qdrant)
    with pytest.raises(ValueError, match="revision"):
        await index.delete(Scope("tenant", "main"), "candidate", revision)


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
    qdrant, monkeypatch, budget, requests, allowed
):
    now = datetime(2026, 10, 13, tzinfo=timezone.utc)
    scope = Scope("tenant", "main")
    embedding = AsyncMock(return_value=[1.0, 0.0, 0.0])
    index = QdrantIndex(qdrant, "aletora", profile(), embedding)
    await index.ensure_collection()
    database = InMemoryDatabase()
    store, sink = InMemoryRecordStore(database), InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: now)

    async def applied(key):
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
    await index.upsert(scope, safe.id, safe.revision - 1, [0.9, math.sqrt(0.19), 0.0])
    await index.upsert(scope, denied.id, denied.revision, [0.8, 0.6, 0.0])
    await index.upsert(scope, safe.id, safe.revision, [0.7, math.sqrt(0.51), 0.0])
    for number in range(33):
        await index.upsert(scope, f"later-{number}", 1, [0.0, 1.0, 0.0])
    query_points = AsyncMock(wraps=qdrant.query_points)
    monkeypatch.setattr(qdrant, "query_points", query_points)
    resolved = []

    class Resolver:
        async def resolve(self, request_scope, ref):
            assert request_scope == scope
            resolved.append(ref)
            if ref == safe.application.ref:
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
    expected_resolved = [denied.application.ref] if budget >= 34 else []
    if allowed:
        expected_resolved.append(safe.application.ref)
    assert resolved == expected_resolved
    assert await qdrant.collection_exists("aletora")


@pytest.mark.parametrize("count", [0, 32, 33])
async def test_scan_exhausts_empty_full_and_short_raw_pages(qdrant, monkeypatch, count):
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    for number in range(count):
        await index.upsert(scope, f"candidate-{number}", 1, [1.0, 0.0, 0.0])
    query_points = AsyncMock(wraps=qdrant.query_points)
    monkeypatch.setattr(qdrant, "query_points", query_points)

    hits = await scan(index, scope, "query", budget=100)

    assert len(hits) == count
    assert [call.kwargs["offset"] for call in query_points.await_args_list] == (
        [0] if count == 0 else [0, 32]
    )


async def test_concurrent_scans_prepare_distinct_queries_and_have_independent_offsets(
    qdrant, monkeypatch
):
    scope = Scope("tenant", "main")
    embedding = AsyncMock(side_effect=lambda query: [1, 0, 0] if query == "x" else [0, 1, 0])
    index = QdrantIndex(qdrant, "aletora", profile(), embedding)
    await index.ensure_collection()
    for number in range(40):
        await index.upsert(scope, f"x-{number}", 1, [1.0, 0.0, 0.0])
        await index.upsert(scope, f"y-{number}", 1, [0.0, 1.0, 0.0])
    query_points = AsyncMock(wraps=qdrant.query_points)
    monkeypatch.setattr(qdrant, "query_points", query_points)
    both_entered = asyncio.Event()
    entered = 0

    async def collect(query):
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


async def test_scan_context_closes_abandoned_iterator_without_closing_client(qdrant):
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "one", 1, [1.0, 0.0, 0.0])
    await index.upsert(scope, "two", 1, [1.0, 0.0, 0.0])
    async with index.scan(scope, "query", budget=10) as discovery:
        await anext(discovery)
    with pytest.raises(StopAsyncIteration):
        await anext(discovery)
    assert await qdrant.collection_exists("aletora")


async def test_scan_cancellation_closes_local_iteration_and_leaves_client_open(qdrant):
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
    await index.upsert(scope, "one", 1, [1.0, 0.0, 0.0])
    await index.upsert(scope, "two", 1, [1.0, 0.0, 0.0])
    paused = asyncio.Event()
    traversals = []

    async def consume():
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
async def test_scan_rejects_invalid_budget_before_embedding(budget):
    embedding = AsyncMock(return_value=[1.0, 0.0, 0.0])
    client = AsyncMock(spec=AsyncQdrantClient)
    index = QdrantIndex(client, "aletora", profile(), embedding)
    with pytest.raises(ValueError, match="budget"):
        await scan(index, Scope("tenant", "main"), "query", budget=budget)
    embedding.assert_not_awaited()
    client.query_points.assert_not_awaited()


async def test_scan_rejects_backend_overproduction_before_yielding():
    client = AsyncMock(spec=AsyncQdrantClient)
    client.query_points.return_value = SimpleNamespace(points=[object(), object()])
    index = QdrantIndex(client, "aletora", profile(), embedder([1.0, 0.0, 0.0]))
    with pytest.raises(ValueError, match="more raw points"):
        await scan(index, Scope("tenant", "main"), "query", budget=1)
    client.close.assert_not_awaited()


async def test_scan_keeps_invalid_score_failure_explicit_and_propagates_backend_errors():
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
