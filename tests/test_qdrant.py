import math
from unittest.mock import AsyncMock

import pytest
from qdrant_client import AsyncQdrantClient, models

from aletora.adapters.qdrant import QdrantIndex
from aletora.models import EmbeddingProfile, Scope


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


async def test_search_returns_only_scoped_profile_hits_without_projected_text(qdrant):
    index = await make_index(qdrant)
    scope = Scope("tenant-1", "main")
    await index.upsert(scope, "candidate-1", 4, [1.0, 0.0, 0.0], evidence_id="evidence-1")
    await index.upsert(Scope("tenant-2", "main"), "candidate-2", 2, [1.0, 0.0, 0.0])
    await index.upsert(Scope("tenant-1", "other"), "candidate-3", 1, [1.0, 0.0, 0.0])

    hits = await index.search(scope, "query", limit=10, offset=0)

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
    await index.delete(same_scope, "same-id")

    assert await index.search(same_scope, "query", limit=5, offset=0) == ()
    assert len(await index.search(other_namespace, "query", limit=5, offset=0)) == 1
    assert len(await index.search(other_tenant, "query", limit=5, offset=0)) == 1


async def test_retry_from_a_rebuilt_adapter_replaces_the_same_projection(qdrant):
    original = await make_index(qdrant)
    rebuilt = QdrantIndex(qdrant, "aletora", profile(), embedder([1, 0, 0]))
    scope = Scope("tenant", "main")
    await original.upsert(scope, "candidate", 2, [0, 1, 0])
    await rebuilt.ensure_collection()
    await rebuilt.upsert(scope, "candidate", 2, [1, 0, 0])

    hits = await rebuilt.search(scope, "query", limit=10, offset=0)

    assert [(hit.candidate_id, hit.revision) for hit in hits] == [("candidate", 2)]


async def test_profiles_are_isolated_and_collection_dimension_is_checked(qdrant):
    first = await make_index(qdrant)
    second = QdrantIndex(qdrant, "aletora", profile(model="other-model"), embedder([1, 0, 0]))
    scope = Scope("tenant", "main")
    await first.upsert(scope, "same-id", 1, [1, 0, 0])
    await second.upsert(scope, "same-id", 1, [1, 0, 0])

    hits = await second.search(scope, "q", limit=10, offset=0)
    assert [hit.candidate_id for hit in hits] == ["same-id"]
    await first.delete(scope, "same-id")
    assert await first.search(scope, "q", limit=10, offset=0) == ()
    assert [hit.candidate_id for hit in await second.search(scope, "q", limit=10, offset=0)] == [
        "same-id"
    ]
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

    hits = await index.search(scope, "q", limit=10, offset=0)
    assert [hit.candidate_id for hit in hits] == ["two"]
    assert len(await index.search(Scope("tenant", "other-namespace"), "q", limit=10, offset=0)) == 1
    assert len(await index.search(Scope("other", "main"), "q", limit=10, offset=0)) == 1


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


async def test_search_rejects_invalid_query_vectors(qdrant):
    index = await make_index(qdrant, vector=[True, 0, 0])
    with pytest.raises(ValueError, match="vector"):
        await index.search(Scope("tenant", "main"), "query", limit=3, offset=0)


async def test_search_discards_malformed_candidate_payload(qdrant):
    index = await make_index(qdrant)
    scope = Scope("tenant", "main")
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

    assert await index.search(scope, "query", limit=10, offset=0) == ()


@pytest.mark.parametrize("revision", [0, -1, True, 1.5])
async def test_upsert_rejects_invalid_revision(qdrant, revision):
    index = await make_index(qdrant)
    with pytest.raises(ValueError, match="revision"):
        await index.upsert(Scope("tenant", "main"), "candidate", revision, [1, 0, 0])
