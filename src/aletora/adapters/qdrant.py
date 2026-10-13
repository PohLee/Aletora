"""Optional Qdrant projection adapter; records remain authoritative in the host store."""

from __future__ import annotations

import json
import math
from collections.abc import Awaitable, Callable, Sequence
from typing import Any
from uuid import UUID, uuid5

try:
    from qdrant_client import AsyncQdrantClient, models
except ModuleNotFoundError as exc:  # pragma: no cover - exercised without the optional extra
    if exc.name is not None and exc.name.startswith("qdrant_client"):
        raise ImportError(
            "QdrantIndex requires the optional dependency; install it with "
            "`pip install aletora[qdrant]`."
        ) from exc
    raise

from ..models import EmbeddingProfile, IndexHit, Scope, SearchPage

Embed = Callable[[str], Awaitable[Sequence[float]]]
_POINT_NAMESPACE = UUID("d731c2e8-cc62-5e5c-8a26-6d722d6144b8")


def _nonempty(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _checked_vector(vector: Sequence[float], dimension: int) -> list[float]:
    if isinstance(vector, (str, bytes)) or not isinstance(vector, Sequence):
        raise ValueError("vector must be a finite numeric sequence of the configured dimension")
    if len(vector) != dimension:
        raise ValueError(f"vector dimension must be {dimension}")
    result: list[float] = []
    for value in vector:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise ValueError("vector values must be finite numbers (booleans are not numbers)")
        result.append(float(value))
    return result


def _revision(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("revision must be a positive integer")


def _scope(scope: Scope) -> None:
    if not isinstance(scope, Scope):
        raise ValueError("scope must be a Scope")


class QdrantIndex:
    """A tenant/profile-filtered, rebuildable vector projection over caller-owned Qdrant."""

    def __init__(
        self,
        client: AsyncQdrantClient,
        collection: str,
        profile: EmbeddingProfile,
        embed: Embed,
    ) -> None:
        self.client = client
        self.collection = _nonempty("collection", collection)
        if not isinstance(profile, EmbeddingProfile):
            raise ValueError("profile must be an EmbeddingProfile")
        if not callable(embed):
            raise ValueError("embed must be an async callable")
        self.profile = profile
        self._embed = embed

    async def ensure_collection(self) -> None:
        """Create the collection if absent; otherwise verify its unnamed vector dimension.

        A collection may hold multiple profile IDs, but every profile must have this same
        vector dimension. Searches always filter to the exact full profile identity.
        """
        if not await self.client.collection_exists(self.collection):
            await self.client.create_collection(
                collection_name=self.collection,
                vectors_config=models.VectorParams(
                    size=self.profile.dimension,
                    distance=models.Distance.COSINE,
                ),
            )
            return
        info = await self.client.get_collection(self.collection)
        vectors = info.config.params.vectors
        if vectors is None or isinstance(vectors, dict):
            raise ValueError("collection must use one unnamed dense-vector configuration")
        if vectors.size != self.profile.dimension:
            raise ValueError(
                f"collection vector dimension {vectors.size} is incompatible with "
                "profile dimension "
                f"{self.profile.dimension}"
            )
        if vectors.distance != models.Distance.COSINE:
            raise ValueError("collection distance must be cosine")

    async def search(
        self,
        scope: Scope,
        query: str,
        *,
        limit: int,
        offset: int,
    ) -> SearchPage:
        """Return validated hits with raw scan accounting.

        `offset` counts raw filtered points already scanned, not validated
        hits. `scanned` reports every raw point examined (including points
        discarded as malformed) and `exhausted` reports whether the
        collection holds no further raw points, so callers keep paging past
        discarded points instead of mistaking a shortened page for the end.
        """
        _scope(scope)
        _nonempty("query", query)
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValueError("offset must be a non-negative integer")
        vector = _checked_vector(await self._embed(query), self.profile.dimension)
        result = await self.client.query_points(
            collection_name=self.collection,
            query=vector,
            query_filter=self._filter(scope),
            limit=limit,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        raw = list(result.points)
        hits: list[IndexHit] = []
        for point in raw:
            payload = point.payload
            if not isinstance(payload, dict) or not self._payload_matches(payload, scope):
                continue
            candidate_id = payload.get("candidate_id")
            revision = payload.get("revision")
            if not isinstance(candidate_id, str) or not candidate_id.strip():
                continue
            if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
                continue
            hits.append(IndexHit(candidate_id, revision, point.score, self.profile.profile_id))
        return SearchPage(tuple(hits), len(raw), len(raw) < limit)

    async def upsert(
        self,
        scope: Scope,
        candidate_id: str,
        revision: int,
        vector: Sequence[float],
        *,
        evidence_id: str | None = None,
    ) -> None:
        _scope(scope)
        _nonempty("candidate_id", candidate_id)
        _revision(revision)
        if evidence_id is not None:
            _nonempty("evidence_id", evidence_id)
        checked = _checked_vector(vector, self.profile.dimension)
        payload: dict[str, Any] = {
            "tenant_id": scope.tenant_id,
            "namespace": scope.namespace,
            "profile_id": self.profile.profile_id,
            "candidate_id": candidate_id,
            "revision": revision,
        }
        if evidence_id is not None:
            payload["evidence_id"] = evidence_id
        await self.client.upsert(
            collection_name=self.collection,
            points=[
                models.PointStruct(
                    id=self._point_id(scope, candidate_id, revision),
                    vector=checked,
                    payload=payload,
                )
            ],
            wait=True,
        )

    async def delete(self, scope: Scope, candidate_id: str, revision: int) -> None:
        _scope(scope)
        _nonempty("candidate_id", candidate_id)
        _revision(revision)
        await self.client.delete(
            collection_name=self.collection,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        *self._scope_conditions(scope),
                        self._match("profile_id", self.profile.profile_id),
                        self._match("candidate_id", candidate_id),
                        models.FieldCondition(
                            key="revision",
                            range=models.Range(lte=revision),
                        ),
                    ]
                )
            ),
            wait=True,
        )

    async def delete_evidence(self, scope: Scope, evidence_id: str) -> None:
        """Remove this profile's scoped projections sourced from withdrawn evidence."""
        _scope(scope)
        _nonempty("evidence_id", evidence_id)
        await self.client.delete(
            collection_name=self.collection,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        *self._scope_conditions(scope),
                        self._match("profile_id", self.profile.profile_id),
                        self._match("evidence_id", evidence_id),
                    ]
                )
            ),
            wait=True,
        )

    def _filter(self, scope: Scope) -> models.Filter:
        return models.Filter(
            must=[
                *self._scope_conditions(scope),
                self._match("profile_id", self.profile.profile_id),
            ]
        )

    @staticmethod
    def _scope_conditions(scope: Scope) -> list[models.FieldCondition]:
        return [
            QdrantIndex._match("tenant_id", scope.tenant_id),
            QdrantIndex._match("namespace", scope.namespace),
        ]

    @staticmethod
    def _match(key: str, value: str) -> models.FieldCondition:
        return models.FieldCondition(key=key, match=models.MatchValue(value=value))

    def _payload_matches(self, payload: dict[str, Any], scope: Scope) -> bool:
        return (
            payload.get("tenant_id") == scope.tenant_id
            and payload.get("namespace") == scope.namespace
            and payload.get("profile_id") == self.profile.profile_id
        )

    def _point_id(self, scope: Scope, candidate_id: str, revision: int) -> str:
        identity = json.dumps(
            [scope.tenant_id, scope.namespace, candidate_id, self.profile.profile_id, revision],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return str(uuid5(_POINT_NAMESPACE, identity))
