"""Small host-supplied seams for persistence, events, search, and resolution."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, Sequence

from .models import FormalRef, IndexHit, ResolvedRecord, Scope


class IndexAction(str, Enum):
    """Event requested projection operation."""

    REFRESH_CANDIDATE = "refresh_candidate"
    DELETE_CANDIDATE = "delete_candidate"
    INVALIDATE_EVIDENCE = "invalidate_evidence"


@dataclass(frozen=True, slots=True)
class StoredRecord:
    id: str
    revision: int
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class InsertResult:
    record: StoredRecord
    created: bool


@dataclass(frozen=True, slots=True)
class IndexEvent:
    id: str
    scope: Scope
    action: IndexAction
    kind: str
    record_id: str
    revision: int

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("id must be a non-empty string")
        if not isinstance(self.scope, Scope):
            raise ValueError("scope must be a Scope")
        if not isinstance(self.action, IndexAction) or self.action not in (
            IndexAction.REFRESH_CANDIDATE,
            IndexAction.DELETE_CANDIDATE,
            IndexAction.INVALIDATE_EVIDENCE,
        ):
            raise ValueError("action must be a valid IndexAction")
        for name in ("kind", "record_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if (
            isinstance(self.revision, bool)
            or not isinstance(self.revision, int)
            or self.revision < 1
        ):
            raise ValueError("revision must be a positive integer")


class RecordStore(Protocol):
    async def get(self, scope: Scope, kind: str, record_id: str) -> StoredRecord | None: ...

    async def insert(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        idempotency_key: str,
        fingerprint: str,
        payload: dict[str, Any],
    ) -> InsertResult: ...

    async def update(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        expected_revision: int,
        payload: dict[str, Any],
    ) -> StoredRecord: ...


class EventSink(Protocol):
    async def append(self, event: IndexEvent) -> None: ...


class SearchIndex(Protocol):
    async def search(
        self, scope: Scope, query: str, *, limit: int, offset: int
    ) -> Sequence[IndexHit]: ...


class RecordResolver(Protocol):
    async def resolve(self, scope: Scope, ref: FormalRef) -> ResolvedRecord | None: ...
