"""Small host-supplied seams for persistence, events, search, and resolution."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from .models import FormalRef, IndexHit, ResolvedRecord, Scope


class IndexAction(StrEnum):
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
        if "\x00" in self.id:
            raise ValueError("id must not contain NUL bytes")
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
            if "\x00" in value:
                raise ValueError(f"{name} must not contain NUL bytes")
        if (
            isinstance(self.revision, bool)
            or not isinstance(self.revision, int)
            or self.revision < 1
        ):
            raise ValueError("revision must be a positive integer")


@runtime_checkable
class RecordStore(Protocol):
    async def get(
        self, scope: Scope, kind: str, record_id: str, *, for_update: bool = False
    ) -> StoredRecord | None:
        """Read a scoped snapshot, optionally locking it until the host transaction ends."""
        ...

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


@runtime_checkable
class ShareLockableRecordStore(RecordStore, Protocol):
    async def get_for_share(self, scope: Scope, kind: str, record_id: str) -> StoredRecord | None:
        """Read a scoped snapshot with a shared row lock until the host transaction ends."""
        ...


@runtime_checkable
class BatchRecordStore(RecordStore, Protocol):
    async def get_many(
        self, scope: Scope, kind: str, record_ids: Sequence[str]
    ) -> Mapping[str, StoredRecord]:
        """Read multiple scoped snapshots in one batch."""
        ...


@runtime_checkable
class EnumerableRecordStore(RecordStore, Protocol):
    async def list_records(
        self,
        scope: Scope,
        kind: str,
        *,
        after_id: str | None = None,
        limit: int = 100,
    ) -> tuple[StoredRecord, ...]:
        """Return keyset-paginated scoped records ordered by ``record_id``."""
        ...


@runtime_checkable
class EventSink(Protocol):
    async def append(self, event: IndexEvent) -> None: ...


@runtime_checkable
class SearchIndex(Protocol):
    def scan(
        self, scope: Scope, query: str, *, budget: int
    ) -> AbstractAsyncContextManager[AsyncIterator[IndexHit]]:
        """Open one bounded, single-use traversal of untrusted discovery hits.

        Prepare the query once and examine at most ``budget`` raw points, including
        malformed points discarded before yielding. Fetch lazily without prefetch;
        exhaustion or budget ends iteration. Each call has independent state.
        Context exit closes traversal-local resources on early stop or failure,
        without closing host-owned dependencies. Errors and cancellation propagate.
        """
        ...


@runtime_checkable
class RecordResolver(Protocol):
    async def resolve(self, scope: Scope, ref: FormalRef) -> ResolvedRecord | None: ...


@runtime_checkable
class BatchRecordResolver(RecordResolver, Protocol):
    async def resolve_many(
        self, scope: Scope, refs: Sequence[FormalRef]
    ) -> Mapping[FormalRef, ResolvedRecord | None]:
        """Authorize and resolve multiple exact formal object versions in one batch."""
        ...
