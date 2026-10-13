"""Transaction-aware in-memory RecordStore and EventSink test adapters.

Both adapters share an ``InMemoryDatabase``. Its caller-owned transaction context
stages a private snapshot, publishes it only on normal exit, and discards it on
exception. A transaction is single-task and cannot be nested or concurrently reused.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from ..errors import IdempotencyConflict, NotFound, RevisionConflict
from ..models import Scope
from ..ports import EventSink, IndexEvent, InsertResult, RecordStore, StoredRecord

_RecordKey = tuple[str, str, str, str]
_IdempotencyKey = tuple[str, str, str, str]


@dataclass(slots=True)
class _Transaction:
    records: dict[_RecordKey, StoredRecord]
    idempotency: dict[_IdempotencyKey, tuple[str, _RecordKey]]
    events: list[IndexEvent]


class _TransactionContext:
    def __init__(self, database: InMemoryDatabase) -> None:
        self._database = database

    async def __aenter__(self) -> InMemoryDatabase:
        database = self._database
        if database._transaction is not None:
            raise RuntimeError("nested or concurrent in-memory transactions are not supported")
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("an in-memory transaction requires an asyncio task")
        database._owner = task
        database._transaction = _Transaction(
            deepcopy(database._records),
            deepcopy(database._idempotency),
            deepcopy(database._events),
        )
        return database

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        database = self._database
        database._require_transaction()
        if exc_type is None:
            assert database._transaction is not None
            database._records = database._transaction.records
            database._idempotency = database._transaction.idempotency
            database._events = database._transaction.events
        database._transaction = None
        database._owner = None
        return False


class InMemoryDatabase:
    """Shared state and caller-owned transaction context for the memory adapters."""

    def __init__(self) -> None:
        self._records: dict[_RecordKey, StoredRecord] = {}
        self._idempotency: dict[_IdempotencyKey, tuple[str, _RecordKey]] = {}
        self._events: list[IndexEvent] = []
        self._transaction: _Transaction | None = None
        self._owner: asyncio.Task[Any] | None = None

    def transaction(self) -> _TransactionContext:
        return _TransactionContext(self)

    @property
    def events(self) -> tuple[IndexEvent, ...]:
        return tuple(deepcopy(self._events))

    def _require_transaction(self) -> _Transaction:
        transaction = self._transaction
        if transaction is None:
            raise RuntimeError("an active caller-owned transaction is required")
        if asyncio.current_task() is not self._owner:
            raise RuntimeError("in-memory transaction cannot be shared across asyncio tasks")
        return transaction


class InMemoryRecordStore(RecordStore):
    def __init__(self, database: InMemoryDatabase) -> None:
        self._database = database

    async def get(
        self, scope: Scope, kind: str, record_id: str, *, for_update: bool = False
    ) -> StoredRecord | None:
        # The transaction already excludes every other task from the database.
        transaction = self._database._require_transaction()
        stored = transaction.records.get((scope.tenant_id, scope.namespace, kind, record_id))
        return deepcopy(stored) if stored is not None else None

    async def insert(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        idempotency_key: str,
        fingerprint: str,
        payload: dict[str, Any],
    ) -> InsertResult:
        transaction = self._database._require_transaction()
        idem_key = (scope.tenant_id, scope.namespace, kind, idempotency_key)
        existing = transaction.idempotency.get(idem_key)
        if existing is not None:
            existing_fingerprint, record_key = existing
            if existing_fingerprint != fingerprint:
                raise IdempotencyConflict(f"idempotency key {idempotency_key!r} was reused")
            record = transaction.records[record_key]
            return InsertResult(deepcopy(record), False)

        record_key = (scope.tenant_id, scope.namespace, kind, record_id)
        if record_key in transaction.records:
            raise IdempotencyConflict(f"record ID {record_id!r} already exists")
        record = StoredRecord(record_id, 1, deepcopy(payload))
        transaction.records[record_key] = record
        transaction.idempotency[idem_key] = (fingerprint, record_key)
        return InsertResult(deepcopy(record), True)

    async def update(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        expected_revision: int,
        payload: dict[str, Any],
    ) -> StoredRecord:
        transaction = self._database._require_transaction()
        record_key = (scope.tenant_id, scope.namespace, kind, record_id)
        stored = transaction.records.get(record_key)
        if stored is None:
            raise NotFound(f"{kind} {record_id!r} was not found")
        if stored.revision != expected_revision:
            raise RevisionConflict(
                f"{kind} {record_id!r} revision is {stored.revision}, expected {expected_revision}"
            )
        updated = StoredRecord(record_id, stored.revision + 1, deepcopy(payload))
        transaction.records[record_key] = updated
        return deepcopy(updated)


class InMemoryEventSink(EventSink):
    def __init__(self, database: InMemoryDatabase, *, failure: Exception | None = None) -> None:
        self._database = database
        self.failure = failure

    async def append(self, event: IndexEvent) -> None:
        transaction = self._database._require_transaction()
        if self.failure is not None:
            raise self.failure
        transaction.events.append(deepcopy(event))
