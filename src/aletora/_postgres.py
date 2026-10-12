"""Shared, parameterized PostgreSQL contract for the SQL adapters."""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from .errors import IdempotencyConflict, NotFound, RevisionConflict
from .models import Scope
from .ports import InsertResult, StoredRecord

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
Executor = Callable[[str, dict[str, Any]], Awaitable[Mapping[str, Any] | None]]


def qualified_table(schema: str | None) -> str:
    if schema is None:
        return '"aletora_records"'
    if not isinstance(schema, str) or not _IDENTIFIER.fullmatch(schema):
        raise ValueError("schema must be a simple PostgreSQL identifier")
    return f'"{schema}"."aletora_records"'


def decode_record(row: Mapping[str, Any]) -> StoredRecord:
    """Normalize SQLAlchemy and asyncpg JSONB row representations."""
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise ValueError("stored Aletora payload must be a JSON object")
    return StoredRecord(id=row["record_id"], revision=row["revision"], payload=payload)


def _scope_params(scope: Scope, kind: str, record_id: str) -> dict[str, Any]:
    return {
        "tenant_id": scope.tenant_id,
        "namespace": scope.namespace,
        "kind": kind,
        "record_id": record_id,
    }


class PostgresRecordContract:
    """SQL and state mapping shared by transaction-specific frontends."""

    def __init__(self, schema: str | None = None) -> None:
        self.table = qualified_table(schema)

    @property
    def get_sql(self) -> str:
        return (
            f"SELECT record_id, revision, payload FROM {self.table} "
            "WHERE tenant_id=:tenant_id AND namespace=:namespace "
            "AND kind=:kind AND record_id=:record_id"
        )

    @property
    def insert_sql(self) -> str:
        return f"""INSERT INTO {self.table}
            (tenant_id, namespace, kind, record_id, idempotency_key, fingerprint, revision, payload)
            VALUES (:tenant_id, :namespace, :kind, :record_id, :idempotency_key, :fingerprint,
                    1, CAST(:payload AS JSONB))
            ON CONFLICT DO NOTHING
            RETURNING record_id, revision, payload"""

    @property
    def by_key_sql(self) -> str:
        return f"""SELECT record_id, revision, payload, fingerprint FROM {self.table}
            WHERE tenant_id=:tenant_id AND namespace=:namespace AND kind=:kind
              AND idempotency_key=:idempotency_key"""

    @property
    def by_id_sql(self) -> str:
        return (
            f"SELECT record_id, revision, payload, fingerprint FROM {self.table} "
            "WHERE tenant_id=:tenant_id AND namespace=:namespace "
            "AND kind=:kind AND record_id=:record_id"
        )

    @property
    def update_sql(self) -> str:
        return f"""UPDATE {self.table}
            SET payload=CAST(:payload AS JSONB), revision=revision+1
            WHERE tenant_id=:tenant_id AND namespace=:namespace AND kind=:kind
              AND record_id=:record_id AND revision=:expected_revision
            RETURNING record_id, revision, payload"""

    def key_params(
        self, scope: Scope, kind: str, record_id: str, idempotency_key: str
    ) -> dict[str, Any]:
        params = _scope_params(scope, kind, record_id)
        params["idempotency_key"] = idempotency_key
        return params

    async def get(
        self, execute: Executor, scope: Scope, kind: str, record_id: str
    ) -> StoredRecord | None:
        row = await execute(self.get_sql, _scope_params(scope, kind, record_id))
        return decode_record(row) if row is not None else None

    async def insert(
        self,
        execute: Executor,
        scope: Scope,
        kind: str,
        record_id: str,
        idempotency_key: str,
        fingerprint: str,
        payload: dict[str, Any],
    ) -> InsertResult:
        params = self.key_params(scope, kind, record_id, idempotency_key)
        params.update(fingerprint=fingerprint, payload=json.dumps(payload, ensure_ascii=False))
        row = await execute(self.insert_sql, params)
        if row is not None:
            return InsertResult(decode_record(row), created=True)

        winner = await execute(self.by_key_sql, params)
        if winner is not None:
            if winner["fingerprint"] != fingerprint:
                raise IdempotencyConflict("idempotency key was reused with different content")
            return InsertResult(decode_record(winner), created=False)

        # A record-ID collision with a distinct key is not an idempotent retry.
        existing = await execute(self.by_id_sql, params)
        if existing is not None:
            raise IdempotencyConflict("record ID already exists with a different idempotency key")
        # RLS can deliberately hide the conflicting row. Do not disclose its existence.
        raise IdempotencyConflict("insert conflicted with an inaccessible record")

    async def update(
        self,
        execute: Executor,
        scope: Scope,
        kind: str,
        record_id: str,
        expected_revision: int,
        payload: dict[str, Any],
    ) -> StoredRecord:
        params = _scope_params(scope, kind, record_id)
        params.update(
            expected_revision=expected_revision, payload=json.dumps(payload, ensure_ascii=False)
        )
        row = await execute(self.update_sql, params)
        if row is not None:
            return decode_record(row)
        existing = await execute(self.get_sql, _scope_params(scope, kind, record_id))
        if existing is None:
            raise NotFound(f"{kind} record {record_id!r} was not found")
        raise RevisionConflict(f"{kind} record {record_id!r} revision changed")
