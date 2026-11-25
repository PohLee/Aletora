"""Tortoise adapter restricted to its caller-owned asyncpg transaction wrapper."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from .._postgres import Executor, ManyExecutor, PostgresRecordContract
from ..models import GovernanceState, Scope, _governance_state_values
from ..ports import InsertResult, StoredRecord

# Negative lookbehind avoids matching PostgreSQL ``::type`` casts as named params.
_PARAMETER = re.compile(r"(?<!:):([a-z_][a-z0-9_]*)")


class TortoiseRecordStore:
    """Persist records on the transaction returned by ``in_transaction``.

    A bare Tortoise client is intentionally rejected: transaction methods must
    stay on the wrapper-bound connection instead of falling through to a pool.
    """

    def __init__(self, transaction: Any | None = None, *, schema: str | None = None) -> None:
        try:
            from tortoise.backends.base.client import TransactionalDBClient
        except ImportError as error:
            raise ImportError(
                "TortoiseRecordStore requires the optional 'tortoise' dependencies; "
                "install Aletora with `pip install aletora[tortoise]`."
            ) from error
        self._transaction_type = TransactionalDBClient
        self._transaction = transaction
        self._contract = PostgresRecordContract(schema)
        if transaction is not None:
            self._validate_transaction(transaction)

    def _validate_transaction(self, transaction: Any) -> None:
        if not isinstance(transaction, self._transaction_type):
            raise TypeError("transaction must be a Tortoise in_transaction() wrapper")
        capabilities = getattr(transaction, "capabilities", None)
        if getattr(capabilities, "dialect", None) != "postgres":
            raise TypeError("TortoiseRecordStore supports PostgreSQL transaction wrappers only")
        if not hasattr(transaction, "transaction") and hasattr(transaction, "_transaction"):
            raise TypeError(
                "TortoiseRecordStore supports Tortoise's asyncpg PostgreSQL backend only"
            )

        # Tortoise has no public transaction-state API. Its PostgreSQL asyncpg
        # wrapper tracks wrapper finalization and the underlying asyncpg
        # transaction and connection. Check these before every statement so a
        # released wrapper can never fall back into Tortoise's pool/autocommit path.
        finalized = getattr(transaction, "_finalized", None)
        if finalized is None:
            raise RuntimeError("Tortoise transaction wrapper is missing _finalized")
        if finalized is not False:
            raise RuntimeError("an active PostgreSQL Tortoise transaction is required")
        underlying_transaction = getattr(transaction, "transaction", None)
        connection = getattr(transaction, "_connection", None)
        is_in_transaction = getattr(connection, "is_in_transaction", None)
        if connection is not None and not callable(is_in_transaction):
            raise RuntimeError("Tortoise connection is missing callable is_in_transaction")
        if underlying_transaction is None or not callable(is_in_transaction):
            raise RuntimeError("an active PostgreSQL Tortoise transaction is required")
        try:
            active = is_in_transaction()
        except Exception as error:
            raise RuntimeError("an active PostgreSQL Tortoise transaction is required") from error
        if not active:
            raise RuntimeError("an active PostgreSQL Tortoise transaction is required")

    def _active(self, transaction: Any | None) -> Any:
        active = transaction if transaction is not None else self._transaction
        if active is None:
            raise RuntimeError("a Tortoise in_transaction() wrapper is required")
        self._validate_transaction(active)
        return active

    @staticmethod
    def _prepare_query(statement: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
        values: list[Any] = []

        def positional(match: re.Match[str]) -> str:
            values.append(params[match.group(1)])
            return f"${len(values)}"

        return _PARAMETER.sub(positional, statement), values

    async def _execute(
        self, transaction: Any, statement: str, params: dict[str, Any]
    ) -> Mapping[str, Any] | None:
        query, values = self._prepare_query(statement, params)
        rows: list[Mapping[str, Any]] = await transaction.execute_query_dict(query, values)
        return rows[0] if rows else None

    async def _execute_many(
        self, transaction: Any, statement: str, params: dict[str, Any]
    ) -> list[Mapping[str, Any]]:
        query, values = self._prepare_query(statement, params)
        rows: list[Mapping[str, Any]] = await transaction.execute_query_dict(query, values)
        return rows

    def _execute_for(self, transaction: Any) -> Executor:
        async def execute(statement: str, params: dict[str, Any]) -> Mapping[str, Any] | None:
            return await self._execute(transaction, statement, params)

        return execute

    def _execute_many_for(self, transaction: Any) -> ManyExecutor:
        async def execute_many(
            statement: str, params: dict[str, Any]
        ) -> Sequence[Mapping[str, Any]]:
            return await self._execute_many(transaction, statement, params)

        return execute_many

    async def get(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        *,
        for_update: bool = False,
        transaction: Any | None = None,
    ) -> StoredRecord | None:
        active = self._active(transaction)
        return await self._contract.get(
            self._execute_for(active), scope, kind, record_id, for_update=for_update
        )

    async def get_for_share(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        *,
        transaction: Any | None = None,
    ) -> StoredRecord | None:
        active = self._active(transaction)
        return await self._contract.get_for_share(self._execute_for(active), scope, kind, record_id)

    async def get_many(
        self,
        scope: Scope,
        kind: str,
        record_ids: Sequence[str],
        *,
        transaction: Any | None = None,
    ) -> dict[str, StoredRecord]:
        active = self._active(transaction)
        return await self._contract.get_many(
            self._execute_many_for(active), scope, kind, record_ids
        )

    async def list_records(
        self,
        scope: Scope,
        kind: str,
        *,
        after_id: str | None = None,
        limit: int = 100,
        transaction: Any | None = None,
    ) -> tuple[StoredRecord, ...]:
        active = self._active(transaction)
        return await self._contract.list_records(
            self._execute_many_for(active), scope, kind, after_id=after_id, limit=limit
        )

    async def list_candidates_by_state(
        self,
        scope: Scope,
        states: Sequence[GovernanceState],
        *,
        after_id: str | None = None,
        limit: int = 100,
        transaction: Any | None = None,
    ) -> tuple[StoredRecord, ...]:
        active = self._active(transaction)
        return await self._contract.list_candidates(
            self._execute_many_for(active),
            scope,
            "state",
            _governance_state_values(states),
            after_id=after_id,
            limit=limit,
        )

    async def list_candidates_by_evidence(
        self,
        scope: Scope,
        evidence_id: str,
        *,
        after_id: str | None = None,
        limit: int = 100,
        transaction: Any | None = None,
    ) -> tuple[StoredRecord, ...]:
        active = self._active(transaction)
        return await self._contract.list_candidates(
            self._execute_many_for(active),
            scope,
            "evidence_id",
            (evidence_id,),
            after_id=after_id,
            limit=limit,
        )

    async def insert(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        idempotency_key: str,
        fingerprint: str,
        payload: dict[str, Any],
        *,
        transaction: Any | None = None,
    ) -> InsertResult:
        active = self._active(transaction)
        return await self._contract.insert(
            self._execute_for(active), scope, kind, record_id, idempotency_key, fingerprint, payload
        )

    async def update(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        expected_revision: int,
        payload: dict[str, Any],
        *,
        transaction: Any | None = None,
    ) -> StoredRecord:
        active = self._active(transaction)
        return await self._contract.update(
            self._execute_for(active), scope, kind, record_id, expected_revision, payload
        )
