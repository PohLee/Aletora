"""Tortoise adapter restricted to its caller-owned transaction wrapper."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from .._postgres import Executor, PostgresRecordContract
from ..models import Scope
from ..ports import InsertResult, StoredRecord

_PARAMETER = re.compile(r":([a-z_][a-z0-9_]*)")


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

        # Tortoise has no public transaction-state API. Its PostgreSQL wrapper
        # tracks wrapper finalization and the underlying asyncpg transaction and
        # connection. Check these before every statement so a released wrapper
        # can never fall back into Tortoise's pool/autocommit path.
        if getattr(transaction, "_finalized", True) is not False:
            raise RuntimeError("an active PostgreSQL Tortoise transaction is required")
        underlying_transaction = getattr(transaction, "transaction", None)
        connection = getattr(transaction, "_connection", None)
        is_in_transaction = getattr(connection, "is_in_transaction", None)
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

    async def _execute(
        self, transaction: Any, statement: str, params: dict[str, Any]
    ) -> Mapping[str, Any] | None:
        values: list[Any] = []

        def positional(match: re.Match[str]) -> str:
            values.append(params[match.group(1)])
            return f"${len(values)}"

        query = _PARAMETER.sub(positional, statement)
        rows: list[Mapping[str, Any]] = await transaction.execute_query_dict(query, values)
        return rows[0] if rows else None

    def _execute_for(self, transaction: Any) -> Executor:
        async def execute(statement: str, params: dict[str, Any]) -> Mapping[str, Any] | None:
            return await self._execute(transaction, statement, params)

        return execute

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
