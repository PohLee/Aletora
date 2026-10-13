"""SQLAlchemy AsyncSession adapter for a caller-owned PostgreSQL transaction."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from .._postgres import Executor, PostgresRecordContract
from ..models import Scope
from ..ports import InsertResult, StoredRecord

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class SQLAlchemyRecordStore:
    """Persist records using the supplied, already-active AsyncSession.

    The session is never committed, rolled back, or closed by this adapter. It may
    be supplied at construction or per operation; the latter is useful when one
    adapter instance is shared across host transaction scopes.
    """

    def __init__(self, session: AsyncSession | None = None, *, schema: str | None = None) -> None:
        try:
            from sqlalchemy.ext.asyncio import AsyncSession
            from sqlalchemy.sql import text
        except ImportError as error:
            raise ImportError(
                "SQLAlchemyRecordStore requires the optional 'sqlalchemy' dependencies; "
                "install Aletora with `pip install aletora[sqlalchemy]`."
            ) from error
        self._async_session_type = AsyncSession
        self._text = text
        self._session = session
        self._contract = PostgresRecordContract(schema)
        if session is not None:
            self._validate_session(session)

    def _validate_session(self, session: AsyncSession) -> None:
        if not isinstance(session, self._async_session_type):
            raise TypeError("session must be a SQLAlchemy AsyncSession")
        if not session.in_transaction():
            raise RuntimeError("SQLAlchemy AsyncSession must have an active transaction")

    def _active(self, session: AsyncSession | None) -> AsyncSession:
        active = session if session is not None else self._session
        if active is None:
            raise RuntimeError("an active SQLAlchemy AsyncSession transaction is required")
        self._validate_session(active)
        return active

    async def _execute(
        self, session: AsyncSession, statement: str, params: dict[str, Any]
    ) -> Mapping[str, Any] | None:
        clause = self._text(statement)
        connection = await session.connection(bind_arguments={"clause": clause})
        dbapi_connection = (await connection.get_raw_connection()).dbapi_connection
        # SQLAlchemy's logical transaction and get_isolation_level() do not reveal
        # DBAPI AUTOCOMMIT. Read the actual connection, including caller overrides.
        if getattr(dbapi_connection, "autocommit", None) is not False:
            raise RuntimeError(
                "SQLAlchemy AsyncSession requires a non-AUTOCOMMIT database transaction"
            )
        result = await connection.execute(clause, params)
        row = result.mappings().first()
        return dict(row) if row is not None else None

    async def get(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        *,
        for_update: bool = False,
        session: AsyncSession | None = None,
    ) -> StoredRecord | None:
        active = self._active(session)
        return await self._contract.get(
            self._execute_for(active), scope, kind, record_id, for_update=for_update
        )

    def _execute_for(self, session: AsyncSession) -> Executor:
        async def execute(statement: str, params: dict[str, Any]) -> Mapping[str, Any] | None:
            return await self._execute(session, statement, params)

        return execute

    async def insert(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        idempotency_key: str,
        fingerprint: str,
        payload: dict[str, Any],
        *,
        session: AsyncSession | None = None,
    ) -> InsertResult:
        active = self._active(session)
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
        session: AsyncSession | None = None,
    ) -> StoredRecord:
        active = self._active(session)
        return await self._contract.update(
            self._execute_for(active), scope, kind, record_id, expected_revision, payload
        )
