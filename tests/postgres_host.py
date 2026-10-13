"""Real test hosts sharing Aletora records, formal writes and a durable outbox."""

from __future__ import annotations

import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Any

from aletora import FormalRef, Memory, Scope
from aletora.migrations import render_migration
from aletora.ports import IndexAction, IndexEvent, RecordStore

Execute = Callable[[str, dict[str, object]], Awaitable[None]]


@dataclass(frozen=True)
class CommittedState:
    """Values read on an independent connection, never the transaction under test."""

    records: int
    formal_refs: tuple[FormalRef, ...]
    events: tuple[IndexEvent, ...]


@dataclass(frozen=True)
class HostTransaction:
    """The host owns the transaction; Memory and formal writes merely participate."""

    memory: Memory
    _execute: Execute
    _formal_table: str
    _pid: int

    async def write_formal(self, ref: FormalRef) -> None:
        await self._execute(
            f"INSERT INTO {self._formal_table} VALUES (:owner, :object_type, :object_id, :version)",
            {
                "owner": ref.owner,
                "object_type": ref.object_type,
                "object_id": ref.object_id,
                "version": ref.version,
            },
        )


class _Outbox:
    def __init__(self, execute: Execute, table: str, fail_on_append: int | None) -> None:
        self._execute = execute
        self._table = table
        self._fail_on_append = fail_on_append
        self._calls = 0

    async def append(self, event: IndexEvent) -> None:
        await self._execute(
            f"INSERT INTO {self._table} VALUES "
            "(:id, :tenant, :namespace, :action, :kind, :record_id, :revision)",
            {
                "id": event.id,
                "tenant": event.scope.tenant_id,
                "namespace": event.scope.namespace,
                "action": event.action.value,
                "kind": event.kind,
                "record_id": event.record_id,
                "revision": event.revision,
            },
        )
        self._calls += 1
        # Fail after the real insert: rollback must undo this append too.
        if self._calls == self._fail_on_append:
            raise RuntimeError("sink failed")


class PostgresHost:
    """Keep adapter setup, transaction ownership and observation local to one module."""

    def __init__(
        self,
        observer: Any,
        schema: str,
        transaction: Callable[[int | None], AbstractAsyncContextManager[HostTransaction]],
    ) -> None:
        self._observer = observer
        self._schema = schema
        self._transaction = transaction

    def transaction(
        self, *, fail_on_append: int | None = None
    ) -> AbstractAsyncContextManager[HostTransaction]:
        """Commit on normal exit and roll back on an escaping exception.

        Concurrent Tortoise tasks must be created outside an existing transaction
        context so they own independent transactions instead of nested savepoints.
        """
        return self._transaction(fail_on_append)

    async def committed(self) -> CommittedState:
        # A read-only snapshot keeps these three observations mutually consistent.
        async with self._observer.transaction(isolation="repeatable_read", readonly=True):
            records = await self._observer.fetchval(
                f'SELECT count(*) FROM "{self._schema}".aletora_records'
            )
            formal_rows = await self._observer.fetch(
                f'SELECT * FROM "{self._schema}".host_formal '
                "ORDER BY owner, object_type, object_id, version"
            )
            event_rows = await self._observer.fetch(
                f'SELECT * FROM "{self._schema}".host_outbox ORDER BY record_id, revision, id'
            )
        return CommittedState(
            records,
            tuple(FormalRef(**dict(row)) for row in formal_rows),
            tuple(
                IndexEvent(
                    row["id"],
                    Scope(row["tenant_id"], row["namespace"]),
                    IndexAction(row["action"]),
                    row["kind"],
                    row["record_id"],
                    row["revision"],
                )
                for row in event_rows
            ),
        )

    async def is_blocked(self, transaction: HostTransaction, *, by: HostTransaction) -> bool:
        """Observe PostgreSQL lock scheduling without exposing ORM connections to tests."""
        return bool(
            await self._observer.fetchval(
                "SELECT $1::integer = ANY(pg_blocking_pids($2::integer))",
                by._pid,
                transaction._pid,
            )
        )


@asynccontextmanager
async def open_postgres_host(dsn: str, adapter: str) -> AsyncIterator[PostgresHost]:
    """Install an isolated schema and clean up all resources, including on setup failure."""
    import asyncpg

    dsn = re.sub(r"^postgres(?:ql)?(?:\+asyncpg)?://", "postgresql://", dsn)
    schema = f"aletora_host_{uuid.uuid4().hex}"
    formal_table = f'"{schema}".host_formal'
    outbox_table = f'"{schema}".host_outbox'
    async with AsyncExitStack() as cleanup:
        observer = await asyncpg.connect(dsn)
        cleanup.push_async_callback(observer.close)
        await observer.execute(f'CREATE SCHEMA "{schema}"')
        cleanup.push_async_callback(observer.execute, f'DROP SCHEMA "{schema}" CASCADE')
        await observer.execute(render_migration("dedicated.sql", schema=schema))
        await observer.execute(
            f"CREATE TABLE {formal_table} (owner text NOT NULL, object_type text NOT NULL, "
            "object_id text NOT NULL, version text NOT NULL, "
            "PRIMARY KEY (owner, object_type, object_id, version))"
        )
        await observer.execute(
            f"CREATE TABLE {outbox_table} (id text PRIMARY KEY, tenant_id text NOT NULL, "
            "namespace text NOT NULL, action text NOT NULL, kind text NOT NULL, "
            "record_id text NOT NULL, revision integer NOT NULL)"
        )

        def bind(
            store: RecordStore, execute: Execute, pid: int, fail_on_append: int | None
        ) -> HostTransaction:
            return HostTransaction(
                Memory(store, _Outbox(execute, outbox_table, fail_on_append)),
                execute,
                formal_table,
                pid,
            )

        if adapter == "sqlalchemy":
            from sqlalchemy import text
            from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

            from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore

            engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://", 1))
            cleanup.push_async_callback(engine.dispose)
            sessions = async_sessionmaker(engine, expire_on_commit=False)

            @asynccontextmanager
            async def transaction(fail_on_append: int | None) -> AsyncIterator[HostTransaction]:
                async with sessions.begin() as session:
                    pid = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()

                    async def execute(statement: str, params: dict[str, object]) -> None:
                        await session.execute(text(statement), params)

                    yield bind(
                        SQLAlchemyRecordStore(session, schema=schema), execute, pid, fail_on_append
                    )

        elif adapter == "tortoise":
            from tortoise import Tortoise
            from tortoise.transactions import in_transaction

            from aletora.adapters.tortoise import TortoiseRecordStore

            cleanup.push_async_callback(Tortoise.close_connections)
            await Tortoise.init(
                db_url=dsn.replace("postgresql://", "postgres://", 1), modules={"models": []}
            )

            @asynccontextmanager
            async def transaction(fail_on_append: int | None) -> AsyncIterator[HostTransaction]:
                async with in_transaction() as connection:
                    rows = await connection.execute_query_dict("SELECT pg_backend_pid() AS pid")

                    async def execute(statement: str, params: dict[str, object]) -> None:
                        values: list[object] = []

                        def positional(match: re.Match[str]) -> str:
                            values.append(params[match.group(1)])
                            return f"${len(values)}"

                        query = re.sub(r":([a-z_]+)", positional, statement)
                        await connection.execute_query(query, values)

                    yield bind(
                        TortoiseRecordStore(connection, schema=schema),
                        execute,
                        rows[0]["pid"],
                        fail_on_append,
                    )

        else:
            raise ValueError(f"unknown test host adapter: {adapter}")

        yield PostgresHost(observer, schema, transaction)
