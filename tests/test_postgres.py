"""Real PostgreSQL contract tests and adapter-level guards."""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aletora import GovernanceState

DSN = os.environ.get("ALETORA_TEST_POSTGRES_DSN")
postgres = pytest.mark.postgres


def _schema_name(prefix: str = "aletora_test") -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


@pytest.fixture
async def postgres_schema() -> AsyncIterator[tuple[str, str]]:
    """Create and later remove only a uniquely named schema owned by this test."""
    if not DSN:
        pytest.skip("ALETORA_TEST_POSTGRES_DSN is not configured")
    asyncpg = pytest.importorskip("asyncpg")

    schema = _schema_name()
    connection = await asyncpg.connect(_asyncpg_dsn(DSN))
    created = False
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        created = True
        yield DSN, schema
    finally:
        if created:
            await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await connection.close()


def _asyncpg_dsn(dsn: str) -> str:
    return re.sub(r"^postgresql\+asyncpg://", "postgresql://", dsn)


def _sqlalchemy_dsn(dsn: str) -> str:
    dsn = re.sub(r"^postgres://", "postgresql://", dsn)
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    return dsn


async def _install(dsn: str, schema: str) -> None:
    asyncpg = pytest.importorskip("asyncpg")
    from aletora.migrations import render_migration

    connection = await asyncpg.connect(_asyncpg_dsn(dsn))
    try:
        await connection.execute(render_migration("dedicated.sql", schema=schema))
    finally:
        await connection.close()


@postgres
@pytest.mark.asyncio
async def test_sqlalchemy_store_scopes_records_and_uses_compare_and_swap(
    postgres_schema: tuple[str, str],
) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore
    from aletora.errors import IdempotencyConflict, NotFound, RevisionConflict
    from aletora.models import Scope

    dsn, schema = postgres_schema
    engine = create_async_engine(_sqlalchemy_dsn(dsn))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await _install(dsn, schema)
    store = SQLAlchemyRecordStore(schema=schema)
    scope = Scope("tenant-a", "notes")
    try:
        async with sessions() as connection:
            with pytest.raises(RuntimeError, match="active transaction"):
                await store.get(scope, "candidate", "one", session=connection)

        async with sessions.begin() as connection:
            result = await store.insert(
                scope,
                "candidate",
                "one",
                "idem-1",
                "fingerprint-1",
                {"text": "first"},
                session=connection,
            )
            assert result.created
            assert result.record.revision == 1
            assert result.record.payload == {"text": "first"}
            retry = await store.insert(
                scope,
                "candidate",
                "different-id",
                "idem-1",
                "fingerprint-1",
                {"ignored": True},
                session=connection,
            )
            assert not retry.created
            assert retry.record.id == "one"
            with pytest.raises(IdempotencyConflict):
                await store.insert(
                    scope,
                    "candidate",
                    "other",
                    "idem-1",
                    "changed",
                    {},
                    session=connection,
                )

            assert (
                await store.get(Scope("tenant-b", "notes"), "candidate", "one", session=connection)
                is None
            )
            shared = await store.get_for_share(scope, "candidate", "one", session=connection)
            assert shared is not None and shared.payload == {"text": "first"}

            await store.insert(
                scope, "candidate", "two", "idem-2", "fp-2", {"text": "second"}, session=connection
            )
            batch = await store.get_many(
                scope, "candidate", ["two", "one", "missing", "one"], session=connection
            )
            assert set(batch) == {"one", "two"}
            listed1 = await store.list_records(scope, "candidate", limit=1, session=connection)
            listed2 = await store.list_records(
                scope, "candidate", after_id=listed1[0].id, limit=1, session=connection
            )
            assert [r.id for r in (*listed1, *listed2)] == ["one", "two"]

            updated = await store.update(
                scope,
                "candidate",
                "one",
                1,
                {"text": "second"},
                session=connection,
            )
            assert updated.revision == 2
            with pytest.raises(RevisionConflict):
                await store.update(scope, "candidate", "one", 1, {}, session=connection)
            with pytest.raises(NotFound):
                await store.update(
                    Scope("tenant-b", "notes"),
                    "candidate",
                    "one",
                    2,
                    {},
                    session=connection,
                )
    finally:
        await engine.dispose()


@postgres
@pytest.mark.asyncio
async def test_concurrent_insert_and_cas_have_one_winner(postgres_schema: tuple[str, str]) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore
    from aletora.errors import RevisionConflict
    from aletora.models import Scope

    dsn, schema = postgres_schema
    engine = create_async_engine(_sqlalchemy_dsn(dsn))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await _install(dsn, schema)
    store = SQLAlchemyRecordStore(schema=schema)
    scope = Scope("tenant", "ns")
    try:

        async def insert(payload: dict[str, str]) -> Any:
            async with sessions.begin() as connection:
                return await store.insert(
                    scope,
                    "candidate",
                    "same",
                    "key",
                    "fp",
                    payload,
                    session=connection,
                )

        inserted = await asyncio.gather(insert({"n": "1"}), insert({"n": "2"}))
        assert sum(item.created for item in inserted) == 1
        assert inserted[0].record == inserted[1].record

        async def update() -> str:
            try:
                async with sessions.begin() as connection:
                    await store.update(
                        scope,
                        "candidate",
                        "same",
                        1,
                        {"winner": True},
                        session=connection,
                    )
                return "updated"
            except RevisionConflict:
                return "conflict"

        outcomes = await asyncio.gather(update(), update())
        assert sorted(outcomes) == ["conflict", "updated"]
    finally:
        await engine.dispose()


@postgres
@pytest.mark.asyncio
async def test_dedicated_schema_isolation_preserves_search_path(
    postgres_schema: tuple[str, str],
) -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.sql import text

    from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore
    from aletora.models import Scope

    dsn, first_schema = postgres_schema
    second_schema = _schema_name()
    second_created = False
    engine = create_async_engine(_sqlalchemy_dsn(dsn))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        asyncpg = pytest.importorskip("asyncpg")
        connection = await asyncpg.connect(_asyncpg_dsn(dsn))
        try:
            await connection.execute(f'CREATE SCHEMA "{second_schema}"')
            second_created = True
            from aletora.migrations import render_migration

            await connection.execute(render_migration("dedicated.sql", schema=second_schema))
            await connection.execute(render_migration("dedicated.sql", schema=first_schema))
        finally:
            await connection.close()
        store1 = SQLAlchemyRecordStore(schema=first_schema)
        store2 = SQLAlchemyRecordStore(schema=second_schema)
        search_path_store = SQLAlchemyRecordStore()
        scope = Scope("tenant", "ns")
        async with sessions.begin() as session:
            await session.execute(text(f'SET LOCAL search_path TO "{first_schema}", public'))
            before: Any = (await session.execute(text("SHOW search_path"))).scalar_one()
            await store1.insert(scope, "candidate", "same", "id", "fp", {"v": 1}, session=session)
            await store2.insert(scope, "candidate", "same", "id", "fp", {"v": 2}, session=session)
            await search_path_store.insert(
                scope, "candidate", "via_path", "path1", "fp", {"v": 3}, session=session
            )
            assert (await session.execute(text("SHOW search_path"))).scalar_one() == before
            record1 = await store1.get(scope, "candidate", "same", session=session)
            record2 = await store2.get(scope, "candidate", "same", session=session)
            record3 = await search_path_store.get(scope, "candidate", "via_path", session=session)
            assert record1 is not None and record1.payload == {"v": 1}
            assert record2 is not None and record2.payload == {"v": 2}
            assert record3 is not None and record3.payload == {"v": 3}
    finally:
        if second_created:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{second_schema}" CASCADE'))
        await engine.dispose()


def test_migration_templates_render_safe_identifiers_and_hardening() -> None:
    from aletora._postgres import qualified_table
    from aletora.migrations import read_migration, render_migration

    assert qualified_table("a" * 63) == f'"{"a" * 63}"."aletora_records"'
    with pytest.raises(ValueError, match="63"):
        qualified_table("a" * 64)

    assert '"client_data"."aletora_records"' in render_migration(
        "dedicated.sql", schema="client_data"
    )
    with pytest.raises(ValueError, match="PostgreSQL identifier"):
        render_migration("dedicated.sql", schema='bad"; DROP SCHEMA public; --')

    dedicated = read_migration("dedicated.sql")
    shared = read_migration("shared.sql")
    assert "btrim(tenant_id, U&'" in dedicated
    assert "SET LOCAL app.current_tenant_id" in shared
    assert "NULLIF(current_setting('app.current_tenant_id', true), '')" in shared
    assert "btrim(tenant_id, U&'" in shared


def test_adapter_imports_do_not_load_optional_orms() -> None:
    script = (
        "import sys; import aletora; "
        "import aletora.adapters.sqlalchemy; import aletora.adapters.tortoise; "
        "assert 'sqlalchemy' not in sys.modules; assert 'tortoise' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", script], check=True)


def test_tortoise_prepare_query_preserves_postgres_casts_and_rejects_psycopg_wrapper() -> None:
    pytest.importorskip("tortoise")
    from tortoise.backends.base.client import Capabilities, TransactionalDBClient

    from aletora.adapters.tortoise import TortoiseRecordStore

    query, values = TortoiseRecordStore._prepare_query(
        "SELECT payload::jsonb FROM t WHERE record_id = ANY(:record_ids::text[])",
        {"record_ids": ["a", "b"]},
    )
    assert query == "SELECT payload::jsonb FROM t WHERE record_id = ANY($1::text[])"
    assert values == [["a", "b"]]

    psycopg_wrapper = MagicMock(spec=TransactionalDBClient)
    psycopg_wrapper.capabilities = Capabilities(dialect="postgres")
    psycopg_wrapper._transaction = object()
    psycopg_wrapper._finalized = False

    with pytest.raises(TypeError, match="asyncpg"):
        TortoiseRecordStore(transaction=psycopg_wrapper)


@pytest.mark.asyncio
async def test_sqlalchemy_rejects_non_postgresql_dialect() -> None:
    pytest.importorskip("sqlalchemy")
    from sqlalchemy.ext.asyncio import AsyncSession

    from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore
    from aletora.models import Scope

    fake_connection = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
    session = AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.connection.return_value = fake_connection

    store = SQLAlchemyRecordStore(session)
    with pytest.raises(TypeError, match="PostgreSQL dialects only"):
        await store.get(Scope("t", "n"), "candidate", "one")


@postgres
@pytest.mark.asyncio
async def test_tortoise_adapter_requires_transaction_wrapper_and_uses_it(
    postgres_schema: tuple[str, str],
) -> None:
    pytest.importorskip("tortoise")
    pytest.importorskip("asyncpg")
    from tortoise import Tortoise, connections
    from tortoise.backends.base.client import Capabilities
    from tortoise.transactions import in_transaction

    from aletora.adapters.tortoise import TortoiseRecordStore
    from aletora.errors import IdempotencyConflict
    from aletora.models import Scope

    dsn, schema = postgres_schema
    # Tortoise uses asyncpg URL spelling.
    tortoise_dsn = re.sub(r"^postgresql\+asyncpg://", "postgres://", dsn)
    tortoise_dsn = re.sub(r"^postgresql://", "postgres://", tortoise_dsn)
    await Tortoise.init(db_url=tortoise_dsn, modules={"models": []})
    try:
        await _install(dsn, schema)
        with pytest.raises(TypeError, match=r"Tortoise.*wrapper"):
            TortoiseRecordStore(transaction=connections.get("default"), schema=schema)
        store = TortoiseRecordStore(schema=schema)
        scope = Scope("tenant", "ns")
        transaction_context = in_transaction()
        unbegun_transaction = transaction_context.client
        with pytest.raises(RuntimeError, match="active PostgreSQL Tortoise transaction"):
            await store.get(scope, "candidate", "never-begun", transaction=unbegun_transaction)

        async with transaction_context as transaction:
            original_capabilities = transaction.capabilities
            transaction.capabilities = Capabilities(dialect="sqlite")
            with pytest.raises(TypeError, match="PostgreSQL"):
                await store.get(scope, "candidate", "wrong-dialect", transaction=transaction)
            transaction.capabilities = original_capabilities

            first = await store.insert(
                scope, "candidate", "one", "key", "fp", {"x": 1}, transaction=transaction
            )
            again = await store.insert(
                scope, "candidate", "two", "key", "fp", {}, transaction=transaction
            )
            assert first.created and not again.created
            assert again.record.id == "one"
            with pytest.raises(IdempotencyConflict):
                await store.insert(
                    scope, "candidate", "three", "key", "different", {}, transaction=transaction
                )
            loaded = await store.get(scope, "candidate", "one", transaction=transaction)
            assert loaded is not None and loaded.payload == {"x": 1}
            shared = await store.get_for_share(scope, "candidate", "one", transaction=transaction)
            assert shared is not None and shared.payload == {"x": 1}
            batch = await store.get_many(
                scope, "candidate", ["one", "missing"], transaction=transaction
            )
            assert list(batch) == ["one"]
            listed = await store.list_records(scope, "candidate", limit=10, transaction=transaction)
            assert [r.id for r in listed] == ["one"]
        with pytest.raises(RuntimeError, match="active PostgreSQL Tortoise transaction"):
            await store.insert(
                scope, "candidate", "after-close", "closed-key", "fp", {}, transaction=transaction
            )
        async with in_transaction() as verification:
            assert (
                await store.get(scope, "candidate", "after-close", transaction=verification) is None
            )
    finally:
        await Tortoise.close_connections()


@postgres
@pytest.mark.asyncio
async def test_tortoise_shared_rls_enforces_tenant_and_missing_context(
    postgres_schema: tuple[str, str],
) -> None:
    pytest.importorskip("tortoise")
    asyncpg = pytest.importorskip("asyncpg")
    from tortoise import Tortoise
    from tortoise.exceptions import OperationalError
    from tortoise.transactions import in_transaction

    from aletora.adapters.tortoise import TortoiseRecordStore
    from aletora.errors import NotFound
    from aletora.migrations import render_migration
    from aletora.models import Scope

    dsn, schema = postgres_schema
    role = _schema_name("aletora_tortoise_role")
    admin = await asyncpg.connect(_asyncpg_dsn(dsn))
    role_created = False
    try:
        await admin.execute(f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER NOBYPASSRLS')
        role_created = True
        await admin.execute(render_migration("shared.sql", schema=schema))
        await admin.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
        await admin.execute(
            f'GRANT SELECT, INSERT, UPDATE ON "{schema}"."aletora_records" TO "{role}"'
        )
        flags = await admin.fetchrow(
            "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname=$1", role
        )
        assert dict(flags) == {"rolsuper": False, "rolbypassrls": False}
    except BaseException:
        if role_created:
            relation = await admin.fetchval(
                "SELECT to_regclass($1)", f'"{schema}"."aletora_records"'
            )
            if relation is not None:
                await admin.execute(f'REVOKE ALL ON "{schema}"."aletora_records" FROM "{role}"')
            await admin.execute(f'REVOKE ALL ON SCHEMA "{schema}" FROM "{role}"')
            await admin.execute(f'DROP ROLE "{role}"')
        raise
    finally:
        await admin.close()

    tortoise_dsn = re.sub(r"^postgresql\+asyncpg://", "postgres://", dsn)
    tortoise_dsn = re.sub(r"^postgresql://", "postgres://", tortoise_dsn)
    await Tortoise.init(db_url=tortoise_dsn, modules={"models": []})
    store = TortoiseRecordStore(schema=schema)
    scope = Scope("alpha", "ns")
    try:
        async with in_transaction() as transaction:
            await transaction.execute_query(f'SET LOCAL ROLE "{role}"')
            assert await store.get(scope, "candidate", "one", transaction=transaction) is None
        with pytest.raises(OperationalError):
            async with in_transaction() as transaction:
                await transaction.execute_query(f'SET LOCAL ROLE "{role}"')
                await store.insert(
                    scope, "candidate", "one", "key", "fp", {}, transaction=transaction
                )

        async with in_transaction() as transaction:
            await transaction.execute_query(f'SET LOCAL ROLE "{role}"')
            await transaction.execute_query_dict(
                "SELECT set_config($1, $2, true)", ["app.current_tenant_id", "alpha"]
            )
            assert (
                await store.insert(
                    scope,
                    "candidate",
                    "one",
                    "key",
                    "fp",
                    {"state": "proposed", "evidence_id": "parent"},
                    transaction=transaction,
                )
            ).created
            with pytest.raises(ValueError, match="GovernanceState"):
                await store.list_candidates_by_state(
                    scope,
                    ["proposed"],  # type: ignore[list-item]
                    transaction=transaction,
                )
            assert [
                record.id
                for record in await store.list_candidates_by_state(
                    scope, [GovernanceState.PROPOSED], transaction=transaction
                )
            ] == ["one"]
            assert [
                record.id
                for record in await store.list_candidates_by_evidence(
                    scope, "parent", transaction=transaction
                )
            ] == ["one"]
            assert (
                await store.get_for_share(scope, "candidate", "one", transaction=transaction)
                is not None
            )
            assert list(
                await store.get_many(scope, "candidate", ["one"], transaction=transaction)
            ) == ["one"]
            assert [
                r.id for r in await store.list_records(scope, "candidate", transaction=transaction)
            ] == ["one"]

            await transaction.execute_query_dict(
                "SELECT set_config($1, $2, true)", ["app.current_tenant_id", ""]
            )
            assert await store.get(scope, "candidate", "one", transaction=transaction) is None

            await transaction.execute_query_dict(
                "SELECT set_config($1, $2, true)", ["app.current_tenant_id", "beta"]
            )
            assert await store.get(scope, "candidate", "one", transaction=transaction) is None
            assert (
                await store.list_candidates_by_state(
                    scope, [GovernanceState.PROPOSED], transaction=transaction
                )
                == ()
            )
            assert (
                await store.list_candidates_by_evidence(scope, "parent", transaction=transaction)
                == ()
            )
            with pytest.raises(NotFound):
                await store.update(scope, "candidate", "one", 1, {"x": 2}, transaction=transaction)
        with pytest.raises(OperationalError):
            async with in_transaction() as transaction:
                await transaction.execute_query(f'SET LOCAL ROLE "{role}"')
                await transaction.execute_query_dict(
                    "SELECT set_config($1, $2, true)", ["app.current_tenant_id", "beta"]
                )
                await store.insert(
                    scope, "candidate", "two", "key-2", "fp", {}, transaction=transaction
                )
    finally:
        await Tortoise.close_connections()
        if role_created:
            cleanup = await asyncpg.connect(_asyncpg_dsn(dsn))
            try:
                await cleanup.execute(f'REVOKE ALL ON "{schema}"."aletora_records" FROM "{role}"')
                await cleanup.execute(f'REVOKE ALL ON SCHEMA "{schema}" FROM "{role}"')
                await cleanup.execute(f'DROP ROLE "{role}"')
            finally:
                await cleanup.close()


@postgres
@pytest.mark.asyncio
async def test_tortoise_dedicated_schemas_and_search_path_on_one_transaction(
    postgres_schema: tuple[str, str],
) -> None:
    pytest.importorskip("tortoise")
    asyncpg = pytest.importorskip("asyncpg")
    from tortoise import Tortoise
    from tortoise.transactions import in_transaction

    from aletora.adapters.tortoise import TortoiseRecordStore
    from aletora.migrations import render_migration
    from aletora.models import Scope

    dsn, first_schema = postgres_schema
    second_schema = _schema_name()
    second_created = False
    setup = await asyncpg.connect(_asyncpg_dsn(dsn))
    try:
        await setup.execute(f'CREATE SCHEMA "{second_schema}"')
        second_created = True
        await setup.execute(render_migration("dedicated.sql", schema=first_schema))
        await setup.execute(render_migration("dedicated.sql", schema=second_schema))
    finally:
        await setup.close()

    tortoise_dsn = re.sub(r"^postgresql\+asyncpg://", "postgres://", dsn)
    tortoise_dsn = re.sub(r"^postgresql://", "postgres://", tortoise_dsn)
    await Tortoise.init(db_url=tortoise_dsn, modules={"models": []})
    scope = Scope("same-tenant", "same-namespace")
    try:
        async with in_transaction() as transaction:
            await transaction.execute_query(f'SET LOCAL search_path TO "{first_schema}", public')
            before = await transaction.execute_query_dict("SHOW search_path")
            default_store = TortoiseRecordStore(transaction)
            first_store = TortoiseRecordStore(transaction, schema=first_schema)
            second_store = TortoiseRecordStore(transaction, schema=second_schema)
            await default_store.insert(scope, "candidate", "same-id", "same-key", "fp", {"v": 1})
            await first_store.insert(scope, "candidate", "shared-id", "shared-key", "fp", {"v": 2})
            await second_store.insert(scope, "candidate", "shared-id", "shared-key", "fp", {"v": 3})
            rec_default = await default_store.get(scope, "candidate", "same-id")
            rec_first = await first_store.get(scope, "candidate", "shared-id")
            rec_second = await second_store.get(scope, "candidate", "shared-id")
            assert rec_default is not None and rec_default.payload == {"v": 1}
            assert rec_first is not None and rec_first.payload == {"v": 2}
            assert rec_second is not None and rec_second.payload == {"v": 3}
            after = await transaction.execute_query_dict("SHOW search_path")
            assert after == before
    finally:
        await Tortoise.close_connections()
        if second_created:
            cleanup = await asyncpg.connect(_asyncpg_dsn(dsn))
            try:
                await cleanup.execute(f'DROP SCHEMA "{second_schema}" CASCADE')
            finally:
                await cleanup.close()


@postgres
@pytest.mark.asyncio
async def test_shared_rls_denies_missing_and_cross_tenant_access(
    postgres_schema: tuple[str, str],
) -> None:
    pytest.importorskip("sqlalchemy")
    asyncpg = pytest.importorskip("asyncpg")
    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore
    from aletora.errors import NotFound
    from aletora.migrations import render_migration
    from aletora.models import Scope

    dsn, schema = postgres_schema
    role = _schema_name("aletora_role")
    admin = await asyncpg.connect(_asyncpg_dsn(dsn))
    role_created = False
    try:
        await admin.execute(f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER NOBYPASSRLS')
        role_created = True
        await admin.execute(render_migration("shared.sql", schema=schema))
        await admin.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
        await admin.execute(
            f'GRANT SELECT, INSERT, UPDATE ON "{schema}"."aletora_records" TO "{role}"'
        )
    except BaseException:
        if role_created:
            relation = await admin.fetchval(
                "SELECT to_regclass($1)", f'"{schema}"."aletora_records"'
            )
            if relation is not None:
                await admin.execute(f'REVOKE ALL ON "{schema}"."aletora_records" FROM "{role}"')
            await admin.execute(f'REVOKE ALL ON SCHEMA "{schema}" FROM "{role}"')
            await admin.execute(f'DROP ROLE "{role}"')
        raise
    finally:
        await admin.close()

    engine = create_async_engine(_sqlalchemy_dsn(dsn))
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = SQLAlchemyRecordStore(schema=schema)

    async def assume_role(session: Any, tenant: str | None = None) -> None:
        await session.execute(text(f'SET LOCAL ROLE "{role}"'))
        if tenant is not None:
            await session.execute(
                text("SELECT set_config('app.current_tenant_id', :tenant, true)"),
                {"tenant": tenant},
            )

    try:
        # The setting is transaction-local and absent by default: no rows visible.
        async with sessions.begin() as session:
            await assume_role(session)
            assert (
                await store.get(Scope("alpha", "ns"), "candidate", "one", session=session) is None
            )
        with pytest.raises(DBAPIError):
            async with sessions.begin() as session:
                await assume_role(session)
                await store.insert(
                    Scope("alpha", "ns"), "candidate", "one", "k", "f", {}, session=session
                )

        async with sessions.begin() as session:
            await assume_role(session, "alpha")
            assert (
                await store.insert(
                    Scope("alpha", "ns"),
                    "candidate",
                    "one",
                    "k",
                    "f",
                    {"state": "proposed", "evidence_id": "parent"},
                    session=session,
                )
            ).created
            with pytest.raises(ValueError, match="GovernanceState"):
                await store.list_candidates_by_state(
                    Scope("alpha", "ns"),
                    ["proposed"],  # type: ignore[list-item]
                    session=session,
                )
            assert [
                record.id
                for record in await store.list_candidates_by_state(
                    Scope("alpha", "ns"), [GovernanceState.PROPOSED], session=session
                )
            ] == ["one"]
            assert [
                record.id
                for record in await store.list_candidates_by_evidence(
                    Scope("alpha", "ns"), "parent", session=session
                )
            ] == ["one"]
            assert (
                await store.get_for_share(Scope("alpha", "ns"), "candidate", "one", session=session)
                is not None
            )
            assert (
                await store.update(
                    Scope("alpha", "ns"),
                    "candidate",
                    "one",
                    1,
                    {"state": "proposed", "evidence_id": "parent", "ok": True},
                    session=session,
                )
            ).revision == 2
        async with sessions.begin() as session:
            await assume_role(session, "")
            assert (
                await store.get(Scope("alpha", "ns"), "candidate", "one", session=session) is None
            )
        async with sessions.begin() as session:
            await assume_role(session, "beta")
            assert (
                await store.get(Scope("alpha", "ns"), "candidate", "one", session=session) is None
            )
            assert (
                await store.get_many(Scope("alpha", "ns"), "candidate", ["one"], session=session)
                == {}
            )
            assert (
                await store.list_records(Scope("alpha", "ns"), "candidate", session=session) == ()
            )
            assert (
                await store.list_candidates_by_state(
                    Scope("alpha", "ns"), [GovernanceState.PROPOSED], session=session
                )
                == ()
            )
            assert (
                await store.list_candidates_by_evidence(
                    Scope("alpha", "ns"), "parent", session=session
                )
                == ()
            )
            with pytest.raises(NotFound):
                await store.update(Scope("alpha", "ns"), "candidate", "one", 2, {}, session=session)
        with pytest.raises(DBAPIError):
            async with sessions.begin() as session:
                await assume_role(session, "beta")
                await store.insert(
                    Scope("alpha", "ns"), "candidate", "two", "k2", "f", {}, session=session
                )
    finally:
        await engine.dispose()
        if role_created:
            cleanup = await asyncpg.connect(_asyncpg_dsn(dsn))
            try:
                relation = await cleanup.fetchval(
                    "SELECT to_regclass($1)", f'"{schema}"."aletora_records"'
                )
                if relation is not None:
                    await cleanup.execute(
                        f'REVOKE ALL ON "{schema}"."aletora_records" FROM "{role}"'
                    )
                await cleanup.execute(f'REVOKE ALL ON SCHEMA "{schema}" FROM "{role}"')
                await cleanup.execute(f'DROP ROLE "{role}"')
            finally:
                await cleanup.close()


@postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("migration", ["dedicated.sql", "shared.sql"])
async def test_migration_rejects_whitespace_only_keys(
    postgres_schema: tuple[str, str], migration: str
) -> None:
    from aletora.migrations import render_migration

    asyncpg = pytest.importorskip("asyncpg")
    dsn, schema = postgres_schema
    connection = await asyncpg.connect(_asyncpg_dsn(dsn))
    try:
        await connection.execute(render_migration(migration, schema=schema))
        indexes = await connection.fetch(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname=$1 AND tablename='aletora_records'",
            schema,
        )
        definitions = {row["indexname"]: row["indexdef"] for row in indexes}
        for field in ("state", "evidence_id"):
            definition = definitions[f"aletora_candidates_{field}_idx"]
            assert f"payload ->> '{field}'" in definition
            assert "WHERE (kind = 'candidate'::text)" in definition
        for index in range(5):
            for blank in ("\t", "\n", "\u00a0", "\u3000"):
                keys = ["tenant", "namespace", "candidate", "id", "key"]
                keys[index] = blank
                with pytest.raises(asyncpg.CheckViolationError):
                    async with connection.transaction():
                        await connection.execute(
                            "SELECT set_config('app.current_tenant_id', $1, true)", keys[0]
                        )
                        await connection.execute(
                            f'INSERT INTO "{schema}".aletora_records '
                            "VALUES ($1, $2, $3, $4, $5, 'fingerprint', 1, '{}')",
                            *keys,
                        )
    finally:
        await connection.close()


@pytest.mark.parametrize("missing", ["_finalized", "is_in_transaction"])
def test_tortoise_rejects_missing_transaction_state_probe(missing: str) -> None:
    pytest.importorskip("tortoise")
    from tortoise.backends.base.client import Capabilities, TransactionalDBClient

    from aletora.adapters.tortoise import TortoiseRecordStore

    wrapper = MagicMock(spec=TransactionalDBClient)
    wrapper.capabilities = Capabilities(dialect="postgres")
    wrapper.transaction = object()
    wrapper._connection = SimpleNamespace()
    if missing == "_finalized":
        del wrapper._finalized
    else:
        wrapper._finalized = False

    with pytest.raises(RuntimeError, match=missing):
        TortoiseRecordStore(wrapper)
