"""Real PostgreSQL contract tests for the optional transaction adapters."""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest

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


def test_migration_templates_render_safe_identifiers() -> None:
    from aletora.migrations import render_migration

    assert '"client_data"."aletora_records"' in render_migration(
        "dedicated.sql", schema="client_data"
    )
    with pytest.raises(ValueError):
        render_migration("dedicated.sql", schema='bad"; DROP SCHEMA public; --')


def test_adapter_imports_do_not_load_optional_orms() -> None:
    script = (
        "import sys; import aletora; "
        "import aletora.adapters.sqlalchemy; import aletora.adapters.tortoise; "
        "assert 'sqlalchemy' not in sys.modules; assert 'tortoise' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", script], check=True)


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
        with pytest.raises(TypeError, match="Tortoise.*wrapper"):
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
            assert (
                await store.get(scope, "candidate", "one", transaction=transaction)
            ).payload == {"x": 1}  # type: ignore[union-attr]
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
                    scope, "candidate", "one", "key", "fp", {}, transaction=transaction
                )
            ).created
            await transaction.execute_query_dict(
                "SELECT set_config($1, $2, true)", ["app.current_tenant_id", "beta"]
            )
            assert await store.get(scope, "candidate", "one", transaction=transaction) is None
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
            assert (await default_store.get(scope, "candidate", "same-id")).payload == {"v": 1}  # type: ignore[union-attr]
            assert (await first_store.get(scope, "candidate", "shared-id")).payload == {"v": 2}  # type: ignore[union-attr]
            assert (await second_store.get(scope, "candidate", "shared-id")).payload == {"v": 3}  # type: ignore[union-attr]
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
                    Scope("alpha", "ns"), "candidate", "one", "k", "f", {}, session=session
                )
            ).created
        async with sessions.begin() as session:
            await assume_role(session, "beta")
            assert (
                await store.get(Scope("alpha", "ns"), "candidate", "one", session=session) is None
            )
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
