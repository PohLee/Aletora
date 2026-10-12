# PostgreSQL adapters

Aletora ships two optional adapters for its asynchronous `RecordStore` port:

- `aletora.adapters.sqlalchemy.SQLAlchemyRecordStore` uses a caller-owned,
  already-active SQLAlchemy `AsyncSession`.
- `aletora.adapters.tortoise.TortoiseRecordStore` uses the transaction wrapper
  yielded by Tortoise `in_transaction()`, never a pooled client or bare
  connection.

Install the relevant extra (`aletora[sqlalchemy]` or `aletora[tortoise]`). The
adapters execute on the supplied host transaction; they never commit, roll
back, close it, allocate a pool, or do network/index work. Pass the transaction
when constructing an adapter or per operation. The adapter raises if the
SQLAlchemy session has no active transaction and rejects a bare Tortoise
client. `schema=` accepts only a simple PostgreSQL identifier. Omit it to use
the caller's unchanged `search_path`.

Tortoise currently exposes no public check for whether its transaction wrapper
is still active. To fail closed before every statement, the adapter checks the
wrapper's PostgreSQL dialect and its `_finalized`, `transaction`, and
`_connection.is_in_transaction()` state. This intentionally narrow compatibility
check uses Tortoise/asyncpg wrapper internals; it rejects wrappers that have not
begun, have been finalized, or whose connection has been released. If a future
Tortoise version changes that wrapper shape, the adapter errors rather than
falling back to a pooled/autocommit connection.

```python
async with session_factory() as session:
    async with session.begin():
        store = SQLAlchemyRecordStore(session)
        await store.insert(scope, "evidence", record_id, key, fingerprint, payload)
        # Host formal writes and outbox appends belong in this same transaction.
```

```python
async with in_transaction("default") as transaction:
    store = TortoiseRecordStore(transaction)
    await store.update(scope, "candidate", candidate_id, revision, payload)
```

## Host-managed migration

Migrations are packaged SQL templates. They are never applied automatically.
The host owns migration ordering, privileges, deployment coordination, backup,
and retention. Read or render them with `aletora.migrations.read_migration`
and `render_migration`; pass `schema="..."` to qualify the table safely.

- `dedicated.sql` creates the aggregate table without RLS. Apply it with the
  dedicated schema in the host's `search_path`, or render with an explicit
  schema.
- `shared.sql` creates the same table, enables and forces row-level security,
  and installs a tenant policy using
  `current_setting('app.current_tenant_id', true)`. Set this transaction-local
  value after host authorization, e.g. `SET LOCAL app.current_tenant_id = ...`
  or `set_config(..., true)`. Missing setting means no rows are visible and
  writes fail the policy's `WITH CHECK`.

RLS is defense in depth; every adapter query still scopes by tenant, namespace,
kind, and record ID. Test shared-RLS behavior using a restricted role that is
neither superuser nor `BYPASSRLS`; table owners are subject to policy because
the template uses `FORCE ROW LEVEL SECURITY`. Superusers and bypass roles do
not prove the policy works.

## Transaction and failure obligations

Host formal writes, Aletora aggregate changes, and durable outbox appends must
share one transaction. If an outbox sink, validation step, or later host write
fails, the exception must escape the transaction boundary so the host rolls
back every write. If the host catches an exception inside the transaction, it
must explicitly roll back (or mark its transaction rollback-only); PostgreSQL
cannot infer that a caught application error invalidated the unit of work.
Adapters intentionally do not make this decision for the host. Do not swallow
adapter errors or retry an operation on a fresh transaction while leaving the
host transaction committed.

`insert` uses `ON CONFLICT DO NOTHING` then reads the scoped winner, avoiding
PostgreSQL's aborted-transaction behavior from a caught unique violation. It
returns an existing record only when its fingerprint matches; otherwise it
raises `IdempotencyConflict`. `update` increments the revision only when the
expected revision matches and distinguishes a missing/invisible scoped row
(`NotFound`) from a lost compare-and-swap (`RevisionConflict`).

## Migration rollback

There is deliberately no automatic downgrade. Before removing a migration,
stop writers and take the host's normal backup. Reversing `shared.sql` means
dropping its policy, disabling RLS, then dropping the table; reversing
`dedicated.sql` means dropping the table. These operations destroy persisted
Aletora evidence and governance history and must be performed only when the
host explicitly intends that data loss. The host should quote the same schema
and table identifiers used at migration time. Never use a broad schema/database
drop as an adapter cleanup mechanism.

Real PostgreSQL integration tests run when `ALETORA_TEST_POSTGRES_DSN` is set.
They create uniquely named test schemas and (for the RLS test) one uniquely
named restricted role, then remove only those objects. SQLite is not a
substitute for these PostgreSQL checks.
