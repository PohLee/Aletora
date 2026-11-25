# PostgreSQL adapters

Aletora ships two optional adapters for its asynchronous `RecordStore` port:

- `aletora.adapters.sqlalchemy.SQLAlchemyRecordStore` uses a caller-owned,
  already-active SQLAlchemy `AsyncSession` over a PostgreSQL dialect.
- `aletora.adapters.tortoise.TortoiseRecordStore` uses the transaction wrapper
  yielded by Tortoise `in_transaction()` on Tortoise's PostgreSQL `asyncpg`
  backend, never a pooled client or bare connection.

Both adapters also implement `ShareLockableRecordStore` (`get_for_share`),
`BatchRecordStore` (`get_many`), and `EnumerableRecordStore` (`list_records`).

Install the relevant extra (`aletora[sqlalchemy]` or `aletora[tortoise]`). The
adapters execute on the supplied host transaction; they never commit, roll
back, close it, allocate a pool, or do network/index work. Pass the transaction
when constructing an adapter or per operation. The adapter raises if the
SQLAlchemy session has no active transaction, uses a non-PostgreSQL dialect, or
runs in AUTOCOMMIT, and rejects a bare Tortoise client or non-`asyncpg` wrapper.
`schema=` accepts only a simple PostgreSQL identifier of 1–63 ASCII characters
(`[A-Za-z_][A-Za-z0-9_]{0,62}`), matching PostgreSQL's `NAMEDATALEN - 1` limit so
long schema names cannot be silently truncated into collisions. Omit it to use
the caller's unchanged `search_path`.

SQLAlchemy's `session.in_transaction()` describes an ORM logical transaction,
which can exist even when the database is in AUTOCOMMIT. Before executing
Aletora SQL, the adapter resolves the session's actual connection, checks its
PostgreSQL dialect, and checks its DBAPI transaction mode. AUTOCOMMIT is
rejected rather than changed; an uninspectable mode also fails closed.
Configure a transactional isolation level before beginning the host transaction.
This applies to engine defaults, inherited execution options and explicitly
bound connections; a connection override back to a transactional mode is
allowed.

Tortoise currently exposes no public check for whether its transaction wrapper
is still active. To fail closed before every statement, the adapter checks the
wrapper's PostgreSQL `asyncpg` shape and its `_finalized`, `transaction`, and
`_connection.is_in_transaction()` state. This intentionally narrow compatibility
check uses Tortoise/asyncpg wrapper internals; it rejects wrappers that have not
begun, have been finalized, or whose connection has been released, and raises
`TypeError` for Tortoise's `psycopg` wrapper. If a future Tortoise version
changes that wrapper shape, the adapter errors rather than falling back to a
pooled/autocommit connection.

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
  `NULLIF(current_setting('app.current_tenant_id', true), '')`. Set this
  transaction-local value after host authorization, e.g.
  `SET LOCAL app.current_tenant_id = ...` or `set_config(..., true)`. Never use
  session-level `SET app.current_tenant_id` on pooled connections. Missing or
  empty settings mean no rows are visible and writes fail the policy's
  `WITH CHECK`. Both templates also enforce non-blank `CHECK` constraints (using the same whitespace set as Python's `str.strip()`) on
  `tenant_id`, `namespace`, `kind`, `record_id`, and `idempotency_key`.

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

## Concurrent lineage and supersession locking

- New proposals and candidate mutations that check active evidence take a
  shared evidence-row lock via `get_for_share` (`SELECT ... FOR SHARE`).
  Matching proposal replays return after a lock-free candidate lookup.
  Without `get_for_share`, the fallback is an exclusive `FOR UPDATE` lock,
  which serializes new proposals referencing the same evidence.
- `withdraw_evidence` takes `FOR UPDATE` before checking or updating the
  evidence row. Avoid withdrawing evidence in a transaction that already
  acquired a shared lock on that row. Concurrent lock upgrades can deadlock.
  Hosts that combine these operations must be prepared to roll back and retry
  the entire transaction.
- `Memory.supersede` reads both candidates with `RecordStore.get(..., for_update=True)`
  in sorted record-ID order, then validates the locked states and expected
  revision, and then locks the replacement's evidence (`candidate -> evidence`
  lock order). A competing reciprocal supersession waits and then checks current
  state, so both requests cannot commit a replacement cycle. Ordinary
  read-only lookups (`get_evidence`, `get_candidate`, `retrieve`) do not acquire
  row locks.

Hosts performing several operations or taking their own locks in a transaction
must still handle PostgreSQL deadlock/serialization errors by rolling back and
retrying the entire unit of work when appropriate; the library does not retry or
commit part of it. Custom `RecordStore` implementations must honor `for_update`
and optionally `get_for_share` for concurrent storage. The supplied in-memory
store already permits only one active owner.

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
