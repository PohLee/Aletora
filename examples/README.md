# PostgreSQL outbox worker example

This reference belongs in the host application. Provision [outbox.sql](outbox.sql) and
append each Aletora event to the outbox with its stable ID in the transaction that
writes the Aletora aggregate. The [worker](outbox_worker.py) uses the SQLAlchemy
and Qdrant extras. Pass an idle host-owned session, an authorized scope, your event
sink, a configured Qdrant index, and your embedding callable. The event sink is a
required Memory constructor argument. The worker only reads through Memory.

Schedule `run_batch` on the host. A batch locks pending events and processes each
in a PostgreSQL savepoint. The return value counts events marked processed.
On failure the worker rolls back the savepoint, increments `attempts` and records
the exception type in `last_error` before continuing with later events. After three
failures, it sets `dead_lettered_at` and excludes the row from future batches.
Failure counts and processed markers commit together at the end of the batch.

Qdrant writes are external to PostgreSQL and may already have succeeded before a
failure. Retrying reconciliation re-reads authoritative state. Keep batches small
because locks are held during Qdrant requests. The host manages scheduling,
retry backoff, dead-letter inspection and replay, and retention. Apply host
permissions or RLS to the outbox table as appropriate. Both the wheel and source
distribution exclude this example.
