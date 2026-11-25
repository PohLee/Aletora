CREATE TABLE host_outbox (
    id text PRIMARY KEY,
    tenant_id text NOT NULL,
    namespace text NOT NULL,
    action text NOT NULL,
    kind text NOT NULL,
    record_id text NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error text,
    dead_lettered_at timestamptz,
    processed_at timestamptz
);

CREATE INDEX host_outbox_pending_idx
    ON host_outbox (tenant_id, namespace, queued_at, id)
    WHERE processed_at IS NULL AND dead_lettered_at IS NULL;
