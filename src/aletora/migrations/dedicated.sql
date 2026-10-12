-- Host-owned migration: apply in the dedicated schema (or use render_migration).
CREATE TABLE {{table}} (
    tenant_id text NOT NULL,
    namespace text NOT NULL,
    kind text NOT NULL,
    record_id text NOT NULL,
    idempotency_key text NOT NULL,
    fingerprint text NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT aletora_records_pkey PRIMARY KEY (tenant_id, namespace, kind, record_id),
    CONSTRAINT aletora_records_idempotency_key
        UNIQUE (tenant_id, namespace, kind, idempotency_key)
);
