-- Host-owned migration for a shared schema. SET app.current_tenant_id per
-- transaction. Missing settings deny all reads and writes (including WITH CHECK).
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
ALTER TABLE {{table}} ENABLE ROW LEVEL SECURITY;
ALTER TABLE {{table}} FORCE ROW LEVEL SECURITY;
CREATE POLICY aletora_records_tenant_scope ON {{table}}
    USING (tenant_id = current_setting('app.current_tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true));
