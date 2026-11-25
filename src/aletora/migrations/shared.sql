-- Host-owned migration for a shared schema. Run
-- SET LOCAL app.current_tenant_id = '<tenant_id>' (or set_config(..., true))
-- per transaction. Missing or empty settings deny all reads and writes
-- (including WITH CHECK).
CREATE TABLE {{table}} (
    tenant_id text NOT NULL CHECK (btrim(tenant_id, U&'\0009\000A\000B\000C\000D\001C\001D\001E\001F\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000') <> ''),
    namespace text NOT NULL CHECK (btrim(namespace, U&'\0009\000A\000B\000C\000D\001C\001D\001E\001F\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000') <> ''),
    kind text NOT NULL CHECK (btrim(kind, U&'\0009\000A\000B\000C\000D\001C\001D\001E\001F\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000') <> ''),
    record_id text NOT NULL CHECK (btrim(record_id, U&'\0009\000A\000B\000C\000D\001C\001D\001E\001F\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000') <> ''),
    idempotency_key text NOT NULL CHECK (btrim(idempotency_key, U&'\0009\000A\000B\000C\000D\001C\001D\001E\001F\0020\0085\00A0\1680\2000\2001\2002\2003\2004\2005\2006\2007\2008\2009\200A\2028\2029\202F\205F\3000') <> ''),
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
    USING (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), ''))
    WITH CHECK (tenant_id = NULLIF(current_setting('app.current_tenant_id', true), ''));
