# Aletora V1 implementation specification

Status: implementation target approved in this session, 2026-10-13. Research documents remain evidence and recommendations, not implementation claims.

## Scope and exclusions

Build an installable Python >=3.12 package `aletora`, verified on 3.12 and 3.14. Core has no third-party runtime dependencies. Optional extras: `sqlalchemy`, `tortoise`, `qdrant`; development dependencies include pytest, pytest-asyncio, Ruff, mypy and build tooling.

No edits to Impulsira/ServexaOS, no public release, no LLM provider, Docling, graph database, automated promotion, graph reasoning or mandatory vector backend. Hosts supply candidate proposals, embedding policy, current-record authorization and durable worker execution. Typed relationship candidates are deferred until a real caller/test requires them; do not build unused seams.

## Invariants

1. Immutable validated Scope(tenant_id, namespace) is required everywhere and created by the host after authorization. No global/default tenant. Scope is not an authentication token.
2. Source kind (explicit user/document/tool/model), epistemic kind (asserted/inferred/question), governance state (proposed/accepted/rejected/withdrawn/superseded) are independent. Similarity and extractor confidence never authorize review or application.
3. Evidence preserves source ID, version, locator, kind, content, SHA-256 and capture time. Content/source cannot be overwritten. Withdrawal retains the audit snapshot but prevents current retrieval; physical erasure and PII policy are host responsibilities and must be documented as unsupported in V1.
4. Candidate includes evidence ID, text, epistemic kind, proposing actor, extractor/run identity, validity interval, revision, governance history, optional application receipt and replacement ID. No automatic acceptance. V1 prohibits self-review; hosts authenticate reviewer and application actor.
5. `accepted` != `applied`. Only an accepted candidate whose evidence is still active can record the host's exact owner/type/ID/version receipt: revoked lineage cannot be applied, and referencing hits are blocked. This interface does not itself create a formal object or independently prove the host wrote it.
6. All mutation writes and outbox appends participate in a caller-owned transaction. A sink failure is propagated; caller MUST rollback. Adapters do not own transactions or call network indexes during a write.
7. Evidence/proposal imports are tenant+namespace-scoped and idempotent. Reusing a key with different semantic content raises an explicit conflict. Evidence fingerprints cover source identity and content: a retry with a different `captured_at` returns the first snapshot rather than conflicting, since capture time is provenance metadata, not semantic content. Proposal fingerprints additionally include proposed times and validity windows. Mutation methods require expected revision; compare-and-swap prevents lost updates. Duplicate index events use stable IDs for host deduplication.
8. Cross-Scope IDs behave as not found. All natural/unique keys and queries include tenant+namespace. PostgreSQL RLS is additional defense, not the only filtering mechanism.

## Agreed test seams

Confirmed by user: (1) public lifecycle interface; (2) SQLAlchemy/Tortoise adapters participating in host transaction and outbox; (3) retrieval interface, authorization/version checks and optional Qdrant projection.

Tests cross the same interface as callers. Use vertical red-green slices, not tests coupled to private implementation. Concrete database tests may inspect host fixture rows and count Aletora records from an independent connection to prove joint transaction visibility and rollback. Domain snapshots and lifecycle assertions use the public Memory interface.

## Public module

`Memory` exposes `record_evidence`, `get_evidence`, `propose`, `get_candidate`, `review`, `record_application`, `withdraw`, `supersede`, `withdraw_evidence`, `retrieve`. This is the whole lifecycle module, not ten forwarding modules. Scope is explicit on each operation. Reads return immutable typed snapshots; mutation results expose revision.

`propose` accepts one host-produced Proposal and one evidence ID; a host can loop within its transaction for a batch. Review records reviewer/reason/time; application records a FormalRef. Withdrawal/supersession record actor/reason/time. Evidence withdrawal rejects later proposals and blocks referencing hits.

All candidate changes require `expected_revision`. Supersession requires a distinct, accepted replacement in the same Scope. Before checking either state, it locks both candidates in sorted record-ID order until the caller commits or rolls back; reciprocal concurrent supersession must not create a replacement cycle. Invalid transitions and invalid inputs raise typed errors. No state mutation on failed validation.

## Adapter contract

Core-owned `ports.py` seam: persistence, event and resolver operations are async; search returns
an async context manager for a single bounded traversal. Persistence adapters use the caller's
already-open transaction and never commit, rollback, close, or perform network index work during writes:

```python
@dataclass(frozen=True, slots=True)
class StoredRecord:
    id: str
    revision: int
    payload: dict[str, Any]

@dataclass(frozen=True, slots=True)
class InsertResult:
    record: StoredRecord
    created: bool

class RecordStore(Protocol):
    async def get(
        self, scope: Scope, kind: str, record_id: str, *, for_update: bool = False,
    ) -> StoredRecord | None: ...
    async def insert(
        self, scope: Scope, kind: str, record_id: str,
        idempotency_key: str, fingerprint: str, payload: dict[str, Any],
    ) -> InsertResult: ...
    async def update(
        self, scope: Scope, kind: str, record_id: str,
        expected_revision: int, payload: dict[str, Any],
    ) -> StoredRecord: ...

@dataclass(frozen=True, slots=True)
class IndexEvent:
    id: str
    scope: Scope
    action: IndexAction  # refresh_candidate | delete_candidate | invalidate_evidence
    kind: str
    record_id: str
    revision: int

class EventSink(Protocol):
    async def append(self, event: IndexEvent) -> None: ...

class SearchIndex(Protocol):
    def scan(
        self, scope: Scope, query: str, *, budget: int,
    ) -> AbstractAsyncContextManager[AsyncIterator[IndexHit]]: ...

class RecordResolver(Protocol):
    async def resolve(self, scope: Scope, ref: FormalRef) -> ResolvedRecord | None: ...
```

`SearchIndex.scan` is a single-use, sequential discovery traversal. `budget` is a positive
integer (excluding booleans) counting raw index points, including malformed, stale, duplicate
and subsequently denied points. The adapter owns query preparation, raw pagination and
exhaustion; a page with no validated hits does not end the traversal if raw progress remains.
Each backend request is capped by the remaining budget. No next page is prefetched after the
consumer stops. Context exit closes traversal-local iteration, including on errors and
cancellation, while leaving host-owned clients and transactions open. Adapter failures propagate.

The Qdrant adapter prepares one embedding per traversal and keeps query state local to that
traversal; it does not cache embeddings across requests. This fixes query preparation, not an
index snapshot or stable ranking during concurrent projection writes. Custom adapters must
implement this contract. The unreleased 0.1 page-based `search(..., limit, offset)` and
`SearchPage` interface are replaced; `Memory.retrieve` and its result shape remain unchanged.

`IndexHit(candidate_id: str, revision: int, score: float, profile_id: str)` is immutable; score must be finite. `FormalRef(owner, object_type, object_id, version)` identifies one exact host object version; `ResolvedRecord(ref, content)` is returned only for that authorized current version. `None` from the resolver means denied/missing/deleted; a different ref is rejected by the lifecycle module. Search results are untrusted candidates, not authorization.

`RecordStore.get(..., for_update=True)` must protect the returned row against concurrent updates until the caller's transaction ends. PostgreSQL adapters use scoped `SELECT ... FOR UPDATE`; the in-memory adapter already excludes concurrent transactions. Ordinary reads remain unlocked. Custom stores must implement this contract; silently ignoring the flag in a concurrent store is unsafe. No adapter commits or releases caller-owned locks.

`RecordStore.insert` returns the existing record with `created=False` for a matching idempotency key and fingerprint; a semantic mismatch raises `IdempotencyConflict`. PostgreSQL enforces atomic uniqueness. `update` increments revision atomically; missing records raise `NotFound`, stale revisions raise `RevisionConflict`. `EventSink` is supplied by the host on the same transaction. Events carry stable deduplication IDs and request candidate refresh/deletion or evidence invalidation; workers re-read current records before projection. Scope is mandatory and not caller-overridable in index filtering.

## Persistence and migrations

Single prefixed `aletora_records` aggregate table: tenant_id, namespace, kind, record_id, idempotency_key, fingerprint, revision, payload(JSONB). Domain aggregates declare `schema_version=1`; missing/unknown/non-integer versions must fail explicitly rather than be silently reinterpreted. Composite primary key `(tenant_id, namespace, kind, record_id)` and unique `(tenant_id, namespace, kind, idempotency_key)`. Domain lineage checks performed by Memory against scoped evidence; no cross-tenant references. Concurrent duplicates use INSERT ON CONFLICT DO NOTHING then read the winner, not catch a transaction-aborting unique violation.

SQLAlchemy adapter requires an active supplied AsyncSession transaction and executes SQL on it. It also checks the actual PostgreSQL DBAPI connection before Aletora SQL and rejects AUTOCOMMIT (or an uninspectable transaction mode): an ORM logical transaction alone is insufficient. It must use the same connection for validation and execution without changing the host's mode. Tortoise adapter requires a supplied PostgreSQL transaction wrapper, not a bare pool connection; uses that connection for every statement. SQL template schema identifiers must be validated; do not interpolate untrusted identifiers. Public/shared mode supports ENABLE + FORCE ROW LEVEL SECURITY based on `app.current_tenant_id`; dedicated mode follows caller search_path with tables provisioned in that schema. Templates do not apply automatically and require a non-superuser/non-BYPASSRLS fixture to prove RLS. No SQLite tests presented as PostgreSQL verification.

## Retrieval and projection

Retrieval returns only accepted, applied, currently valid candidates with active evidence. IndexHit revision must equal current candidate revision; profile must match configured profile. Host resolver must return exact current FormalRef and authorized content. Results contain formal content, candidate/ref/revision, epistemic kind, governance state, profile ID, citations and separate retrieval score; no raw candidate/evidence text or unverified index text enters context. Any adapter error propagates (no insecure fallback).

Traverse the index within a documented finite raw-point budget to fill authorized result slots;
limit is positive and bounded. The index adapter owns bounded pagination and exhaustion.
Memory consumes validated discoveries and stops as soon as enough authorized results exist. Deduplicate IDs; never return more than requested. Missing index or resolver raises an explicit capability error. A configured index returning no matches returns an empty result. A scan-budget shortfall may return fewer results, never unsafe results.

Qdrant adapter receives caller-owned AsyncQdrantClient, explicit collection and versioned EmbeddingProfile (model, dimension, preprocessing/chunking versions). It uses an injected async embed callable for search; no supplier SDK. Worker-facing upsert/delete validate scope, dimensions, profile and candidate revision. Stable point ID includes Scope, candidate ID, profile and revision: retrying the same revision is idempotent, while a delayed older upsert cannot overwrite a newer revision. Candidate deletion requires a positive revision and removes only matching projections at or below it, never a newer revision. Exact payload tenant/namespace/profile filtering and current revision re-read during retrieval remain mandatory. Older revisions may persist and consume the finite scan budget until worker cleanup/rebuild; no strict recall-quality or physical-erasure guarantee is made. No automatic collection or network access on import. Worker retry/rebuild/delete, delayed old upsert/delete and profile mismatch tests use real local-mode Qdrant.

## Verification gates

- Idempotent evidence/proposals; conflicting retry; identical content across tenants distinct; immutable provenance.
- Invalid transitions, self-review rejected, accepted not applied; optimistic concurrency; explicit supersession and withdrawal; reciprocal supersession rejected under concurrent real PostgreSQL transactions for both adapters.
- Host rollback covers Aletora evidence/candidate/application and durable host outbox, including sink failure. Adapters never commit. SQLAlchemy rejects AUTOCOMMIT from engine or connection configuration before writes, while a connection explicitly restored to transactional mode remains usable.
- Shared tenant RLS using restricted DB role; dedicated schemas with same logical IDs; caller search_path preserved.
- Retrieval denies stale revision/profile/ref, withdrawn/deleted/expired/unapplied data, wrong tenant/namespace, denied role; rejected hits do not consume the first page's return slots.
- Qdrant local mode scoped upsert/scan/delete/retry/rebuild; one embedding across multiple raw pages, malformed-point budget accounting, early stop and independent concurrent traversals. Context cleanup covers errors and cancellation; no live Qdrant-service performance claim.
- Shared real-host transaction scenarios run against both SQLAlchemy and Tortoise, observing committed records, formal writes and durable outbox events from an independent connection. Adapter-specific mode, RLS and connection checks remain explicit.
- Python 3.12 and 3.14 full suite, Ruff, typecheck, wheel/sdist, core-only import/install smoke. CI retains real PostgreSQL gates.

## Task map

1. TL: establish scope/spec/glossary/ADR and frozen contract; review each handoff.
2. Core agent: immutable domain types, errors, ports, Memory lifecycle/retrieval, in-memory transaction adapter, vertical behavior tests.
3. Packaging agent: dependency extras, package build, development setup and CI; independent file ownership.
4. PostgreSQL agent: two concrete transaction adapters, reusable SQL template/RLS, contract and transaction tests against isolated PostgreSQL.
5. Qdrant agent: optional projection adapter and local-mode behavior tests.
6. TL + independent review agents: standards/spec/security review; dispatch fixes; execute final gates; document precise validation scope and commit only created implementation files (not pre-existing untracked research).
