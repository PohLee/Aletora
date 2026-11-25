# Optional Qdrant projection

Install the optional adapter dependency with `pip install 'aletora[qdrant]'`. Import it
explicitly with `from aletora.adapters.qdrant import QdrantIndex`; importing the Aletora
core does not import Qdrant. The host creates and owns the `AsyncQdrantClient`, passes it
and a collection name, an `EmbeddingProfile`, and an async `embed(query) -> VectorLike`
callable to `QdrantIndex`. Construction performs no network calls, and the adapter never
closes the client.

```python
index = QdrantIndex(client, "aletora-candidates", profile, embed)
await index.ensure_collection()  # explicit provisioning/verification step
```

`ensure_collection()` creates a missing collection using cosine distance and the profile's
dimension, handling concurrent worker startup races if another replica creates the collection
first. For an existing collection, it accepts only one unnamed dense-vector configuration
with cosine distance and the same dimension; it rejects a different dimension, distance, or
named-vector policy rather than silently reusing it. After verifying the collection vector
configuration, it creates/ensures payload indexes for `tenant_id` (keyword with
`is_tenant=True`), `namespace`, `profile_id`, `candidate_id`, `evidence_id` (keyword), and
`revision` (integer). Collections may contain several profile identities of the same dimension:
every read/write/delete is filtered to the full `profile_id`, which includes model, dimension,
preprocessing version, and chunking version. Do not change the meaning of a profile without
changing one of those versioned fields. Collection provisioning remains the host's explicit
startup/deployment responsibility.

Workers project candidates with `await index.upsert(scope, candidate_id, revision, vector,
evidence_id=candidate.evidence_id)` (`evidence_id` is required so evidence invalidation always
covers projected points; pass `prune_prior_revisions=True` to best-effort delete revisions
`< revision` after upserting). Deletion is `await index.delete(scope, candidate_id, revision)`.
The delete revision is the highest candidate revision known to be terminal and deletes only
matching candidate points whose projected revision is less than or equal to it. Evidence
withdrawal events call `await index.delete_evidence(scope, evidence_id)`, which filters on
tenant, namespace, profile, and evidence ID.

For outbox workers, `aletora.adapters.qdrant.reconcile_event(memory, index, embed_candidate, event)`
implements the recommended re-read-and-project workflow: it checks the authoritative candidate
and evidence state through `Memory` and either upserts the current accepted+applied revision or
deletes terminal/withdrawn/missing projections. Candidate refresh and delete require kind
`candidate`, and evidence invalidation requires kind `evidence`. An accepted, applied candidate
with active evidence can be projected before `valid_from` or at/after `valid_until`. Retrieval
checks those bounds against its clock for each call.

The projected payload contains only scope, candidate/evidence IDs, profile ID, and revision—not
candidate or evidence text. `scan` yields validated `IndexHit` candidate IDs, revisions, profile
identity and Qdrant score through one async context manager. It is a discovery mechanism, not an
authorization decision; `Memory` must reread the current candidate/evidence and the host resolver
must authorize and return the exact current formal object version.

```python
async with index.scan(scope, "approval policy", budget=200) as discoveries:
    async for hit in discoveries:
        # A discovery is not authorized business content.
        # Normally Memory.retrieve or Memory.retrieve_with_stats performs
        # the required governance and host resolution.
        print(hit.candidate_id, hit.revision)
```

Each scan prepares the query embedding once. Raw pagination and exhaustion stay inside the
adapter; points with malformed payloads still consume the positive integer budget, and an
entirely discarded page does not hide later valid discoveries. Invalid or nonfinite scores
continue to raise explicitly instead of entering retrieval. Backend requests never exceed the
remaining budget. The adapter fetches lazily in bounded batches and does not request another page
after the consumer stops. A small result limit can still fetch extra raw points in the current
batch. On early exit, error or cancellation the context closes traversal-local iteration, not the
host's Qdrant client. Concurrent scans have independent query vectors and continuation state;
there is no cross-request cache. A traversal does not promise snapshot consistency while the
projection changes. Embedder and backend failures propagate without fallback.

This replaces the unreleased 0.1 `search(scope, query, limit=..., offset=...)` method and
`SearchPage` type. Custom index adapters must implement `SearchIndex.scan`; callers already
using `Memory.retrieve` need no change.

Hosts should append projection events to their transactionally durable outbox and let a worker
re-read the authoritative record before computing/projecting it. The point ID is UUID5 over a
JSON array of tenant, namespace, candidate ID, profile ID, and revision. Thus retries/rebuilds
of the same revision are idempotent while different revisions cannot overwrite one another.
When `prune_prior_revisions=False`, upsert does not prune older revisions; search may return stale
hits, and `Memory` checks each hit's revision against the authoritative candidate so stale
duplicates do not hide a current hit. Delete uses exact tenant, namespace, profile, and candidate
filters plus `revision <= delete revision`, so a delayed lower-revision delete cannot remove a
newer point. No Qdrant write provides an atomic compare-and-set against the host record.
Serialize projection per candidate where possible and always reread the current record in the
worker before projection; revision checks during retrieval remain mandatory. Do not assume Qdrant
is authoritative or that an acknowledged write is transactionally coupled to the host database. A
failed operation must be retried by the host worker.

A delayed upsert for a terminal candidate can still recreate a point for an old revision after
its deletion. Retrieval rejects it by rereading current state/revision, but this adapter does not
promise physical erasure or retention. Hosts must order/reconcile terminal deletes and refreshes,
and own retention/erasure policy. Rebuilds should paginate authoritative candidates with
`Memory.list_candidates` and follow the same worker ordering/re-read rules. The revision-bearing
point identity changes the experimental 0.1 projection layout; although that version is unreleased,
rebuild the collection from authoritative records rather than expecting legacy point IDs to be
migrated in place.

`Scope` is expected to come from the trusted host after authorization. Search does not accept
caller-supplied filters, and always applies exact tenant, namespace, and profile conditions;
returned payloads are also checked defensively. Scope filters do not defend against a malicious
host or replace current-record authorization. Validate embedding policy and collection migration
before enabling a new profile.

Local unit tests use Qdrant's local in-memory mode (`AsyncQdrantClient(":memory:")`), while
server-mode integration tests gated by `ALETORA_TEST_QDRANT_URL` run against a real Qdrant
service in CI.

Rebuild collections populated by older versions that wrote points without
`evidence_id`. Evidence invalidation filters by that payload field and cannot
find those points. Run reconciliation with PostgreSQL READ COMMITTED so the
post-upsert evidence check can observe withdrawals committed during the refresh.
