# Optional Qdrant projection

Install the optional adapter dependency with `pip install 'aletora[qdrant]'`. Import it
explicitly with `from aletora.adapters.qdrant import QdrantIndex`; importing the Aletora
core does not import Qdrant. The host creates and owns the `AsyncQdrantClient`, passes it
and a collection name, an `EmbeddingProfile`, and an async `embed(query) -> Sequence[float]`
callable to `QdrantIndex`. Construction performs no network calls, and the adapter never
closes the client.

```python
index = QdrantIndex(client, "aletora-candidates", profile, embed)
await index.ensure_collection()  # explicit provisioning/verification step
```

`ensure_collection()` creates a missing collection using cosine distance and the profile's
dimension. For an existing collection, it accepts only one unnamed dense-vector configuration
with cosine distance and the same dimension; it rejects a different dimension, distance, or
named-vector policy rather than silently reusing it. Collections may contain several profile identities of the same dimension:
every read/write/delete is filtered to the full `profile_id`, which includes model, dimension,
preprocessing version, and chunking version. Do not change the meaning of a profile without
changing one of those versioned fields. Collection provisioning remains the host's explicit
startup/deployment responsibility.

Workers project candidates with `await index.upsert(scope, candidate_id, revision, vector,
evidence_id=...)`; deletion is `await index.delete(scope, candidate_id, revision)`. The delete
revision is the highest candidate revision known to be terminal and deletes only matching
candidate points whose projected revision is less than or equal to it. Evidence withdrawal
events can call `await index.delete_evidence(scope, evidence_id)`, which filters on tenant,
namespace, profile, and evidence ID. Supply evidence IDs on upsert if evidence invalidation
must remove all affected points. The projected payload contains only scope, candidate/evidence
IDs, profile ID, and revision—not candidate or evidence text. Search returns a `SearchPage`:
validated `IndexHit` candidate IDs, revisions, profile identity, and Qdrant score, plus raw
`scanned`/`exhausted` accounting so retrieval keeps paging past discarded points. It is a discovery mechanism, not
an authorization decision; `Memory` must reread the current candidate/evidence and the host
resolver must authorize and return the exact current formal object version.

Hosts should append projection events to their transactionally durable outbox and let a worker
re-read the authoritative record before computing/projecting it. The point ID is UUID5 over a
JSON array of tenant, namespace, candidate ID, profile ID, and revision. Thus retries/rebuilds
of the same revision are idempotent while different revisions cannot overwrite one another.
Upsert does not prune older revisions: search may return stale hits, and `Memory` checks each
hit's revision against the authoritative candidate so stale duplicates do not hide a current hit.
Delete uses exact tenant, namespace, profile, and candidate filters plus `revision <= delete
revision`, so a delayed lower-revision delete cannot remove a newer point. No Qdrant write
provides an atomic compare-and-set against the host record. Serialize projection per candidate
where possible and always reread the current record in the worker before projection; revision
checks during retrieval remain mandatory. Do not assume Qdrant is authoritative or that an
acknowledged write is transactionally coupled to the host database. A failed operation must be
retried by the host worker.

A delayed upsert for a terminal candidate can still recreate a point for an old revision after
its deletion. Retrieval rejects it by rereading current state/revision, but this adapter does not
promise physical erasure or retention. Hosts must order/reconcile terminal deletes and refreshes,
and own retention/erasure policy. Rebuilds should read current records and follow the same worker
ordering/re-read rules. The revision-bearing point identity changes the experimental 0.1
projection layout; although that version is unreleased, rebuild the collection from authoritative
records rather than expecting legacy point IDs to be migrated in place.

`Scope` is expected to come from the trusted host after authorization. Search does not accept
caller-supplied filters, and always applies exact tenant, namespace, and profile conditions;
returned payloads are also checked defensively. Scope filters do not defend against a malicious
host or replace current-record authorization. Validate embedding policy and collection migration
before enabling a new profile.

Tests use Qdrant's local in-memory mode (`AsyncQdrantClient(":memory:")`) and make no claim about
live-service performance or distributed consistency.
