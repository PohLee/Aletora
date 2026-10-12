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
evidence_id=...)`; deletion is `await index.delete(scope, candidate_id)`. Evidence withdrawal
events can call `await index.delete_evidence(scope, evidence_id)`, which filters on tenant,
namespace, profile, and evidence ID. Supply evidence IDs on upsert if evidence invalidation
must remove all affected points. The projected payload contains only scope, candidate/evidence
IDs, profile ID, and revision—not candidate or evidence text. Search returns only `IndexHit`
candidate IDs, revisions, profile identity, and Qdrant score. It is a discovery mechanism, not
an authorization decision; `Memory` must reread the current candidate/evidence and the host
resolver must authorize and return the exact current formal object version.

Hosts should append projection events to their transactionally durable outbox and let a worker
re-read the authoritative record before computing/projecting it. The point ID is UUID5 over a
JSON array of tenant, namespace, candidate ID, and profile ID, so retrying the same current
projection (including a rebuild) replaces the same point without colliding across scopes or
profiles. Qdrant does not compare candidate revisions atomically. Concurrent workers or
out-of-order retries can therefore overwrite a newer vector with an older revision. Serialize
projection per scoped candidate, or otherwise ensure stale jobs re-read and project the current
record; revision equality is checked again by retrieval against the authoritative store. Do not
assume that a revision in Qdrant is authoritative or that an acknowledged write is transactionally
coupled to the host database. A failed index operation propagates and must be retried by the
host worker. A full rebuild should follow the same re-read/ordering rules; deletes should be
ordered with refreshes so an old refresh cannot resurrect a withdrawn point.

`Scope` is expected to come from the trusted host after authorization. Search does not accept
caller-supplied filters, and always applies exact tenant, namespace, and profile conditions;
returned payloads are also checked defensively. Scope filters do not defend against a malicious
host or replace current-record authorization. Validate embedding policy and collection migration
before enabling a new profile.

Tests use Qdrant's local in-memory mode (`AsyncQdrantClient(":memory:")`) and make no claim about
live-service performance or distributed consistency.
