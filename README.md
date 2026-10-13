# Aletora

Aletora is a Python package for keeping source evidence, governing knowledge candidates, and retrieving only knowledge that the host application currently authorizes.

It is designed to be as a dependency. Each application remains the authority for its own users, business records, authorization, database transactions, migrations, and background workers.

Inspired by Greek aletheia (truth) and orchestration. Suggests an intelligence engine that turns trustworthy knowledge into action. Evidence into intelligence. Intelligence into action.


> **V1 / experimental:** Aletora 0.1 has not been publicly released. Its adapter interfaces may still change.

## How it fits into your application

~~~text
Your application / extractor
  └─ records Evidence and proposes a Candidate
       └─ an authenticated reviewer accepts or rejects it
            └─ your application writes the formal business record
                 ├─ Memory records an ApplicationReceipt
                 └─ the same PostgreSQL transaction appends an outbox event
                      └─ your worker projects eligible candidates to Qdrant
                           └─ Memory retrieves candidates and asks your host
                              to authorize and return the exact current record
~~~

Aletora manages **Evidence** (a versioned, traceable source snapshot) and **Candidate** (an assertion, inference, or question that needs governance). Acceptance is a review decision; it does not publish a business record. Only after your application writes that record and records its exact version as an application receipt can the candidate be retrieved.

Aletora does not extract documents, call an LLM, accept candidates automatically, store your application's formal records, or decide who may access them. Your application performs extraction, authenticates reviewers, and supplies authorization.

## Where data lives

- Aletora records live in PostgreSQL, using the host application's existing database and transaction. The package does not create a separate database or connection pool.
- Your application owns the formal business records and the durable outbox table.
- Qdrant is optional. When used, it is a rebuildable search projection, not the source of truth.
- The SQLAlchemy and Tortoise adapters participate in a transaction that your application already opened. They never commit, roll back, or close it.
- The package core has no third-party runtime dependencies. PostgreSQL adapters and Qdrant are optional extras.

Your host creates Scope(tenant_id, namespace) only after authenticating and authorizing the caller. Scope keeps records separate; it is not an authentication token.

## Install

Install from a checkout with the adapters you plan to use:

~~~sh
python -m pip install -e '.[sqlalchemy,qdrant]'
# Or install the Tortoise PostgreSQL adapter:
python -m pip install -e '.[tortoise,qdrant]'
~~~

For applications that do not use Qdrant, omit the qdrant extra. Import optional adapters explicitly:

~~~python
from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore
from aletora.adapters.qdrant import QdrantIndex
~~~

Core imports do not load SQLAlchemy, Tortoise, Qdrant, or an embedding provider.

## 1. Prepare PostgreSQL

Apply one migration with your application's migration tooling. Aletora does not apply migrations on startup:

~~~python
from aletora.migrations import render_migration

# Apply the returned SQL through your normal migration runner.
sql = render_migration("dedicated.sql", schema="app_data")
~~~

Use shared.sql if the Aletora table should use PostgreSQL row-level security, or dedicated.sql for a dedicated schema/table. Configure tenant RLS context within each transaction when using the shared template. See [PostgreSQL integration](docs/POSTGRES.md) for schema, RLS and transaction requirements.

Your host also needs a durable outbox table for IndexEvent rows. Implement the small EventSink.append(event) interface by inserting into that table on the **same transaction** as Aletora's records. Aletora does not provide or own the host outbox.

## 2. Record evidence and propose a candidate

Extraction can come from a user, your application, a tool, or an external model. Store the source snapshot and the proposed meaning separately. The example below uses SQLAlchemy; HostOutbox represents your own EventSink implementation bound to the same session.

~~~python
from datetime import datetime, timezone

from aletora import (
    EpistemicKind,
    Memory,
    Proposal,
    Scope,
    SourceKind,
    SourceRef,
)
from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore


async def capture_and_propose(session, host_outbox, authorized_tenant, brand_id):
    # Build Scope only after the host has authenticated and authorized the caller.
    scope = Scope(authorized_tenant, brand_id)
    memory = Memory(
        SQLAlchemyRecordStore(session, schema="app_data"),
        host_outbox,  # Host EventSink using this same session.
    )

    evidence = await memory.record_evidence(
        scope,
        SourceRef(
            SourceKind.DOCUMENT,
            source_id="brand-guidelines",
            version="2026-10",
            locator="headlines/section-2",
        ),
        content="The brand guide recommends short, direct headlines.",
        idempotency_key="brand-guidelines-2026-10-headlines-2",
        captured_at=datetime.now(timezone.utc),
    )
    candidate = await memory.propose(
        scope,
        evidence.id,
        Proposal(
            text="Use short, direct headlines.",
            epistemic_kind=EpistemicKind.ASSERTED,
            proposer_id="brand-editor-17",
            extractor_id="guideline-importer",
            run_id="import-2026-10-13",
            proposed_at=datetime.now(timezone.utc),
        ),
        idempotency_key="brand-guidelines-2026-10-short-headlines",
    )
    return scope, candidate
~~~

Call this within a host-owned transaction, for example:

~~~python
async with session_factory.begin() as session:
    scope, candidate = await capture_and_propose(
        session, HostOutbox(session), authorized_tenant, brand_id
    )
~~~

The HostOutbox class is application code. It should persist each event's ID, scope, action, record kind, record ID, and revision using the supplied session. If anything fails, let the exception leave the transaction so PostgreSQL rolls back all writes.

## 3. Review, then apply to your formal record

A separately authenticated reviewer records a decision and reason. A proposer cannot review their own candidate.

~~~python
from aletora import GovernanceState, Memory
from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore


async with session_factory.begin() as session:
    memory = Memory(
        SQLAlchemyRecordStore(session, schema="app_data"),
        HostOutbox(session),
    )
    current = await memory.get_candidate(scope, candidate.id)
    accepted = await memory.review(
        scope,
        current.id,
        GovernanceState.ACCEPTED,
        actor="independent-reviewer-4",
        reason="Verified against the cited source.",
        expected_revision=current.revision,
    )
~~~

When the host creates or updates its formal business record, record the exact reference and version in that same host transaction:

~~~python
from aletora import FormalRef, GovernanceState, Memory
from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore


async with session_factory.begin() as session:
    memory = Memory(
        SQLAlchemyRecordStore(session, schema="app_data"),
        HostOutbox(session),
    )

    current = await memory.get_candidate(scope, accepted.id)
    if current.state is not GovernanceState.ACCEPTED:
        raise ValueError("candidate is no longer accepted")

    # Your application writes the authoritative record first.
    await host_policies.save(session, policy_id, approved_text)

    applied = await memory.record_application(
        scope,
        current.id,
        FormalRef(
            owner="brand-service",
            object_type="headline-policy",
            object_id=policy_id,
            version="v3",
        ),
        actor="policy-publisher",
        expected_revision=current.revision,
    )
~~~

The formal write, application receipt, and outbox event must commit or roll back together. An accepted candidate without an application receipt is not eligible for retrieval. Every mutation that changes a candidate requires its current expected_revision; reload the record after a revision conflict.

The Tortoise adapter follows the same ownership rule:

~~~python
from tortoise.transactions import in_transaction
from aletora import Memory
from aletora.adapters.tortoise import TortoiseRecordStore


async with in_transaction("default") as transaction:
    memory = Memory(
        TortoiseRecordStore(transaction, schema="app_data"),
        HostOutbox(transaction),
    )
    # Use memory and perform host writes within this transaction.
~~~

## 4. Project eligible candidates to Qdrant

Qdrant and embeddings are optional. Your application owns the embedding model and produces vectors both for projection and for queries. The embedding profile identifies the vector model, dimension, preprocessing version, and chunking version.

~~~python
from qdrant_client import AsyncQdrantClient

from aletora import EmbeddingProfile
from aletora.adapters.qdrant import QdrantIndex


profile = EmbeddingProfile(
    model="your-embedding-model",
    dimension=384,
    preprocessing_version="text-v1",
    chunking_version="whole-candidate-v1",
)
qdrant = AsyncQdrantClient(url=settings.qdrant_url)  # Host-owned client.
index = QdrantIndex(
    qdrant,
    collection="brand-knowledge",
    profile=profile,
    embed=your_async_embed_function,
)
await index.ensure_collection()  # Explicit startup/provisioning step.
~~~

A host worker reads committed events from its durable outbox. Before a refresh, it re-reads the current Candidate and Evidence through Memory. If the candidate is still accepted and applied and its evidence is active, embed the candidate text and call:

~~~python
await index.upsert(
    scope,
    candidate.id,
    candidate.revision,
    vector,
    evidence_id=candidate.evidence_id,
)
~~~

For a candidate deletion event, call index.delete(scope, candidate_id, terminal_revision). For evidence withdrawal, call index.delete_evidence(scope, evidence_id). Deduplicate outbox events, serialize projection updates for the same candidate where possible, and retry failed worker operations. Qdrant writes are not atomic with PostgreSQL; every retrieval therefore checks current revisions and evidence again. See [Qdrant integration](docs/QDRANT.md) for stale projection behavior, collection requirements and rebuild guidance.

## 5. Retrieve for an authorized request

Bind the resolver to the current request's authenticated user. It must return only the exact current FormalRef that this user may read. Return None for a missing, changed, or unauthorized record.

~~~python
from aletora import (
    Memory,
    ResolvedRecord,
    Scope,
)
from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore


class RequestResolver:
    def __init__(self, session, user):
        self.session = session
        self.user = user

    async def resolve(self, scope, ref):
        # Implement this host query with the caller's authorization policy.
        row = await host_policies.get_current_authorized(
            self.session, self.user, scope, ref
        )
        if row is None or row.version != ref.version:
            return None
        return ResolvedRecord(ref=ref, content=row.content)


async def answer_context(session, user, authorized_tenant, brand_id, index, profile):
    scope = Scope(authorized_tenant, brand_id)
    memory = Memory(
        SQLAlchemyRecordStore(session, schema="app_data"),
        HostOutbox(session),
        index=index,
        resolver=RequestResolver(session, user),
        profile=profile,
        scan_budget=200,  # Maximum raw Qdrant points examined per retrieval.
    )
    return await memory.retrieve(
        scope,
        "What tone should a campaign headline use?",
        limit=5,
    )
~~~

Call answer_context inside the request's host transaction/session. Results contain the host-authorized formal content, exact reference, current candidate revision, epistemic kind, retrieval score, and source citations. A similarity hit is only a discovery hint. If the resolver returns a different version, or the candidate/evidence is stale, withdrawn, unreviewed, unapplied, or invalid, Memory filters it out. Adapter and resolver failures propagate; there is no fallback that returns unverified index text.

## Lifecycle operations

- record_evidence(scope, source, content, idempotency_key=...) records an immutable source snapshot and content hash.
- propose(scope, evidence_id, proposal, idempotency_key=...) creates a Candidate in PROPOSED state.
- review(..., ACCEPTED | REJECTED, ..., expected_revision=...) records an attributable decision; it does not write a formal record.
- record_application(scope, candidate_id, FormalRef(...), actor, expected_revision=...) records the host's successful formal write.
- withdraw(..., expected_revision=...) and supersede(..., expected_revision=...) change candidate governance state with an audit reason.
- withdraw_evidence(..., expected_revision=...) blocks retrieval of candidates supported by that Evidence while retaining its audit history.

See the [API examples and guarantees](docs/V1-SPEC.md), [PostgreSQL integration](docs/POSTGRES.md), and [Qdrant integration](docs/QDRANT.md) for full method contracts.

## Local development and verification

~~~sh
uv sync --frozen --all-extras --group dev
uv run --frozen ruff check .
uv run --frozen ruff format --check src tests
uv run --frozen mypy
uv run --frozen pytest
~~~

PostgreSQL adapter tests require an isolated database in ALETORA_TEST_POSTGRES_DSN; see [development instructions](docs/DEVELOPMENT.md). The full verification gate tests Python 3.12 and 3.14 against real PostgreSQL and local-mode Qdrant. See [the validation record](docs/VALIDATION.md) for the tested versions and current integration limits.


## License

Apache-2.0.
