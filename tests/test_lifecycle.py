import hashlib
from datetime import datetime, timezone

import pytest

from aletora import (
    EpistemicKind,
    FormalRef,
    GovernanceState,
    Proposal,
    Scope,
    SourceKind,
    SourceRef,
)
from aletora.adapters.memory import InMemoryDatabase, InMemoryEventSink, InMemoryRecordStore
from aletora.errors import IdempotencyConflict, InvalidTransition, NotFound, RevisionConflict
from aletora.memory import Memory
from aletora.ports import StoredRecord


@pytest.mark.asyncio
async def test_evidence_is_scoped_and_retrievable_through_memory():
    database = InMemoryDatabase()
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")
    source = SourceRef(SourceKind.DOCUMENT, "doc-1", "rev-1", "page:2")
    content = "A source statement"

    async with database.transaction():
        recorded = await memory.record_evidence(
            scope,
            source,
            content,
            idempotency_key="source-import-1",
            captured_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        loaded = await memory.get_evidence(scope, recorded.id)

    assert loaded == recorded
    assert recorded.content_hash == hashlib.sha256(content.encode()).hexdigest()


def _proposal(actor: str = "author"):
    return Proposal(
        "The source supports this claim",
        EpistemicKind.ASSERTED,
        actor,
        "extractor-v1",
        "run-1",
        datetime(2026, 1, 2, tzinfo=timezone.utc),
    )


async def _seed(memory: Memory, scope: Scope, suffix: str = "") -> tuple[str, str]:
    evidence = await memory.record_evidence(
        scope,
        SourceRef(SourceKind.EXPLICIT_USER, "user-input", "1", "message:1"),
        "A source statement",
        idempotency_key=f"evidence-key{suffix}",
        captured_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    candidate = await memory.propose(
        scope,
        evidence.id,
        _proposal(actor=f"author{suffix}"),
        idempotency_key=f"proposal-key{suffix}",
    )
    return evidence.id, candidate.id


class CorruptingReadStore:
    def __init__(self, store, kind: str, mode: str, version: object = None):
        self.store = store
        self.kind = kind
        self.mode = mode
        self.version = version

    async def get(self, scope: Scope, kind: str, record_id: str):
        record = await self.store.get(scope, kind, record_id)
        if record is None or kind != self.kind:
            return record
        payload = dict(record.payload)
        assert payload["schema_version"] == 1
        if self.mode == "missing":
            payload.pop("schema_version", None)
        else:
            payload["schema_version"] = self.version
        return StoredRecord(record.id, record.revision, payload)


class StaleCandidateReadStore:
    def __init__(self, store, stale: StoredRecord):
        self.store = store
        self.stale = stale

    async def get(self, scope: Scope, kind: str, record_id: str):
        if kind == "candidate" and record_id == self.stale.id:
            return self.stale
        return await self.store.get(scope, kind, record_id)

    async def insert(self, *args, **kwargs):
        return await self.store.insert(*args, **kwargs)


@pytest.mark.asyncio
async def test_import_keys_are_idempotent_and_conflicting_reuse_is_rejected():
    database = InMemoryDatabase()
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")
    source = SourceRef(SourceKind.DOCUMENT, "doc-1", "1", "p1")
    captured = datetime(2026, 1, 1, tzinfo=timezone.utc)

    async with database.transaction():
        first = await memory.record_evidence(
            scope, source, "same", idempotency_key="import", captured_at=captured
        )
        retry = await memory.record_evidence(
            scope, source, "same", idempotency_key="import", captured_at=captured
        )
        assert retry == first
        with pytest.raises(IdempotencyConflict):
            await memory.record_evidence(
                scope, source, "different", idempotency_key="import", captured_at=captured
            )


@pytest.mark.asyncio
async def test_proposal_retry_checks_semantics_and_returns_insert_winner_snapshot():
    database = InMemoryDatabase()
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    memory = Memory(store, sink)
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        evidence_id, _ = await _seed(memory, scope)
        proposal = _proposal()
        first = await memory.propose(scope, evidence_id, proposal, idempotency_key="proposal-retry")
        retry = await memory.propose(scope, evidence_id, proposal, idempotency_key="proposal-retry")
        assert retry == first
        with pytest.raises(IdempotencyConflict):
            await memory.propose(
                scope,
                evidence_id,
                Proposal(
                    "different immutable text",
                    proposal.epistemic_kind,
                    proposal.proposer_id,
                    proposal.extractor_id,
                    proposal.run_id,
                    proposal.proposed_at,
                ),
                idempotency_key="proposal-retry",
            )

        stale = await store.get(scope, "candidate", first.id)
        accepted = await memory.review(
            scope,
            first.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=first.revision,
        )
        assert stale is not None
        retry_memory = Memory(StaleCandidateReadStore(store, stale), sink)
        refreshed_retry = await retry_memory.propose(
            scope, evidence_id, proposal, idempotency_key="proposal-retry"
        )

    assert refreshed_retry == accepted
    assert refreshed_retry.revision == 2
    assert refreshed_retry.state is GovernanceState.ACCEPTED


@pytest.mark.asyncio
async def test_review_application_and_withdrawal_require_revisions_and_keep_receipts():
    database = InMemoryDatabase()
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        _, candidate_id = await _seed(memory, scope)
        with pytest.raises(InvalidTransition):
            await memory.review(
                scope,
                candidate_id,
                GovernanceState.ACCEPTED,
                "author",
                "self-review",
                expected_revision=1,
            )
        with pytest.raises(NotFound):
            await memory.get_candidate(Scope("tenant-b", "notes"), candidate_id)

        accepted = await memory.review(
            scope,
            candidate_id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "verified",
            expected_revision=1,
        )
        assert accepted.revision == 2
        assert accepted.application is None
        ref = FormalRef("billing", "Policy", "policy-1", "v7")
        applied = await memory.record_application(
            scope, candidate_id, ref, "host", expected_revision=2
        )
        assert applied.revision == 3
        assert applied.application is not None and applied.application.ref == ref
        assert (
            await memory.record_application(
                scope, candidate_id, ref, "retry-actor", expected_revision=3
            )
            == applied
        )
        with pytest.raises(InvalidTransition):
            await memory.record_application(
                scope,
                candidate_id,
                FormalRef("billing", "Policy", "policy-2", "v1"),
                "host",
                expected_revision=3,
            )
        withdrawn = await memory.withdraw(
            scope, candidate_id, "reviewer", "no longer current", expected_revision=3
        )
        assert withdrawn.state is GovernanceState.WITHDRAWN
        assert withdrawn.application == applied.application
        with pytest.raises(RevisionConflict):
            await memory.withdraw(scope, candidate_id, "reviewer", "stale", expected_revision=3)
    assert [event.action.value for event in database.events] == [
        "refresh_candidate",
        "refresh_candidate",
        "delete_candidate",
    ]


@pytest.mark.asyncio
async def test_withdrawn_evidence_retains_audit_and_blocks_new_proposals():
    database = InMemoryDatabase()
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        evidence = await memory.record_evidence(
            scope,
            SourceRef(SourceKind.TOOL, "lookup", "3", "result:1"),
            "Found value",
            idempotency_key="evidence-key",
            captured_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        prior_candidate = await memory.propose(
            scope, evidence.id, _proposal(), idempotency_key="before-withdrawal"
        )
        withdrawn = await memory.withdraw_evidence(
            scope, evidence.id, "owner", "source revoked", expected_revision=1
        )
        assert withdrawn.withdrawal is not None
        assert withdrawn.withdrawal.actor == "owner"
        with pytest.raises(InvalidTransition):
            await memory.review(
                scope,
                prior_candidate.id,
                GovernanceState.ACCEPTED,
                "reviewer",
                "cannot accept withdrawn lineage",
                expected_revision=1,
            )
        with pytest.raises(InvalidTransition):
            await memory.propose(scope, evidence.id, _proposal(), idempotency_key="later-proposal")
    assert database.events[0].action.value == "invalidate_evidence"


@pytest.mark.asyncio
async def test_outbox_failure_rolls_back_candidate_write_when_host_rolls_back():
    database = InMemoryDatabase()
    sink = InMemoryEventSink(database)
    memory = Memory(InMemoryRecordStore(database), sink)
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        _, candidate_id = await _seed(memory, scope)

    sink.failure = RuntimeError("durable outbox unavailable")
    with pytest.raises(RuntimeError, match="durable outbox unavailable"):
        async with database.transaction():
            await memory.review(
                scope,
                candidate_id,
                GovernanceState.ACCEPTED,
                "reviewer",
                "verified",
                expected_revision=1,
            )

    sink.failure = None
    async with database.transaction():
        unchanged = await memory.get_candidate(scope, candidate_id)
    assert unchanged.state is GovernanceState.PROPOSED
    assert unchanged.revision == 1
    assert database.events == ()


@pytest.mark.asyncio
async def test_supersession_requires_another_accepted_candidate_and_records_link():
    database = InMemoryDatabase()
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        _, old_id = await _seed(memory, scope, "-old")
        _, replacement_id = await _seed(memory, scope, "-new")
        old = await memory.review(
            scope,
            old_id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=1,
        )
        await memory.review(
            scope,
            replacement_id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=1,
        )
        with pytest.raises(InvalidTransition):
            await memory.supersede(
                scope, old_id, old_id, "reviewer", "bad replacement", expected_revision=2
            )
        superseded = await memory.supersede(
            scope,
            old_id,
            replacement_id,
            "reviewer",
            "newer evidence",
            expected_revision=old.revision,
        )
        replacement = await memory.get_candidate(scope, replacement_id)

    assert superseded.state is GovernanceState.SUPERSEDED
    assert superseded.replacement_id == replacement_id
    assert replacement.state is GovernanceState.ACCEPTED


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["evidence", "candidate"])
@pytest.mark.parametrize(
    ("mode", "version"),
    [("missing", None), ("value", 2), ("value", True), ("value", "1")],
)
async def test_memory_rejects_missing_or_invalid_persisted_schema_versions(kind, mode, version):
    database = InMemoryDatabase()
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    scope = Scope("tenant-a", "notes")
    async with database.transaction():
        evidence_id, candidate_id = await _seed(Memory(store, sink), scope)

    corrupted = Memory(CorruptingReadStore(store, kind, mode, version), sink)
    with pytest.raises(ValueError, match="schema_version"):
        async with database.transaction():
            if kind == "evidence":
                await corrupted.get_evidence(scope, evidence_id)
            else:
                await corrupted.get_candidate(scope, candidate_id)
