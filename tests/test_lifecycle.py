import hashlib
from datetime import UTC, datetime
from typing import Any

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
from aletora.errors import (
    CapabilityUnavailable,
    IdempotencyConflict,
    InvalidTransition,
    NotFound,
    RevisionConflict,
)
from aletora.memory import Memory
from aletora.ports import InsertResult, RecordStore, StoredRecord


@pytest.mark.asyncio
async def test_evidence_is_scoped_and_retrievable_through_memory() -> None:
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
            captured_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        loaded = await memory.get_evidence(scope, recorded.id)

    assert loaded == recorded
    assert recorded.content_hash == hashlib.sha256(content.encode()).hexdigest()


def _proposal(actor: str = "author") -> Proposal:
    return Proposal(
        "The source supports this claim",
        EpistemicKind.ASSERTED,
        actor,
        "extractor-v1",
        "run-1",
        datetime(2026, 1, 2, tzinfo=UTC),
    )


async def _seed(memory: Memory, scope: Scope, suffix: str = "") -> tuple[str, str]:
    evidence = await memory.record_evidence(
        scope,
        SourceRef(SourceKind.EXPLICIT_USER, "user-input", "1", "message:1"),
        "A source statement",
        idempotency_key=f"evidence-key{suffix}",
        captured_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    candidate = await memory.propose(
        scope,
        evidence.id,
        _proposal(actor=f"author{suffix}"),
        idempotency_key=f"proposal-key{suffix}",
    )
    return evidence.id, candidate.id


class CorruptingReadStore:
    def __init__(self, store: RecordStore, kind: str, mode: str, version: object = None) -> None:
        self.store = store
        self.kind = kind
        self.mode = mode
        self.version = version

    async def get(
        self, scope: Scope, kind: str, record_id: str, *, for_update: bool = False
    ) -> StoredRecord | None:
        record = await self.store.get(scope, kind, record_id, for_update=for_update)
        if record is None or kind != self.kind:
            return record
        payload = dict(record.payload)
        assert payload["schema_version"] == 1
        if self.mode == "missing":
            payload.pop("schema_version", None)
        elif self.mode == "corrupt_field":
            payload["scope"] = "not-an-object"
        elif self.mode == "missing_key":
            payload.pop("source", None)
            payload.pop("history", None)
        elif self.mode == "empty_optional_object":
            payload["withdrawal"] = {}
            payload["application"] = {}
        elif self.mode == "bad_history_type":
            payload["history"] = "abc"
        else:
            payload["schema_version"] = self.version
        return StoredRecord(record.id, record.revision, payload)


class MissingCandidateReadStore:
    def __init__(self, store: RecordStore, candidate_id: str) -> None:
        self.store = store
        self.candidate_id = candidate_id

    async def get(
        self, scope: Scope, kind: str, record_id: str, *, for_update: bool = False
    ) -> StoredRecord | None:
        if kind == "candidate" and record_id == self.candidate_id:
            return None
        return await self.store.get(scope, kind, record_id, for_update=for_update)

    async def insert(self, *args: Any, **kwargs: Any) -> InsertResult:
        return await self.store.insert(*args, **kwargs)


class MinimalRecordStore:
    """Custom V1 RecordStore implementing only the core three methods."""

    def __init__(self, inner: InMemoryRecordStore) -> None:
        self._inner = inner
        self.get_calls: list[tuple[str, str, bool]] = []
        self.insert_calls: list[str] = []

    async def get(
        self, scope: Scope, kind: str, record_id: str, *, for_update: bool = False
    ) -> StoredRecord | None:
        self.get_calls.append((kind, record_id, for_update))
        return await self._inner.get(scope, kind, record_id, for_update=for_update)

    async def insert(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        idempotency_key: str,
        fingerprint: str,
        payload: dict[str, Any],
    ) -> InsertResult:
        self.insert_calls.append(idempotency_key)
        return await self._inner.insert(
            scope, kind, record_id, idempotency_key, fingerprint, payload
        )

    async def update(
        self,
        scope: Scope,
        kind: str,
        record_id: str,
        expected_revision: int,
        payload: dict[str, Any],
    ) -> StoredRecord:
        return await self._inner.update(scope, kind, record_id, expected_revision, payload)


@pytest.mark.asyncio
async def test_import_keys_are_idempotent_with_differing_captured_at_and_reject_conflicts() -> None:
    database = InMemoryDatabase()
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")
    source = SourceRef(SourceKind.DOCUMENT, "doc-1", "1", "p1")
    captured = datetime(2026, 1, 1, tzinfo=UTC)
    later = datetime(2026, 1, 5, tzinfo=UTC)

    async with database.transaction():
        first = await memory.record_evidence(
            scope, source, "same", idempotency_key="import", captured_at=captured
        )
        retry = await memory.record_evidence(
            scope, source, "same", idempotency_key="import", captured_at=later
        )
        assert retry == first
        assert retry.captured_at == captured
        with pytest.raises(IdempotencyConflict):
            await memory.record_evidence(
                scope, source, "different", idempotency_key="import", captured_at=captured
            )


@pytest.mark.asyncio
async def test_proposal_retry_checks_semantics_and_returns_insert_winner_snapshot() -> None:
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

        accepted = await memory.review(
            scope,
            first.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=first.revision,
        )
        retry_memory = Memory(MissingCandidateReadStore(store, first.id), sink)  # type: ignore[arg-type]
        refreshed_retry = await retry_memory.propose(
            scope, evidence_id, proposal, idempotency_key="proposal-retry"
        )

    assert refreshed_retry == accepted
    assert refreshed_retry.revision == 2
    assert refreshed_retry.state is GovernanceState.ACCEPTED


@pytest.mark.asyncio
async def test_review_application_and_withdrawal_require_revisions_and_keep_receipts() -> None:
    database = InMemoryDatabase()
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        evidence_id, candidate_id = await _seed(memory, scope)
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
        await memory.withdraw_evidence(scope, evidence_id, "owner", "revoked", expected_revision=1)
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
        "invalidate_evidence",
        "delete_candidate",
    ]


@pytest.mark.asyncio
async def test_review_and_mutation_methods_validate_inputs_before_store_access() -> None:
    database = InMemoryDatabase()
    minimal = MinimalRecordStore(InMemoryRecordStore(database))
    memory = Memory(minimal, InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        evidence_id, candidate_id = await _seed(memory, scope)
        # Minimal store falls back to for_update=True when ShareLockableRecordStore is absent.
        assert ("evidence", evidence_id, True) in minimal.get_calls
        minimal.get_calls.clear()

        for bad_decision in ("invalid", None, 123, "accepted", "rejected"):
            with pytest.raises(ValueError, match="GovernanceState"):
                await memory.review(
                    scope,
                    candidate_id,
                    bad_decision,  # type: ignore[arg-type]
                    "reviewer",
                    "reason",
                    expected_revision=1,
                )
        with pytest.raises(ValueError, match="actor"):
            await memory.review(
                scope,
                candidate_id,
                GovernanceState.ACCEPTED,
                " ",
                "reason",
                expected_revision=1,
            )
        with pytest.raises(ValueError, match="FormalRef"):
            await memory.record_application(
                scope,
                candidate_id,
                "not-a-ref",  # type: ignore[arg-type]
                "host",
                expected_revision=1,
            )
        with pytest.raises(ValueError, match="reason"):
            await memory.withdraw(scope, candidate_id, "actor", " ", expected_revision=1)
        assert minimal.get_calls == []

        with pytest.raises(InvalidTransition, match="ACCEPTED or REJECTED"):
            await memory.review(
                scope,
                candidate_id,
                GovernanceState.PROPOSED,
                "reviewer",
                "bad state",
                expected_revision=1,
            )
        assert minimal.get_calls == []

        # Mutations acquire candidate FOR UPDATE before locking parent evidence.
        accepted = await memory.review(
            scope,
            candidate_id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=1,
        )
        assert minimal.get_calls[:2] == [
            ("candidate", candidate_id, True),
            ("evidence", accepted.evidence_id, True),
        ]
        minimal.get_calls.clear()
        await memory.record_application(
            scope,
            candidate_id,
            FormalRef("host", "Policy", "p-lock", "v1"),
            "host-actor",
            expected_revision=accepted.revision,
        )
        assert minimal.get_calls[:2] == [
            ("candidate", candidate_id, True),
            ("evidence", accepted.evidence_id, True),
        ]


@pytest.mark.asyncio
async def test_memory_rejects_nul_bytes_across_inputs() -> None:
    database = InMemoryDatabase()
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        with pytest.raises(ValueError, match="NUL"):
            await memory.record_evidence(
                scope,
                SourceRef(SourceKind.DOCUMENT, "doc", "1", "p1"),
                "bad\x00text",
                idempotency_key="k",
            )
        with pytest.raises(ValueError, match="NUL"):
            await memory.record_evidence(
                scope,
                SourceRef(SourceKind.DOCUMENT, "doc", "1", "p1"),
                "ok",
                idempotency_key="bad\x00key",
            )
        evidence_id, candidate_id = await _seed(memory, scope)
        with pytest.raises(ValueError, match="NUL"):
            await memory.review(
                scope,
                candidate_id,
                GovernanceState.ACCEPTED,
                "reviewer\x00",
                "ok",
                expected_revision=1,
            )
        with pytest.raises(ValueError, match="NUL"):
            await memory.withdraw_evidence(
                scope, evidence_id, "owner", "bad\x00reason", expected_revision=1
            )
        with pytest.raises(ValueError, match="NUL"):
            await memory.retrieve(scope, "bad\x00query", limit=1)


@pytest.mark.asyncio
async def test_propose_many_and_list_candidates_behavior() -> None:
    database = InMemoryDatabase()
    minimal = MinimalRecordStore(InMemoryRecordStore(database))
    sink = InMemoryEventSink(database)
    store = InMemoryRecordStore(database)
    memory = Memory(store, sink)
    minimal_memory = Memory(minimal, sink)
    scope = Scope("tenant-a", "notes")
    other_scope = Scope("tenant-a", "other")

    async with database.transaction():
        evidence = await memory.record_evidence(
            scope,
            SourceRef(SourceKind.DOCUMENT, "doc-batch", "1", "p1"),
            "Shared document source",
            idempotency_key="batch-evidence",
            captured_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        assert await memory.propose_many(scope, evidence.id, []) == ()

        items = [
            (_proposal(actor="author-1"), "batch-c1"),
            (_proposal(actor="author-2"), "batch-c2"),
            (_proposal(actor="author-3"), "batch-c3"),
        ]
        batch = await minimal_memory.propose_many(scope, evidence.id, items)
        assert len(batch) == 3
        # Evidence was locked/loaded once for the entire batch of 3 proposals.
        assert [kind for kind, _, _ in minimal.get_calls].count("evidence") == 1

        minimal.get_calls.clear()
        replay = await minimal_memory.propose_many(scope, evidence.id, items)
        assert replay == batch
        assert (
            await minimal_memory.propose(
                scope, evidence.id, items[0][0], idempotency_key=items[0][1]
            )
            == batch[0]
        )
        assert all(kind == "candidate" and not locked for kind, _, locked in minimal.get_calls)

        with pytest.raises(IdempotencyConflict):
            await memory.propose_many(
                scope,
                evidence.id,
                [
                    (_proposal(), "fresh-before-conflict"),
                    (
                        Proposal(
                            "changed",
                            EpistemicKind.ASSERTED,
                            "a",
                            "e",
                            "r",
                            _proposal().proposed_at,
                        ),
                        "batch-c1",
                    ),
                ],
            )

        # Scope isolation and keyset pagination on list_candidates.
        await memory.record_evidence(
            other_scope,
            SourceRef(SourceKind.DOCUMENT, "doc-other", "1", "p1"),
            "Other scope source",
            idempotency_key="other-ev",
            captured_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        page1 = await memory.list_candidates(scope, limit=2)
        assert len(page1) == 2
        page2 = await memory.list_candidates(scope, after_id=page1[-1].id, limit=2)
        assert len(page2) == 1
        page3 = await memory.list_candidates(scope, after_id=page2[-1].id, limit=2)
        assert page3 == ()
        assert {c.id for c in (*page1, *page2)} == {c.id for c in batch}

        with pytest.raises(CapabilityUnavailable, match="EnumerableRecordStore"):
            await minimal_memory.list_candidates(scope)

        # Validation and intra-batch conflicts fail before inserting any record.
        minimal.insert_calls.clear()
        minimal.get_calls.clear()
        for bad_items in (None, "not-a-sequence", (entry for entry in items)):
            with pytest.raises(ValueError, match="sequence"):
                await minimal_memory.propose_many(
                    scope,
                    evidence.id,
                    bad_items,  # type: ignore[arg-type]
                )
        with pytest.raises(IdempotencyConflict, match="within batch"):
            await minimal_memory.propose_many(
                scope,
                evidence.id,
                [
                    (_proposal(actor="author-a"), "dup-key"),
                    (_proposal(actor="author-b"), "dup-key"),
                ],
            )
        assert minimal.insert_calls == []
        assert minimal.get_calls == []

        # Mixing an existing key with a new key after withdrawal fails with zero inserts.
        await minimal_memory.withdraw_evidence(
            scope, evidence.id, "owner", "revoked", expected_revision=evidence.revision
        )
        minimal.insert_calls.clear()
        with pytest.raises(InvalidTransition, match="withdrawn"):
            await minimal_memory.propose_many(
                scope,
                evidence.id,
                [items[0], (_proposal(actor="author-new"), "brand-new-key")],
            )
        assert minimal.insert_calls == []


@pytest.mark.asyncio
async def test_withdrawn_evidence_retains_audit_and_blocks_new_proposals() -> None:
    database = InMemoryDatabase()
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        evidence = await memory.record_evidence(
            scope,
            SourceRef(SourceKind.TOOL, "lookup", "3", "result:1"),
            "Found value",
            idempotency_key="evidence-key",
            captured_at=datetime(2026, 1, 1, tzinfo=UTC),
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
        # Replay of an already-inserted proposal after withdrawal still returns it.
        assert (
            await memory.propose(
                scope, evidence.id, _proposal(), idempotency_key="before-withdrawal"
            )
            == prior_candidate
        )
    assert database.events[0].action.value == "invalidate_evidence"


@pytest.mark.asyncio
async def test_outbox_failure_rolls_back_candidate_write_when_host_rolls_back() -> None:
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
async def test_supersession_requires_another_accepted_candidate_and_records_link() -> None:
    database = InMemoryDatabase()
    minimal = MinimalRecordStore(InMemoryRecordStore(database))
    memory = Memory(minimal, InMemoryEventSink(database))
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        _, old_id = await _seed(memory, scope, "-old")
        replacement_evidence_id, replacement_id = await _seed(memory, scope, "-new")
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
        minimal.get_calls.clear()
        superseded = await memory.supersede(
            scope,
            old_id,
            replacement_id,
            "reviewer",
            "newer evidence",
            expected_revision=old.revision,
        )
        assert minimal.get_calls == [
            *(("candidate", record_id, True) for record_id in sorted((old_id, replacement_id))),
            ("evidence", replacement_evidence_id, True),
        ]
        replacement = await memory.get_candidate(scope, replacement_id)

    assert superseded.state is GovernanceState.SUPERSEDED
    assert superseded.replacement_id == replacement_id
    assert replacement.state is GovernanceState.ACCEPTED


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["evidence", "candidate"])
@pytest.mark.parametrize(
    ("mode", "version"),
    [
        ("missing", None),
        ("value", 2),
        ("value", True),
        ("value", "1"),
        ("corrupt_field", None),
        ("missing_key", None),
        ("empty_optional_object", None),
    ],
)
async def test_memory_rejects_missing_or_corrupt_persisted_payloads(
    kind: str, mode: str, version: object
) -> None:
    database = InMemoryDatabase()
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    scope = Scope("tenant-a", "notes")
    async with database.transaction():
        evidence_id, candidate_id = await _seed(Memory(store, sink), scope)

    corrupted = Memory(
        CorruptingReadStore(store, kind, mode, version),  # type: ignore[arg-type]
        sink,
    )
    with pytest.raises(ValueError):
        async with database.transaction():
            if kind == "evidence":
                await corrupted.get_evidence(scope, evidence_id)
            else:
                await corrupted.get_candidate(scope, candidate_id)


@pytest.mark.asyncio
async def test_memory_rejects_non_list_candidate_history() -> None:
    database = InMemoryDatabase()
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    scope = Scope("tenant-a", "notes")
    async with database.transaction():
        _, candidate_id = await _seed(Memory(store, sink), scope)

    corrupted = Memory(
        CorruptingReadStore(store, "candidate", "bad_history_type"),  # type: ignore[arg-type]
        sink,
    )
    with pytest.raises(ValueError, match="history"):
        async with database.transaction():
            await corrupted.get_candidate(scope, candidate_id)
