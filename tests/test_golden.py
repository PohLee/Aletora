"""Acceptance cases for the public V1 Memory contract."""

import asyncio
from datetime import datetime, timezone

import pytest

from aletora import (
    EmbeddingProfile,
    EpistemicKind,
    FormalRef,
    GovernanceState,
    IndexHit,
    Memory,
    Proposal,
    ResolvedRecord,
    Scope,
    SourceKind,
    SourceRef,
)
from aletora.adapters.memory import InMemoryDatabase, InMemoryEventSink, InMemoryRecordStore
from aletora.errors import IdempotencyConflict, InvalidTransition, NotFound, RevisionConflict

NOW = datetime(2026, 2, 1, tzinfo=timezone.utc)
PROFILE = EmbeddingProfile("embed-v2", 4, "prep-v1", "chunk-v3")


def _source(source_id: str = "source") -> SourceRef:
    return SourceRef(SourceKind.DOCUMENT, source_id, "v1", "page:1")


def _proposal(
    actor: str = "author",
    *,
    text: str = "claim from source",
    valid_from: datetime | None = None,
    valid_until: datetime | None = None,
) -> Proposal:
    return Proposal(
        text,
        EpistemicKind.ASSERTED,
        actor,
        "extractor-v1",
        "run-1",
        NOW,
        valid_from,
        valid_until,
    )


async def _evidence(memory: Memory, scope: Scope, key: str = "evidence"):
    return await memory.record_evidence(
        scope,
        _source(key),
        f"evidence content {key}",
        idempotency_key=key,
        captured_at=NOW,
    )


async def _candidate(memory: Memory, scope: Scope, key: str = "candidate", **kwargs):
    evidence = await _evidence(memory, scope, f"evidence-{key}")
    candidate = await memory.propose(
        scope,
        evidence.id,
        _proposal(actor=f"author-{key}", **kwargs),
        idempotency_key=key,
    )
    return evidence, candidate


async def _accepted_applied(memory: Memory, scope: Scope, key: str = "candidate", **kwargs):
    evidence, candidate = await _candidate(memory, scope, key, **kwargs)
    candidate = await memory.review(
        scope,
        candidate.id,
        GovernanceState.ACCEPTED,
        "reviewer",
        "approved",
        expected_revision=candidate.revision,
    )
    ref = FormalRef("host", "Policy", f"formal-{key}", "v1")
    candidate = await memory.record_application(
        scope, candidate.id, ref, "host-actor", expected_revision=candidate.revision
    )
    return evidence, candidate


class _SearchIndex:
    def __init__(self, hits: list[IndexHit]):
        self.hits = hits
        self.calls: list[tuple[Scope, str, int, int]] = []

    async def search(self, scope: Scope, query: str, *, limit: int, offset: int):
        self.calls.append((scope, query, limit, offset))
        return self.hits[offset : offset + limit]


class _Resolver:
    def __init__(self, records: dict[FormalRef, ResolvedRecord | None]):
        self.records = records
        self.calls: list[tuple[Scope, FormalRef]] = []

    async def resolve(self, scope: Scope, ref: FormalRef):
        self.calls.append((scope, ref))
        return self.records.get(ref)


def _memory(database: InMemoryDatabase, **kwargs) -> Memory:
    return Memory(InMemoryRecordStore(database), InMemoryEventSink(database), **kwargs)


@pytest.mark.asyncio
async def test_identical_evidence_in_distinct_scopes_remains_distinct():
    database = InMemoryDatabase()
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database))
    scopes = (
        Scope("tenant-a", "notes"),
        Scope("tenant-b", "notes"),
        Scope("tenant-a", "archive"),
    )
    source = SourceRef(SourceKind.DOCUMENT, "same-document", "v1", "page:1")

    async with database.transaction():
        recorded = [
            await memory.record_evidence(
                scope,
                source,
                "identical source content",
                idempotency_key="same-import",
                captured_at=NOW,
            )
            for scope in scopes
        ]
        loaded = [
            await memory.get_evidence(scope, evidence.id)
            for scope, evidence in zip(scopes, recorded, strict=True)
        ]

    assert len(set(evidence.id for evidence in recorded)) == 3
    assert len(set(evidence.scope for evidence in recorded)) == 3
    assert loaded == recorded


@pytest.mark.asyncio
async def test_identical_proposal_retry_is_stable_and_semantic_reuse_conflicts():
    database = InMemoryDatabase()
    memory = _memory(database)
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        evidence = await _evidence(memory, scope)
        original = await memory.propose(
            scope, evidence.id, _proposal(), idempotency_key="proposal-key"
        )
        replay = await memory.propose(
            scope, evidence.id, _proposal(), idempotency_key="proposal-key"
        )
        loaded = await memory.get_candidate(scope, original.id)
        with pytest.raises(IdempotencyConflict):
            await memory.propose(
                scope,
                evidence.id,
                _proposal(text="different claim"),
                idempotency_key="proposal-key",
            )

    assert replay == original == loaded
    assert original.revision == 1


@pytest.mark.asyncio
async def test_rejected_candidate_cannot_be_applied():
    database = InMemoryDatabase()
    memory = _memory(database)
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        _, candidate = await _candidate(memory, scope)
        rejected = await memory.review(
            scope,
            candidate.id,
            GovernanceState.REJECTED,
            "reviewer",
            "unsupported",
            expected_revision=candidate.revision,
        )
        with pytest.raises(InvalidTransition):
            await memory.record_application(
                scope,
                candidate.id,
                FormalRef("host", "Policy", "policy", "v1"),
                "host-actor",
                expected_revision=rejected.revision,
            )
        unchanged = await memory.get_candidate(scope, candidate.id)

    assert unchanged == rejected
    assert unchanged.application is None


@pytest.mark.asyncio
async def test_acceptance_without_application_is_not_a_retrieval_result():
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "notes")
    lifecycle = _memory(database)
    async with database.transaction():
        _, candidate = await _candidate(lifecycle, scope)
        accepted = await lifecycle.review(
            scope,
            candidate.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=candidate.revision,
        )

    index = _SearchIndex([IndexHit(accepted.id, accepted.revision, 0.9, PROFILE.profile_id)])
    resolver = _Resolver({})
    retrieval = _memory(
        database,
        index=index,
        resolver=resolver,
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        results = await retrieval.retrieve(scope, "query", limit=1)

    assert accepted.application is None
    assert results == ()
    assert resolver.calls == []


@pytest.mark.asyncio
async def test_application_receipt_cannot_be_reassigned():
    database = InMemoryDatabase()
    memory = _memory(database)
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        _, candidate = await _candidate(memory, scope)
        accepted = await memory.review(
            scope,
            candidate.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=candidate.revision,
        )
        first_ref = FormalRef("host", "Policy", "policy-1", "v1")
        applied = await memory.record_application(
            scope, candidate.id, first_ref, "host-actor", expected_revision=accepted.revision
        )
        with pytest.raises(InvalidTransition):
            await memory.record_application(
                scope,
                candidate.id,
                FormalRef("host", "Policy", "policy-2", "v1"),
                "host-actor",
                expected_revision=applied.revision,
            )
        unchanged = await memory.get_candidate(scope, candidate.id)

    assert unchanged.application == applied.application


@pytest.mark.asyncio
async def test_withdrawn_applied_candidate_keeps_receipt_but_is_not_retrieved():
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "notes")
    lifecycle = _memory(database, clock=lambda: NOW)
    async with database.transaction():
        _, applied = await _accepted_applied(lifecycle, scope)
        withdrawn = await lifecycle.withdraw(
            scope,
            applied.id,
            "reviewer",
            "no longer current",
            expected_revision=applied.revision,
        )

    assert withdrawn.application == applied.application
    index = _SearchIndex([IndexHit(withdrawn.id, withdrawn.revision, 0.9, PROFILE.profile_id)])
    resolver = _Resolver(
        {applied.application.ref: ResolvedRecord(applied.application.ref, "secret")}
    )
    retrieval = _memory(
        database,
        index=index,
        resolver=resolver,
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        results = await retrieval.retrieve(scope, "query", limit=1)

    assert results == ()
    assert resolver.calls == []


@pytest.mark.asyncio
async def test_superseded_candidate_is_excluded_in_favor_of_replacement():
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "notes")
    lifecycle = _memory(database, clock=lambda: NOW)
    async with database.transaction():
        _, old = await _accepted_applied(lifecycle, scope, "old")
        _, replacement = await _accepted_applied(lifecycle, scope, "replacement")
        superseded = await lifecycle.supersede(
            scope,
            old.id,
            replacement.id,
            "reviewer",
            "replacement approved",
            expected_revision=old.revision,
        )

    hits = [
        IndexHit(superseded.id, superseded.revision, 0.99, PROFILE.profile_id),
        IndexHit(replacement.id, replacement.revision, 0.98, PROFILE.profile_id),
    ]
    replacement_ref = replacement.application.ref
    resolver = _Resolver({replacement_ref: ResolvedRecord(replacement_ref, "replacement content")})
    retrieval = _memory(
        database,
        index=_SearchIndex(hits),
        resolver=resolver,
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        results = await retrieval.retrieve(scope, "query", limit=2)

    assert [result.candidate_id for result in results] == [replacement.id]
    assert [ref for _, ref in resolver.calls] == [replacement_ref]


@pytest.mark.asyncio
async def test_expired_and_future_validity_windows_are_not_retrieved():
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "notes")
    lifecycle = _memory(database, clock=lambda: NOW)
    async with database.transaction():
        _, expired = await _accepted_applied(lifecycle, scope, "expired", valid_until=NOW)
        _, future = await _accepted_applied(
            lifecycle,
            scope,
            "future",
            valid_from=datetime(2026, 2, 2, tzinfo=timezone.utc),
        )
        _, current = await _accepted_applied(
            lifecycle,
            scope,
            "current",
            valid_from=datetime(2026, 2, 1, tzinfo=timezone.utc),
            valid_until=datetime(2026, 2, 2, tzinfo=timezone.utc),
        )

    hits = [
        IndexHit(expired.id, expired.revision, 0.99, PROFILE.profile_id),
        IndexHit(future.id, future.revision, 0.98, PROFILE.profile_id),
        IndexHit(current.id, current.revision, 0.97, PROFILE.profile_id),
    ]
    current_ref = current.application.ref
    resolver = _Resolver({current_ref: ResolvedRecord(current_ref, "currently valid")})
    retrieval = _memory(
        database,
        index=_SearchIndex(hits),
        resolver=resolver,
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        results = await retrieval.retrieve(scope, "query", limit=3)

    assert [result.candidate_id for result in results] == [current.id]
    assert [ref for _, ref in resolver.calls] == [current_ref]


@pytest.mark.asyncio
async def test_withdrawn_evidence_blocks_application_and_later_proposals():
    database = InMemoryDatabase()
    memory = _memory(database)
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        evidence, candidate = await _candidate(memory, scope)
        accepted = await memory.review(
            scope,
            candidate.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=candidate.revision,
        )
        await memory.withdraw_evidence(
            scope,
            evidence.id,
            "owner",
            "source revoked",
            expected_revision=evidence.revision,
        )
        with pytest.raises(InvalidTransition):
            await memory.record_application(
                scope,
                candidate.id,
                FormalRef("host", "Policy", "policy", "v1"),
                "host-actor",
                expected_revision=accepted.revision,
            )
        with pytest.raises(InvalidTransition):
            await memory.propose(
                scope,
                evidence.id,
                _proposal(),
                idempotency_key="after-withdrawal",
            )

        unchanged = await memory.get_candidate(scope, candidate.id)

    assert unchanged == accepted
    assert unchanged.application is None


@pytest.mark.asyncio
async def test_retrieval_caps_scans_and_never_returns_more_than_limit():
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "notes")
    lifecycle = _memory(database, clock=lambda: NOW)
    async with database.transaction():
        _, valid = await _accepted_applied(lifecycle, scope, "valid")

    hits = [IndexHit(f"missing-{number}", 1, 0.99, PROFILE.profile_id) for number in range(8)] + [
        IndexHit(valid.id, valid.revision, 0.5, PROFILE.profile_id)
    ]
    index = _SearchIndex(hits)
    ref = valid.application.ref
    resolver = _Resolver({ref: ResolvedRecord(ref, "valid formal content")})
    retrieval = _memory(
        database,
        index=index,
        resolver=resolver,
        profile=PROFILE,
        clock=lambda: NOW,
        scan_budget=3,
    )
    async with database.transaction():
        results = await retrieval.retrieve(scope, "query", limit=1)

    assert sum(call[2] for call in index.calls) <= 3
    assert sum(len(index.hits[call[3] : call[3] + call[2]]) for call in index.calls) <= 3
    assert len(results) <= 1
    assert results == ()
    assert resolver.calls == []


@pytest.mark.asyncio
async def test_foreign_namespace_index_id_is_not_resolved():
    database = InMemoryDatabase()
    owner_scope = Scope("tenant-a", "private")
    queried_scope = Scope("tenant-a", "public")
    lifecycle = _memory(database, clock=lambda: NOW)
    async with database.transaction():
        _, candidate = await _accepted_applied(lifecycle, owner_scope, "private")

    ref = candidate.application.ref
    index = _SearchIndex([IndexHit(candidate.id, candidate.revision, 1.0, PROFILE.profile_id)])
    resolver = _Resolver({ref: ResolvedRecord(ref, "private formal content")})
    retrieval = _memory(
        database,
        index=index,
        resolver=resolver,
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        results = await retrieval.retrieve(queried_scope, "query", limit=1)

    assert results == ()
    assert index.calls == [(queried_scope, "query", 2, 0)]
    assert resolver.calls == []


@pytest.mark.asyncio
async def test_retrieval_revalidates_stale_profile_and_role_denial_across_pages():
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    lifecycle = _memory(database, clock=lambda: NOW)
    async with database.transaction():
        _, allowed_one = await _accepted_applied(lifecycle, scope, "allowed-one")
        _, allowed_two = await _accepted_applied(lifecycle, scope, "allowed-two")
        _, denied = await _accepted_applied(lifecycle, scope, "denied")

    ref_one = allowed_one.application.ref
    ref_two = allowed_two.application.ref
    denied_ref = denied.application.ref
    hits = [
        IndexHit(allowed_one.id, allowed_one.revision - 1, 1.0, PROFILE.profile_id),
        IndexHit(allowed_two.id, allowed_two.revision, 0.99, "wrong-profile"),
        IndexHit(denied.id, denied.revision, 0.98, PROFILE.profile_id),
        IndexHit(allowed_one.id, allowed_one.revision, 0.97, PROFILE.profile_id),
        IndexHit(allowed_one.id, allowed_one.revision, 0.96, PROFILE.profile_id),
        IndexHit(allowed_two.id, allowed_two.revision, 0.95, PROFILE.profile_id),
    ]
    index = _SearchIndex(hits)
    resolver = _Resolver(
        {
            ref_one: ResolvedRecord(ref_one, "authorized one"),
            ref_two: ResolvedRecord(ref_two, "authorized two"),
            denied_ref: None,
        }
    )
    retrieval = _memory(
        database,
        index=index,
        resolver=resolver,
        profile=PROFILE,
        clock=lambda: NOW,
        scan_budget=8,
    )
    async with database.transaction():
        results = await retrieval.retrieve(scope, "query", limit=2)

    assert {result.candidate_id for result in results} == {allowed_one.id, allowed_two.id}
    assert len(results) == 2
    assert all(result.content != "denied content" for result in results)
    assert index.calls == [(scope, "query", 4, 0), (scope, "query", 4, 4)]
    assert [ref for _, ref in resolver.calls] == [denied_ref, ref_one, ref_two]


@pytest.mark.asyncio
async def test_sink_failure_and_host_rollback_remove_evidence_and_proposal():
    database = InMemoryDatabase()
    sink = InMemoryEventSink(database)
    memory = Memory(InMemoryRecordStore(database), sink)
    scope = Scope("tenant-a", "notes")
    evidence_id = candidate_id = None
    sink.failure = RuntimeError("outbox unavailable")

    with pytest.raises(RuntimeError, match="outbox unavailable"):
        async with database.transaction():
            evidence = await memory.record_evidence(
                scope,
                _source(),
                "rollback evidence",
                idempotency_key="rollback-evidence",
                captured_at=NOW,
            )
            candidate = await memory.propose(
                scope, evidence.id, _proposal(), idempotency_key="rollback-candidate"
            )
            evidence_id, candidate_id = evidence.id, candidate.id
            await memory.review(
                scope,
                candidate.id,
                GovernanceState.ACCEPTED,
                "reviewer",
                "approved",
                expected_revision=candidate.revision,
            )

    sink.failure = None
    assert evidence_id is not None and candidate_id is not None
    async with database.transaction():
        with pytest.raises(NotFound):
            await memory.get_evidence(scope, evidence_id)
        with pytest.raises(NotFound):
            await memory.get_candidate(scope, candidate_id)
    assert database.events == ()


@pytest.mark.asyncio
async def test_revision_cas_is_explicit_and_rejects_boolean_revisions():
    database = InMemoryDatabase()
    memory = _memory(database)
    scope = Scope("tenant-a", "notes")

    async with database.transaction():
        _, candidate = await _candidate(memory, scope)
        accepted = await memory.review(
            scope,
            candidate.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=1,
        )
        with pytest.raises(RevisionConflict):
            await memory.withdraw(
                scope,
                candidate.id,
                "reviewer",
                "stale write",
                expected_revision=1,
            )
        with pytest.raises(ValueError):
            await memory.withdraw(
                scope,
                candidate.id,
                "reviewer",
                "boolean is not a revision",
                expected_revision=True,
            )
        unchanged = await memory.get_candidate(scope, candidate.id)

    assert unchanged == accepted
    assert unchanged.revision == 2


@pytest.mark.asyncio
async def test_inmemory_access_requires_owner_task_transaction():
    database = InMemoryDatabase()
    memory = _memory(database)
    scope = Scope("tenant-a", "notes")

    with pytest.raises(RuntimeError, match="caller-owned transaction"):
        await memory.record_evidence(
            scope,
            _source(),
            "requires transaction",
            idempotency_key="without-transaction",
            captured_at=NOW,
        )

    async with database.transaction():
        evidence = await _evidence(memory, scope)
        child = asyncio.create_task(memory.get_evidence(scope, evidence.id))
        with pytest.raises(RuntimeError, match="cannot be shared across asyncio tasks"):
            await child
