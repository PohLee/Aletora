import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Mapping, Sequence
from contextlib import aclosing, asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest

from aletora import (
    EmbeddingProfile,
    EpistemicKind,
    EvidenceState,
    FormalRef,
    GovernanceState,
    IndexHit,
    Proposal,
    ResolvedRecord,
    Scope,
    SourceKind,
    SourceRef,
)
from aletora.adapters.memory import InMemoryDatabase, InMemoryEventSink, InMemoryRecordStore
from aletora.errors import CapabilityUnavailable, NotFound
from aletora.memory import Memory
from aletora.ports import InsertResult, StoredRecord

NOW = datetime(2026, 2, 1, tzinfo=UTC)
PROFILE = EmbeddingProfile("embed-v2", 4, "prep-v1", "chunk-v3")


class FakeIndex:
    def __init__(self, hits: Sequence[object] | Exception) -> None:
        self.hits = hits
        self.calls: list[tuple[Scope, str, int]] = []
        self.consumed: list[object] = []
        self.closed = 0

    @asynccontextmanager
    async def scan(
        self, scope: Scope, query: str, *, budget: int
    ) -> AsyncIterator[AsyncIterator[Any]]:
        self.calls.append((scope, query, budget))
        if isinstance(self.hits, Exception):
            raise self.hits
        raw_hits = self.hits

        async def discover() -> AsyncGenerator[Any, None]:
            for hit in raw_hits[:budget]:
                self.consumed.append(hit)
                yield hit

        async with aclosing(discover()) as discovery:
            try:
                yield discovery
            finally:
                self.closed += 1


class FakeResolver:
    def __init__(self, records: dict[FormalRef, ResolvedRecord | None] | Exception) -> None:
        self.records = records

    async def resolve(self, scope: Scope, ref: FormalRef) -> ResolvedRecord | None:
        if isinstance(self.records, Exception):
            raise self.records
        return self.records.get(ref)


class CountingNonBatchStore:
    """Custom RecordStore without BatchRecordStore to test fallback and memoization."""

    def __init__(self, inner: InMemoryRecordStore) -> None:
        self._inner = inner
        self.get_calls: list[tuple[str, str]] = []

    async def get(
        self, scope: Scope, kind: str, record_id: str, *, for_update: bool = False
    ) -> StoredRecord | None:
        self.get_calls.append((kind, record_id))
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


class CountingBatchStore(CountingNonBatchStore):
    def __init__(self, inner: InMemoryRecordStore) -> None:
        super().__init__(inner)
        self.get_many_calls: list[tuple[str, tuple[str, ...]]] = []

    async def get_many(
        self, scope: Scope, kind: str, record_ids: Sequence[str]
    ) -> Mapping[str, StoredRecord]:
        self.get_many_calls.append((kind, tuple(record_ids)))
        return await self._inner.get_many(scope, kind, record_ids)


class CountingBatchResolver:
    def __init__(self, records: dict[FormalRef, ResolvedRecord | None]) -> None:
        self.records = records
        self.resolve_calls: list[FormalRef] = []
        self.resolve_many_calls: list[tuple[FormalRef, ...]] = []

    async def resolve(self, scope: Scope, ref: FormalRef) -> ResolvedRecord | None:
        self.resolve_calls.append(ref)
        return self.records.get(ref)

    async def resolve_many(
        self, scope: Scope, refs: Sequence[FormalRef]
    ) -> Mapping[FormalRef, ResolvedRecord | None]:
        self.resolve_many_calls.append(tuple(refs))
        return {ref: self.records.get(ref) for ref in refs}


async def _accepted_applied(
    memory: Memory,
    scope: Scope,
    key: str,
    *,
    ref_version: str = "v1",
    valid_from: datetime | None = None,
    valid_until: datetime | None = None,
):
    evidence = await memory.record_evidence(
        scope,
        SourceRef(SourceKind.DOCUMENT, f"doc-{key}", "source-v4", "section:2"),
        f"private evidence content {key}",
        idempotency_key=f"evidence-{key}",
        captured_at=NOW,
    )
    proposal = Proposal(
        f"candidate statement {key}",
        EpistemicKind.ASSERTED,
        f"author-{key}",
        "extractor-v1",
        f"run-{key}",
        NOW,
        valid_from,
        valid_until,
    )
    candidate = await memory.propose(
        scope, evidence.id, proposal, idempotency_key=f"proposal-{key}"
    )
    candidate = await memory.review(
        scope,
        candidate.id,
        GovernanceState.ACCEPTED,
        "independent-reviewer",
        "approved",
        expected_revision=candidate.revision,
    )
    ref = FormalRef("host", "Policy", f"formal-{key}", ref_version)
    candidate = await memory.record_application(
        scope, candidate.id, ref, "host-actor", expected_revision=candidate.revision
    )
    return candidate, evidence


async def _pending_candidate(memory: Memory, scope: Scope, key: str):
    evidence = await memory.record_evidence(
        scope,
        SourceRef(SourceKind.DOCUMENT, f"doc-{key}", "source-v4", "section:2"),
        f"private evidence content {key}",
        idempotency_key=f"evidence-{key}",
        captured_at=NOW,
    )
    proposal = Proposal(
        f"candidate statement {key}",
        EpistemicKind.ASSERTED,
        f"author-{key}",
        "extractor-v1",
        f"run-{key}",
        NOW,
    )
    return await memory.propose(scope, evidence.id, proposal, idempotency_key=f"proposal-{key}")


@pytest.mark.asyncio
async def test_retrieval_returns_authorized_formal_content_and_provenance_only() -> None:
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database), clock=lambda: NOW)
    async with database.transaction():
        candidate, evidence = await _accepted_applied(memory, scope, "one")

    assert candidate.application is not None
    ref = candidate.application.ref
    index = FakeIndex([IndexHit(candidate.id, candidate.revision, 0.875, PROFILE.profile_id)])
    resolver = FakeResolver({ref: ResolvedRecord(ref, "Current formal policy")})
    memory = Memory(
        InMemoryRecordStore(database),
        InMemoryEventSink(database),
        index=index,
        resolver=resolver,
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        results = await memory.retrieve(scope, "policy query", limit=1)

    assert len(results) == 1
    assert results[0].content == "Current formal policy"
    assert results[0].ref == ref
    assert results[0].score == 0.875
    assert results[0].epistemic_kind is EpistemicKind.ASSERTED
    assert results[0].governance_state is GovernanceState.ACCEPTED
    assert results[0].profile_id == PROFILE.profile_id
    assert results[0].citations[0].evidence_id == evidence.id
    assert results[0].citations[0].content_hash == evidence.content_hash
    assert results[0].citations[0].source.source_id == evidence.source.source_id
    assert "private evidence content" not in results[0].content


@pytest.mark.asyncio
async def test_retrieval_excludes_unapplied_and_noncurrent_or_invalid_candidates() -> None:
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: NOW)
    refs: dict[FormalRef, ResolvedRecord | None] = {}
    async with database.transaction():
        accepted_unapplied = await _pending_candidate(lifecycle, scope, "unapplied")
        accepted_unapplied = await lifecycle.review(
            scope,
            accepted_unapplied.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "approved",
            expected_revision=accepted_unapplied.revision,
        )
        rejected = await _pending_candidate(lifecycle, scope, "rejected")
        rejected = await lifecycle.review(
            scope,
            rejected.id,
            GovernanceState.REJECTED,
            "reviewer",
            "declined",
            expected_revision=rejected.revision,
        )
        proposed = await _pending_candidate(lifecycle, scope, "proposed")
        expired, _ = await _accepted_applied(lifecycle, scope, "expired", valid_until=NOW)
        future, _ = await _accepted_applied(
            lifecycle,
            scope,
            "future",
            valid_from=datetime(2026, 2, 2, tzinfo=UTC),
        )
        withdrawn_source, withdrawn_evidence = await _accepted_applied(
            lifecycle, scope, "withdrawn-source"
        )
        await lifecycle.withdraw_evidence(
            scope,
            withdrawn_evidence.id,
            "owner",
            "source retracted",
            expected_revision=withdrawn_evidence.revision,
        )
        withdrawn_candidate, _ = await _accepted_applied(lifecycle, scope, "withdrawn-candidate")
        withdrawn_candidate = await lifecycle.withdraw(
            scope,
            withdrawn_candidate.id,
            "reviewer",
            "no longer valid",
            expected_revision=withdrawn_candidate.revision,
        )
        superseded, _ = await _accepted_applied(lifecycle, scope, "superseded")
        replacement, _ = await _accepted_applied(lifecycle, scope, "replacement")
        superseded = await lifecycle.supersede(
            scope,
            superseded.id,
            replacement.id,
            "reviewer",
            "newer candidate",
            expected_revision=superseded.revision,
        )
        current, _ = await _accepted_applied(lifecycle, scope, "current")

        for candidate in (
            expired,
            future,
            withdrawn_source,
            withdrawn_candidate,
            superseded,
            replacement,
            current,
        ):
            assert candidate.application is not None
            ref = candidate.application.ref
            refs[ref] = ResolvedRecord(ref, f"formal content for {candidate.id}")

    candidates = (
        accepted_unapplied,
        rejected,
        proposed,
        expired,
        future,
        withdrawn_source,
        withdrawn_candidate,
        superseded,
        replacement,
        current,
    )
    index = FakeIndex(
        [
            IndexHit(candidate.id, candidate.revision, 0.9, PROFILE.profile_id)
            for candidate in candidates
        ]
    )
    memory = Memory(
        store,
        sink,
        index=index,
        resolver=FakeResolver(refs),
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        results = await memory.retrieve(scope, "query", limit=10)

    assert {result.candidate_id for result in results} == {replacement.id, current.id}


@pytest.mark.asyncio
async def test_retrieve_with_stats_and_memoization_and_batching() -> None:
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    inner_store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    lifecycle = Memory(inner_store, sink, clock=lambda: NOW)
    async with database.transaction():
        # Two distinct candidates sharing ONE evidence record.
        shared_ev = await lifecycle.record_evidence(
            scope,
            SourceRef(SourceKind.DOCUMENT, "shared-doc", "v1", "p:1"),
            "shared evidence body",
            idempotency_key="shared-ev",
            captured_at=NOW,
        )
        c1, c2 = await lifecycle.propose_many(
            scope,
            shared_ev.id,
            [
                (
                    Proposal("claim 1", EpistemicKind.ASSERTED, "a1", "ext", "run", NOW),
                    "shared-c1",
                ),
                (
                    Proposal("claim 2", EpistemicKind.ASSERTED, "a2", "ext", "run", NOW),
                    "shared-c2",
                ),
            ],
        )
        c1 = await lifecycle.review(
            scope, c1.id, GovernanceState.ACCEPTED, "rev", "ok", expected_revision=c1.revision
        )
        c2 = await lifecycle.review(
            scope, c2.id, GovernanceState.ACCEPTED, "rev", "ok", expected_revision=c2.revision
        )
        ref1 = FormalRef("host", "Policy", "shared-1", "v1")
        ref2 = FormalRef("host", "Policy", "shared-2", "v1")
        c1 = await lifecycle.record_application(
            scope, c1.id, ref1, "host", expected_revision=c1.revision
        )
        c2 = await lifecycle.record_application(
            scope, c2.id, ref2, "host", expected_revision=c2.revision
        )
        unapplied = await _pending_candidate(lifecycle, scope, "unapplied")
        unapplied = await lifecycle.review(
            scope,
            unapplied.id,
            GovernanceState.ACCEPTED,
            "rev",
            "ok",
            expected_revision=unapplied.revision,
        )
        proposed = await _pending_candidate(lifecycle, scope, "proposed")
        expired, _ = await _accepted_applied(lifecycle, scope, "expired", valid_until=NOW)
        revoked_cand, revoked_ev = await _accepted_applied(lifecycle, scope, "revoked")
        await lifecycle.withdraw_evidence(
            scope, revoked_ev.id, "owner", "retracted", expected_revision=revoked_ev.revision
        )
        denied, _ = await _accepted_applied(lifecycle, scope, "denied")

    assert denied.application is not None
    assert expired.application is not None
    assert revoked_cand.application is not None
    hits = [
        IndexHit("missing-id", 1, 0.99, PROFILE.profile_id),
        IndexHit(c1.id, c1.revision, 0.98, "wrong-profile"),
        IndexHit(c1.id, c1.revision - 1, 0.97, PROFILE.profile_id),  # stale revision
        IndexHit(c1.id, c1.revision - 2, 0.96, PROFILE.profile_id),  # duplicate stale candidate
        IndexHit(proposed.id, proposed.revision, 0.95, PROFILE.profile_id),  # ineligible state
        IndexHit(unapplied.id, unapplied.revision, 0.94, PROFILE.profile_id),  # unapplied
        IndexHit(expired.id, expired.revision, 0.93, PROFILE.profile_id),  # validity window
        IndexHit(revoked_cand.id, revoked_cand.revision, 0.92, PROFILE.profile_id),  # inactive ev
        IndexHit(denied.id, denied.revision, 0.91, PROFILE.profile_id),  # resolver denied
        IndexHit(c1.id, c1.revision, 0.90, PROFILE.profile_id),  # valid 1
        IndexHit(c1.id, c1.revision - 1, 0.89, PROFILE.profile_id),  # stale after valid 1
        IndexHit(c1.id, c1.revision, 0.88, "wrong-profile"),  # profile mismatch after valid 1
        IndexHit(c1.id, c1.revision, 0.87, PROFILE.profile_id),  # duplicate of valid 1
        IndexHit(expired.id, expired.revision, 0.86, PROFILE.profile_id),  # still expired
        IndexHit(c2.id, c2.revision, 0.85, PROFILE.profile_id),  # valid 2 (shares evidence)
        IndexHit("never-consumed", 1, 0.1, PROFILE.profile_id),
    ]
    records: dict[FormalRef, ResolvedRecord | None] = {
        ref1: ResolvedRecord(ref1, "formal 1"),
        ref2: ResolvedRecord(ref2, "formal 2"),
        denied.application.ref: None,
    }

    # 1) Batch store + Batch resolver
    batch_store = CountingBatchStore(inner_store)
    batch_resolver = CountingBatchResolver(records)
    batch_index = FakeIndex(hits)
    batch_memory = Memory(
        batch_store,
        sink,
        index=batch_index,
        resolver=batch_resolver,
        profile=PROFILE,
        clock=lambda: NOW,
        scan_budget=50,
    )
    async with database.transaction():
        result = await batch_memory.retrieve_with_stats(scope, "query", limit=2)

    assert [hit.candidate_id for hit in result.hits] == [c1.id, c2.id]
    assert result.stats.hits_examined == 15
    assert result.stats.duplicate_skipped == 1
    assert result.stats.profile_mismatch_skipped == 2
    assert result.stats.not_found_skipped == 1
    assert result.stats.stale_revision_skipped == 3
    assert result.stats.ineligible_state_skipped == 1
    assert result.stats.unapplied_skipped == 1
    assert result.stats.validity_window_skipped == 2
    assert result.stats.inactive_evidence_skipped == 1
    assert result.stats.resolver_denied_skipped == 1
    assert result.stats.returned == 2
    assert result.stats.budget == 50
    assert result.stats.limit_reached is True
    assert batch_index.consumed == hits[:-1]
    assert batch_store.get_calls == []  # used get_many exclusively
    assert batch_resolver.resolve_calls == []  # used resolve_many exclusively
    # Shared evidence and repeated candidate_id were only queried once!
    queried_evidence_ids = [
        eid for kind, ids in batch_store.get_many_calls if kind == "evidence" for eid in ids
    ]
    assert queried_evidence_ids.count(shared_ev.id) == 1
    queried_candidate_ids = [
        cid for kind, ids in batch_store.get_many_calls if kind == "candidate" for cid in ids
    ]
    assert queried_candidate_ids.count(c1.id) == 1

    # 2) Non-batch store + Non-batch resolver still memoizes within one retrieve scan
    seq_store = CountingNonBatchStore(inner_store)
    seq_resolver = FakeResolver(records)
    seq_memory = Memory(
        seq_store,
        sink,
        index=FakeIndex(hits),
        resolver=seq_resolver,
        profile=PROFILE,
        clock=lambda: NOW,
        scan_budget=50,
    )
    async with database.transaction():
        seq_result = await seq_memory.retrieve_with_stats(scope, "query", limit=2)
    assert seq_result == result
    assert [eid for kind, eid in seq_store.get_calls if kind == "evidence"].count(shared_ev.id) == 1
    assert [cid for kind, cid in seq_store.get_calls if kind == "candidate"].count(c1.id) == 1


@pytest.mark.asyncio
async def test_stale_and_unauthorized_first_page_hits_do_not_block_fresh_duplicate() -> None:
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: NOW)
    async with database.transaction():
        safe, _ = await _accepted_applied(lifecycle, scope, "safe")
        denied, _ = await _accepted_applied(lifecycle, scope, "denied")

    wrong_profile = "not-the-active-profile"
    hits = [
        IndexHit(safe.id, safe.revision - 1, 0.99, PROFILE.profile_id),
        IndexHit(safe.id, safe.revision, 0.98, wrong_profile),
        IndexHit(denied.id, denied.revision, 0.97, PROFILE.profile_id),
        IndexHit(safe.id, safe.revision, 0.95, PROFILE.profile_id),
    ]
    assert safe.application is not None
    assert denied.application is not None
    safe_ref = safe.application.ref
    denied_ref = denied.application.ref
    index = FakeIndex(hits)
    resolver = FakeResolver(
        {
            safe_ref: ResolvedRecord(safe_ref, "authorized content"),
            denied_ref: None,
        }
    )
    memory = Memory(
        store,
        sink,
        index=index,
        resolver=resolver,
        profile=PROFILE,
        clock=lambda: NOW,
        scan_budget=8,
    )
    async with database.transaction():
        results = await memory.retrieve(scope, "query", limit=1)

    assert [result.candidate_id for result in results] == [safe.id]
    assert index.calls == [(scope, "query", 8)]
    assert len(index.consumed) == 4
    assert index.closed == 1


@pytest.mark.asyncio
async def test_withdrawn_evidence_and_cross_scope_ids_are_filtered() -> None:
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: NOW)
    async with database.transaction():
        revoked, revoked_evidence = await _accepted_applied(lifecycle, scope, "revoked")
        safe, _ = await _accepted_applied(lifecycle, scope, "remaining")
        await lifecycle.withdraw_evidence(
            scope, revoked_evidence.id, "owner", "source withdrawn", expected_revision=1
        )
        withdrawn = await lifecycle.get_evidence(scope, revoked_evidence.id)
        assert withdrawn.state is EvidenceState.WITHDRAWN

    assert safe.application is not None
    safe_ref = safe.application.ref
    index = FakeIndex(
        [
            IndexHit(revoked.id, revoked.revision, 0.99, PROFILE.profile_id),
            IndexHit(safe.id, safe.revision, 0.98, PROFILE.profile_id),
        ]
    )
    memory = Memory(
        store,
        sink,
        index=index,
        resolver=FakeResolver({safe_ref: ResolvedRecord(safe_ref, "current content")}),
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        results = await memory.retrieve(scope, "query", limit=2)
        with pytest.raises(NotFound):
            await memory.get_candidate(Scope("tenant-b", "policies"), safe.id)
    assert [result.candidate_id for result in results] == [safe.id]


@pytest.mark.asyncio
async def test_missing_capabilities_and_adapter_errors_fail_explicitly() -> None:
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    async with database.transaction():
        await Memory(store, sink).record_evidence(
            scope,
            SourceRef(SourceKind.TOOL, "tool", "1", "result"),
            "evidence",
            idempotency_key="evidence",
            captured_at=NOW,
        )
        with pytest.raises(CapabilityUnavailable):
            await Memory(store, sink).retrieve(scope, "query", limit=1)

    failing = Memory(
        store,
        sink,
        index=FakeIndex(RuntimeError("index offline")),
        resolver=FakeResolver({}),
        profile=PROFILE,
        clock=lambda: NOW,
    )
    with pytest.raises(RuntimeError, match="index offline"):
        async with database.transaction():
            await failing.retrieve(scope, "query", limit=1)


@pytest.mark.asyncio
async def test_resolver_version_mismatch_is_denied_and_resolver_errors_propagate() -> None:
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: NOW)
    async with database.transaction():
        candidate, _ = await _accepted_applied(lifecycle, scope, "versioned")

    assert candidate.application is not None
    expected_ref = candidate.application.ref
    stale_ref = FormalRef(
        expected_ref.owner,
        expected_ref.object_type,
        expected_ref.object_id,
        "old-version",
    )
    index = FakeIndex([IndexHit(candidate.id, candidate.revision, 1.0, PROFILE.profile_id)])
    mismatch = Memory(
        store,
        sink,
        index=index,
        resolver=FakeResolver({expected_ref: ResolvedRecord(stale_ref, "stale content")}),
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        assert await mismatch.retrieve(scope, "query", limit=1) == ()

    failing_resolver = Memory(
        store,
        sink,
        index=index,
        resolver=FakeResolver(RuntimeError("resolver unavailable")),
        profile=PROFILE,
        clock=lambda: NOW,
    )
    with pytest.raises(RuntimeError, match="resolver unavailable"):
        async with database.transaction():
            await failing_resolver.retrieve(scope, "query", limit=1)
    assert index.closed == 2


@pytest.mark.asyncio
async def test_retrieval_stops_consuming_and_closes_discovery_after_success() -> None:
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    store, sink = InMemoryRecordStore(database), InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: NOW)
    async with database.transaction():
        safe, _ = await _accepted_applied(lifecycle, scope, "safe")
    assert safe.application is not None
    ref = safe.application.ref
    index = FakeIndex(
        [
            IndexHit(safe.id, safe.revision, 0.9, PROFILE.profile_id),
            IndexHit("never-consumed", 1, 0.8, PROFILE.profile_id),
        ]
    )
    memory = Memory(
        store,
        sink,
        index=index,
        resolver=FakeResolver({ref: ResolvedRecord(ref, "authorized content")}),
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        results = await memory.retrieve(scope, "query", limit=1)
    assert [result.candidate_id for result in results] == [safe.id]
    assert len(index.consumed) == 1
    assert index.closed == 1


@pytest.mark.asyncio
async def test_retrieval_rejects_invalid_discovery_hits_and_closes_context() -> None:
    database = InMemoryDatabase()
    index = FakeIndex([object()])
    memory = Memory(
        InMemoryRecordStore(database),
        InMemoryEventSink(database),
        index=index,
        resolver=FakeResolver({}),
        profile=PROFILE,
        clock=lambda: NOW,
    )
    with pytest.raises(TypeError, match="IndexHit"):
        async with database.transaction():
            await memory.retrieve(Scope("tenant-a", "main"), "query", limit=1)
    assert index.closed == 1


@pytest.mark.asyncio
async def test_cancellation_during_resolution_closes_discovery() -> None:
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    store, sink = InMemoryRecordStore(database), InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: NOW)
    async with database.transaction():
        safe, _ = await _accepted_applied(lifecycle, scope, "safe")
    index = FakeIndex([IndexHit(safe.id, safe.revision, 0.9, PROFILE.profile_id)])
    resolving = asyncio.Event()

    class WaitingResolver:
        async def resolve(self, request_scope: Scope, ref: FormalRef) -> ResolvedRecord | None:
            resolving.set()
            await asyncio.Event().wait()
            return None

    memory = Memory(
        store,
        sink,
        index=index,
        resolver=WaitingResolver(),
        profile=PROFILE,
        clock=lambda: NOW,
    )

    async def retrieve() -> None:
        async with database.transaction():
            await memory.retrieve(scope, "query", limit=1)

    task = asyncio.create_task(retrieve())
    try:
        async with asyncio.timeout(10):
            await resolving.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert index.closed == 1
