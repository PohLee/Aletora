from datetime import datetime, timezone

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
    SearchPage,
    SourceKind,
    SourceRef,
)
from aletora.adapters.memory import InMemoryDatabase, InMemoryEventSink, InMemoryRecordStore
from aletora.errors import CapabilityUnavailable, NotFound
from aletora.memory import Memory

NOW = datetime(2026, 2, 1, tzinfo=timezone.utc)
PROFILE = EmbeddingProfile("embed-v2", 4, "prep-v1", "chunk-v3")


class FakeIndex:
    def __init__(self, hits: list[IndexHit] | Exception) -> None:
        self.hits = hits
        self.calls: list[tuple[Scope, str, int, int]] = []

    async def search(self, scope: Scope, query: str, *, limit: int, offset: int):
        self.calls.append((scope, query, limit, offset))
        if isinstance(self.hits, Exception):
            raise self.hits
        page = self.hits[offset : offset + limit]
        return SearchPage(tuple(page), len(page), offset + limit >= len(self.hits))


class FakeResolver:
    def __init__(self, records: dict[FormalRef, ResolvedRecord | None] | Exception) -> None:
        self.records = records

    async def resolve(self, scope: Scope, ref: FormalRef):
        if isinstance(self.records, Exception):
            raise self.records
        return self.records.get(ref)


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
async def test_retrieval_returns_authorized_formal_content_and_provenance_only():
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    memory = Memory(InMemoryRecordStore(database), InMemoryEventSink(database), clock=lambda: NOW)
    async with database.transaction():
        candidate, evidence = await _accepted_applied(memory, scope, "one")

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
async def test_retrieval_excludes_unapplied_and_noncurrent_or_invalid_candidates():
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
            valid_from=datetime(2026, 2, 2, tzinfo=timezone.utc),
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
async def test_stale_and_unauthorized_first_page_hits_do_not_block_fresh_duplicate():
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
    assert len(index.calls) == 2
    assert [call[3] for call in index.calls] == [0, 2]


@pytest.mark.asyncio
async def test_withdrawn_evidence_and_cross_scope_ids_are_filtered():
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
async def test_missing_capabilities_and_adapter_errors_fail_explicitly():
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
async def test_resolver_version_mismatch_is_denied_and_resolver_errors_propagate():
    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: NOW)
    async with database.transaction():
        candidate, _ = await _accepted_applied(lifecycle, scope, "versioned")

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


@pytest.mark.asyncio
async def test_discarded_raw_points_do_not_end_pagination_early():
    """A page shortened by discarded raw points must not look like the index end."""

    class RawAccountingIndex:
        """Replays canned raw pages: discarded points still consume the scan budget."""

        def __init__(self, pages: list[SearchPage]) -> None:
            self._pages = list(pages)
            self.calls: list[tuple[Scope, str, int, int]] = []

        async def search(self, scope: Scope, query: str, *, limit: int, offset: int):
            self.calls.append((scope, query, limit, offset))
            return self._pages[len(self.calls) - 1]

    database = InMemoryDatabase()
    scope = Scope("tenant-a", "policies")
    store = InMemoryRecordStore(database)
    sink = InMemoryEventSink(database)
    lifecycle = Memory(store, sink, clock=lambda: NOW)
    async with database.transaction():
        safe, _ = await _accepted_applied(lifecycle, scope, "safe")

    safe_ref = safe.application.ref
    index = RawAccountingIndex(
        [
            SearchPage((), 4, False),
            SearchPage((IndexHit(safe.id, safe.revision, 0.9, PROFILE.profile_id),), 1, True),
        ]
    )
    memory = Memory(
        store,
        sink,
        index=index,
        resolver=FakeResolver({safe_ref: ResolvedRecord(safe_ref, "authorized content")}),
        profile=PROFILE,
        clock=lambda: NOW,
    )
    async with database.transaction():
        results = await memory.retrieve(scope, "query", limit=1)

    assert [result.candidate_id for result in results] == [safe.id]
    assert [call[3] for call in index.calls] == [0, 4]
