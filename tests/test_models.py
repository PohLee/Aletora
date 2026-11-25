import hashlib
import math
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from aletora.models import (
    Candidate,
    Citation,
    EmbeddingProfile,
    EpistemicKind,
    Evidence,
    EvidenceState,
    FormalRef,
    GovernanceEntry,
    GovernanceState,
    IndexHit,
    Proposal,
    ResolvedRecord,
    RetrievalHit,
    RetrievalResult,
    RetrievalStats,
    Scope,
    SourceKind,
    SourceRef,
)
from aletora.ports import IndexAction, IndexEvent


def test_scope_is_immutable_and_requires_both_parts() -> None:
    scope = Scope("tenant-1", "knowledge")

    assert (scope.tenant_id, scope.namespace) == ("tenant-1", "knowledge")
    with pytest.raises(FrozenInstanceError):
        scope.namespace = "other"  # type: ignore[misc]
    with pytest.raises(ValueError, match="non-empty"):
        Scope(" ", "knowledge")
    with pytest.raises(ValueError, match="NUL"):
        Scope("tenant\x00-1", "knowledge")


def test_proposal_requires_typed_epistemic_kind_and_utc_times() -> None:
    instant = datetime(2026, 1, 1, tzinfo=UTC)

    proposal = Proposal(
        "The sky is blue", EpistemicKind.ASSERTED, "user", "extractor", "run-1", instant
    )
    assert proposal.epistemic_kind is EpistemicKind.ASSERTED

    with pytest.raises(ValueError, match="EpistemicKind"):
        Proposal("Claim", "asserted", "user", "extractor", "run-1", instant)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="timezone-aware"):
        Proposal(
            "Claim", EpistemicKind.ASSERTED, "user", "extractor", "run-1", datetime(2026, 1, 1)
        )
    with pytest.raises(ValueError, match="NUL"):
        Proposal("Claim\x00", EpistemicKind.ASSERTED, "user", "extractor", "run-1", instant)


def test_evidence_keeps_a_matching_immutable_provenance_snapshot() -> None:
    content = "The source said this"
    evidence = Evidence(
        "evidence-1",
        Scope("tenant-1", "knowledge"),
        SourceRef(SourceKind.DOCUMENT, "doc-1", "v2", "page:4"),
        content,
        hashlib.sha256(content.encode()).hexdigest(),
        datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert evidence.state is EvidenceState.ACTIVE
    with pytest.raises(ValueError, match="content_hash"):
        Evidence(
            evidence.id,
            evidence.scope,
            evidence.source,
            content,
            "0" * 64,
            evidence.captured_at,
        )
    bad_content = "bad\x00content"
    with pytest.raises(ValueError, match="NUL"):
        Evidence(
            evidence.id,
            evidence.scope,
            evidence.source,
            bad_content,
            hashlib.sha256(bad_content.encode()).hexdigest(),
            evidence.captured_at,
        )
    with pytest.raises(ValueError, match="NUL"):
        SourceRef(SourceKind.DOCUMENT, "doc-1", "v2", "page:4", uri="https://x\x00")
    with pytest.raises(ValueError, match="NUL"):
        ResolvedRecord(FormalRef("host", "Policy", "p1", "v1"), "bad\x00content")


def test_candidate_enforces_supersession_and_history_invariants() -> None:
    instant = datetime(2026, 1, 1, tzinfo=UTC)
    scope = Scope("tenant-1", "knowledge")
    accepted_entry = GovernanceEntry(GovernanceState.ACCEPTED, "reviewer", "ok", instant)
    superseded_entry = GovernanceEntry(GovernanceState.SUPERSEDED, "reviewer", "old", instant)

    with pytest.raises(ValueError, match="replacement_id"):
        Candidate(
            id="cand-1",
            scope=scope,
            evidence_id="ev-1",
            text="claim",
            epistemic_kind=EpistemicKind.ASSERTED,
            proposer_id="author",
            extractor_id="ext",
            run_id="run-1",
            proposed_at=instant,
            valid_from=None,
            valid_until=None,
            revision=2,
            state=GovernanceState.SUPERSEDED,
            history=(accepted_entry, superseded_entry),
            replacement_id=None,
        )

    with pytest.raises(ValueError, match="replacement_id"):
        Candidate(
            id="cand-1",
            scope=scope,
            evidence_id="ev-1",
            text="claim",
            epistemic_kind=EpistemicKind.ASSERTED,
            proposer_id="author",
            extractor_id="ext",
            run_id="run-1",
            proposed_at=instant,
            valid_from=None,
            valid_until=None,
            revision=2,
            state=GovernanceState.ACCEPTED,
            history=(accepted_entry,),
            replacement_id="cand-2",
        )

    with pytest.raises(ValueError, match="supersede itself"):
        Candidate(
            id="cand-1",
            scope=scope,
            evidence_id="ev-1",
            text="claim",
            epistemic_kind=EpistemicKind.ASSERTED,
            proposer_id="author",
            extractor_id="ext",
            run_id="run-1",
            proposed_at=instant,
            valid_from=None,
            valid_until=None,
            revision=3,
            state=GovernanceState.SUPERSEDED,
            history=(accepted_entry, superseded_entry),
            replacement_id="cand-1",
        )

    with pytest.raises(ValueError, match="governance history"):
        Candidate(
            id="cand-1",
            scope=scope,
            evidence_id="ev-1",
            text="claim",
            epistemic_kind=EpistemicKind.ASSERTED,
            proposer_id="author",
            extractor_id="ext",
            run_id="run-1",
            proposed_at=instant,
            valid_from=None,
            valid_until=None,
            revision=2,
            state=GovernanceState.ACCEPTED,
            history=(),
        )


def test_embedding_profile_identity_includes_all_versioned_dimensions() -> None:
    first = EmbeddingProfile("model-v1", 3, "normalize-v1", "chunks-v1")
    second = EmbeddingProfile("model-v1", 3, "normalize-v2", "chunks-v1")

    assert first.profile_id != second.profile_id
    with pytest.raises(ValueError, match="finite"):
        IndexHit("candidate-1", 1, math.inf, first.profile_id)
    with pytest.raises(ValueError, match="NUL"):
        IndexHit("candidate\x00-1", 1, 0.5, first.profile_id)


def test_retrieval_hit_and_stats_validation() -> None:
    ref = FormalRef("host", "Policy", "policy-1", "v1")
    citation = Citation(
        "evidence-1",
        SourceRef(SourceKind.DOCUMENT, "doc-1", "v1", "page:1"),
        "a" * 64,
    )
    hit = RetrievalHit(
        "candidate-1",
        2,
        ref,
        "formal content",
        (citation,),
        0.75,
        EpistemicKind.INFERRED,
        GovernanceState.ACCEPTED,
        "profile-v1",
    )

    assert hit.score == 0.75
    assert hit.epistemic_kind is EpistemicKind.INFERRED
    assert hit.governance_state is GovernanceState.ACCEPTED
    assert hit.profile_id == "profile-v1"
    with pytest.raises(ValueError, match="EpistemicKind"):
        RetrievalHit(
            hit.candidate_id,
            hit.revision,
            hit.ref,
            hit.content,
            hit.citations,
            hit.score,
            "inferred",  # type: ignore[arg-type]
            hit.governance_state,
            hit.profile_id,
        )
    with pytest.raises(ValueError, match="NUL"):
        RetrievalHit(
            hit.candidate_id,
            hit.revision,
            hit.ref,
            "bad\x00content",
            hit.citations,
            hit.score,
            hit.epistemic_kind,
            hit.governance_state,
            hit.profile_id,
        )

    stats = RetrievalStats(
        hits_examined=1,
        duplicate_skipped=0,
        profile_mismatch_skipped=0,
        not_found_skipped=0,
        stale_revision_skipped=0,
        ineligible_state_skipped=0,
        unapplied_skipped=0,
        validity_window_skipped=0,
        inactive_evidence_skipped=0,
        resolver_denied_skipped=0,
        returned=1,
        budget=10,
        limit_reached=True,
    )
    result = RetrievalResult((hit,), stats)
    assert result.stats.returned == 1
    with pytest.raises(ValueError, match="non-negative"):
        RetrievalStats(
            hits_examined=True,
            duplicate_skipped=0,
            profile_mismatch_skipped=0,
            not_found_skipped=0,
            stale_revision_skipped=0,
            ineligible_state_skipped=0,
            unapplied_skipped=0,
            validity_window_skipped=0,
            inactive_evidence_skipped=0,
            resolver_denied_skipped=0,
            returned=0,
            budget=10,
            limit_reached=False,
        )
    with pytest.raises(ValueError, match="positive integer"):
        RetrievalStats(
            hits_examined=0,
            duplicate_skipped=0,
            profile_mismatch_skipped=0,
            not_found_skipped=0,
            stale_revision_skipped=0,
            ineligible_state_skipped=0,
            unapplied_skipped=0,
            validity_window_skipped=0,
            inactive_evidence_skipped=0,
            resolver_denied_skipped=0,
            returned=0,
            budget=0,
            limit_reached=False,
        )


def test_index_event_rejects_nul_bytes() -> None:
    scope = Scope("tenant-1", "knowledge")
    with pytest.raises(ValueError, match="NUL"):
        IndexEvent("evt\x001", scope, IndexAction.REFRESH_CANDIDATE, "candidate", "c1", 1)
    with pytest.raises(ValueError, match="NUL"):
        IndexEvent("evt-1", scope, IndexAction.REFRESH_CANDIDATE, "cand\x00", "c1", 1)
    with pytest.raises(ValueError, match="NUL"):
        IndexEvent("evt-1", scope, IndexAction.REFRESH_CANDIDATE, "candidate", "c\x001", 1)
