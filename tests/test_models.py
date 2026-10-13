import hashlib
import math
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

from aletora.models import (
    Citation,
    EmbeddingProfile,
    EpistemicKind,
    Evidence,
    EvidenceState,
    FormalRef,
    GovernanceState,
    IndexHit,
    Proposal,
    RetrievalHit,
    Scope,
    SourceKind,
    SourceRef,
)


def test_scope_is_immutable_and_requires_both_parts():
    scope = Scope("tenant-1", "knowledge")

    assert (scope.tenant_id, scope.namespace) == ("tenant-1", "knowledge")
    with pytest.raises(FrozenInstanceError):
        scope.namespace = "other"
    with pytest.raises(ValueError):
        Scope(" ", "knowledge")


def test_proposal_requires_typed_epistemic_kind_and_utc_times():
    instant = datetime(2026, 1, 1, tzinfo=timezone.utc)

    proposal = Proposal(
        "The sky is blue", EpistemicKind.ASSERTED, "user", "extractor", "run-1", instant
    )
    assert proposal.epistemic_kind is EpistemicKind.ASSERTED

    with pytest.raises(ValueError):
        Proposal("Claim", "asserted", "user", "extractor", "run-1", instant)
    with pytest.raises(ValueError):
        Proposal(
            "Claim", EpistemicKind.ASSERTED, "user", "extractor", "run-1", datetime(2026, 1, 1)
        )


def test_evidence_keeps_a_matching_immutable_provenance_snapshot():
    content = "The source said this"
    evidence = Evidence(
        "evidence-1",
        Scope("tenant-1", "knowledge"),
        SourceRef(SourceKind.DOCUMENT, "doc-1", "v2", "page:4"),
        content,
        hashlib.sha256(content.encode()).hexdigest(),
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert evidence.state is EvidenceState.ACTIVE
    with pytest.raises(ValueError):
        Evidence(
            evidence.id,
            evidence.scope,
            evidence.source,
            content,
            "0" * 64,
            evidence.captured_at,
        )


def test_embedding_profile_identity_includes_all_versioned_dimensions():
    first = EmbeddingProfile("model-v1", 3, "normalize-v1", "chunks-v1")
    second = EmbeddingProfile("model-v1", 3, "normalize-v2", "chunks-v1")

    assert first.profile_id != second.profile_id
    with pytest.raises(ValueError):
        IndexHit("candidate-1", 1, math.inf, first.profile_id)


def test_retrieval_hit_exposes_typed_explanation_metadata_separately_from_score():
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
    with pytest.raises(ValueError):
        RetrievalHit(
            hit.candidate_id,
            hit.revision,
            hit.ref,
            hit.content,
            hit.citations,
            hit.score,
            "inferred",
            hit.governance_state,
            hit.profile_id,
        )
