"""Immutable, validated snapshots used by Aletora's lifecycle interface."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum, StrEnum


def _no_nul(name: str, value: str) -> None:
    if "\x00" in value:
        raise ValueError(f"{name} must not contain NUL bytes")


def _text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    _no_nul(name, value)


def _utc(name: str, value: datetime) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must be expressed in UTC")


def _revision(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("revision must be a positive integer")


def _enum(name: str, value: object, expected: type[Enum]) -> None:
    if not isinstance(value, expected):
        raise ValueError(f"{name} must be a {expected.__name__} value")


def _validity_window(valid_from: datetime | None, valid_until: datetime | None) -> None:
    if valid_from is not None:
        _utc("valid_from", valid_from)
    if valid_until is not None:
        _utc("valid_until", valid_until)
    if valid_from is not None and valid_until is not None and valid_from >= valid_until:
        raise ValueError("valid_from must precede valid_until")


class SourceKind(StrEnum):
    EXPLICIT_USER = "explicit_user"
    DOCUMENT = "document"
    TOOL = "tool"
    MODEL = "model"


class EpistemicKind(StrEnum):
    ASSERTED = "asserted"
    INFERRED = "inferred"
    QUESTION = "question"


class GovernanceState(StrEnum):
    PROPOSED = "proposed"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    WITHDRAWN = "withdrawn"
    SUPERSEDED = "superseded"


class EvidenceState(StrEnum):
    ACTIVE = "active"
    WITHDRAWN = "withdrawn"


@dataclass(frozen=True, slots=True)
class EvidenceWithdrawal:
    actor: str
    reason: str
    recorded_at: datetime

    def __post_init__(self) -> None:
        _text("actor", self.actor)
        _text("reason", self.reason)
        _utc("recorded_at", self.recorded_at)


@dataclass(frozen=True, slots=True)
class Scope:
    tenant_id: str
    namespace: str

    def __post_init__(self) -> None:
        _text("tenant_id", self.tenant_id)
        _text("namespace", self.namespace)


@dataclass(frozen=True, slots=True)
class SourceRef:
    """Provenance locator for recorded evidence.

    ``source_id``, ``locator``, and ``uri`` are unvalidated, untrusted strings from
    evidence ingestion. Host UIs rendering links must allowlist URI schemes
    (for example ``http:`` and ``https:``) and escape citation fields.
    """

    kind: SourceKind
    source_id: str
    version: str
    locator: str
    uri: str | None = None

    def __post_init__(self) -> None:
        _enum("kind", self.kind, SourceKind)
        for name in ("source_id", "version", "locator"):
            _text(name, getattr(self, name))
        if self.uri is not None:
            _text("uri", self.uri)


@dataclass(frozen=True, slots=True)
class Evidence:
    id: str
    scope: Scope
    source: SourceRef
    content: str
    content_hash: str
    captured_at: datetime
    revision: int = 1
    state: EvidenceState = EvidenceState.ACTIVE
    withdrawal: EvidenceWithdrawal | None = None

    def __post_init__(self) -> None:
        _text("id", self.id)
        if not isinstance(self.scope, Scope) or not isinstance(self.source, SourceRef):
            raise ValueError("scope and source must be typed values")
        if not isinstance(self.content, str):
            raise ValueError("content must be a string")
        _no_nul("content", self.content)
        if not isinstance(self.content_hash, str) or not re.fullmatch(
            r"[0-9a-f]{64}", self.content_hash
        ):
            raise ValueError("content_hash must be a lowercase SHA-256 hex digest")
        if hashlib.sha256(self.content.encode("utf-8")).hexdigest() != self.content_hash:
            raise ValueError("content_hash does not match content")
        _utc("captured_at", self.captured_at)
        _revision(self.revision)
        _enum("state", self.state, EvidenceState)
        if self.withdrawal is not None and not isinstance(self.withdrawal, EvidenceWithdrawal):
            raise ValueError("withdrawal must be an EvidenceWithdrawal")
        if (self.state is EvidenceState.WITHDRAWN) != (self.withdrawal is not None):
            raise ValueError("withdrawn evidence requires its audit entry")


@dataclass(frozen=True, slots=True)
class Proposal:
    text: str
    epistemic_kind: EpistemicKind
    proposer_id: str
    extractor_id: str
    run_id: str
    proposed_at: datetime
    valid_from: datetime | None = None
    valid_until: datetime | None = None

    def __post_init__(self) -> None:
        _text("text", self.text)
        _enum("epistemic_kind", self.epistemic_kind, EpistemicKind)
        for name in ("proposer_id", "extractor_id", "run_id"):
            _text(name, getattr(self, name))
        _utc("proposed_at", self.proposed_at)
        _validity_window(self.valid_from, self.valid_until)


@dataclass(frozen=True, slots=True)
class FormalRef:
    owner: str
    object_type: str
    object_id: str
    version: str

    def __post_init__(self) -> None:
        for name in ("owner", "object_type", "object_id", "version"):
            _text(name, getattr(self, name))


@dataclass(frozen=True, slots=True)
class ResolvedRecord:
    ref: FormalRef
    content: str

    def __post_init__(self) -> None:
        if not isinstance(self.ref, FormalRef):
            raise ValueError("ref must be a FormalRef")
        if not isinstance(self.content, str):
            raise ValueError("content must be a string")
        _no_nul("content", self.content)


@dataclass(frozen=True, slots=True)
class GovernanceEntry:
    state: GovernanceState
    actor: str
    reason: str
    recorded_at: datetime

    def __post_init__(self) -> None:
        _enum("state", self.state, GovernanceState)
        _text("actor", self.actor)
        _text("reason", self.reason)
        _utc("recorded_at", self.recorded_at)


@dataclass(frozen=True, slots=True)
class ApplicationReceipt:
    ref: FormalRef
    actor: str
    applied_at: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.ref, FormalRef):
            raise ValueError("ref must be a FormalRef")
        _text("actor", self.actor)
        _utc("applied_at", self.applied_at)


@dataclass(frozen=True, slots=True)
class Candidate:
    id: str
    scope: Scope
    evidence_id: str
    text: str
    epistemic_kind: EpistemicKind
    proposer_id: str
    extractor_id: str
    run_id: str
    proposed_at: datetime
    valid_from: datetime | None
    valid_until: datetime | None
    revision: int
    state: GovernanceState
    history: tuple[GovernanceEntry, ...]
    application: ApplicationReceipt | None = None
    replacement_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("id", "evidence_id", "text", "proposer_id", "extractor_id", "run_id"):
            _text(name, getattr(self, name))
        if not isinstance(self.scope, Scope):
            raise ValueError("scope must be a Scope")
        _enum("epistemic_kind", self.epistemic_kind, EpistemicKind)
        _enum("state", self.state, GovernanceState)
        _utc("proposed_at", self.proposed_at)
        _validity_window(self.valid_from, self.valid_until)
        _revision(self.revision)
        if not isinstance(self.history, tuple) or not all(
            isinstance(item, GovernanceEntry) for item in self.history
        ):
            raise ValueError("history must be a tuple of GovernanceEntry values")
        if self.application is not None and not isinstance(self.application, ApplicationReceipt):
            raise ValueError("application must be an ApplicationReceipt")
        if self.replacement_id is not None:
            _text("replacement_id", self.replacement_id)
            if self.replacement_id == self.id:
                raise ValueError("candidate cannot supersede itself")
        if (self.state is GovernanceState.SUPERSEDED) != (self.replacement_id is not None):
            raise ValueError("superseded candidates require a replacement_id and vice versa")
        if self.state is not GovernanceState.PROPOSED and not self.history:
            raise ValueError("non-proposed candidates require governance history")
        if self.application is not None and self.state not in (
            GovernanceState.ACCEPTED,
            GovernanceState.SUPERSEDED,
            GovernanceState.WITHDRAWN,
        ):
            raise ValueError("only accepted candidates may retain an application receipt")


@dataclass(frozen=True, slots=True)
class EmbeddingProfile:
    model: str
    dimension: int
    preprocessing_version: str
    chunking_version: str

    def __post_init__(self) -> None:
        for name in ("model", "preprocessing_version", "chunking_version"):
            _text(name, getattr(self, name))
        if (
            isinstance(self.dimension, bool)
            or not isinstance(self.dimension, int)
            or self.dimension < 1
        ):
            raise ValueError("dimension must be a positive integer")

    @property
    def profile_id(self) -> str:
        return json.dumps(
            [self.model, self.dimension, self.preprocessing_version, self.chunking_version],
            ensure_ascii=False,
            separators=(",", ":"),
        )


@dataclass(frozen=True, slots=True)
class Citation:
    evidence_id: str
    source: SourceRef
    content_hash: str

    def __post_init__(self) -> None:
        _text("evidence_id", self.evidence_id)
        if not isinstance(self.source, SourceRef):
            raise ValueError("source must be a SourceRef")
        if not isinstance(self.content_hash, str) or not re.fullmatch(
            r"[0-9a-f]{64}", self.content_hash
        ):
            raise ValueError("content_hash must be a lowercase SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class RetrievalHit:
    candidate_id: str
    revision: int
    ref: FormalRef
    content: str
    citations: tuple[Citation, ...]
    score: float
    epistemic_kind: EpistemicKind
    governance_state: GovernanceState
    profile_id: str

    def __post_init__(self) -> None:
        _text("candidate_id", self.candidate_id)
        _revision(self.revision)
        if not isinstance(self.ref, FormalRef):
            raise ValueError("ref must be a FormalRef")
        if not isinstance(self.content, str):
            raise ValueError("content must be a string")
        _no_nul("content", self.content)
        if not isinstance(self.citations, tuple) or not all(
            isinstance(c, Citation) for c in self.citations
        ):
            raise ValueError("citations must be a tuple of Citation values")
        _enum("epistemic_kind", self.epistemic_kind, EpistemicKind)
        _enum("governance_state", self.governance_state, GovernanceState)
        if self.governance_state is not GovernanceState.ACCEPTED:
            raise ValueError("retrieval hits must represent accepted candidates")
        _text("profile_id", self.profile_id)
        if (
            isinstance(self.score, bool)
            or not isinstance(self.score, (int, float))
            or not math.isfinite(self.score)
        ):
            raise ValueError("score must be finite")


@dataclass(frozen=True, slots=True)
class IndexHit:
    candidate_id: str
    revision: int
    score: float
    profile_id: str

    def __post_init__(self) -> None:
        _text("candidate_id", self.candidate_id)
        _revision(self.revision)
        if (
            isinstance(self.score, bool)
            or not isinstance(self.score, (int, float))
            or not math.isfinite(self.score)
        ):
            raise ValueError("score must be finite")
        _text("profile_id", self.profile_id)


@dataclass(frozen=True, slots=True)
class RetrievalStats:
    """Classify each yielded discovery once, as returned or skipped for one reason.

    Profile and revision mismatches take precedence over duplicates of returned
    candidates. ``budget`` also bounds raw points discarded by the index before
    yielding, which are excluded from ``hits_examined`` and the skip counters.
    """

    hits_examined: int
    duplicate_skipped: int
    profile_mismatch_skipped: int
    not_found_skipped: int
    stale_revision_skipped: int
    ineligible_state_skipped: int
    unapplied_skipped: int
    validity_window_skipped: int
    inactive_evidence_skipped: int
    resolver_denied_skipped: int
    returned: int
    budget: int
    limit_reached: bool

    def __post_init__(self) -> None:
        for name in (
            "hits_examined",
            "duplicate_skipped",
            "profile_mismatch_skipped",
            "not_found_skipped",
            "stale_revision_skipped",
            "ineligible_state_skipped",
            "unapplied_skipped",
            "validity_window_skipped",
            "inactive_evidence_skipped",
            "resolver_denied_skipped",
            "returned",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if isinstance(self.budget, bool) or not isinstance(self.budget, int) or self.budget < 1:
            raise ValueError("budget must be a positive integer")
        if not isinstance(self.limit_reached, bool):
            raise ValueError("limit_reached must be a boolean")


@dataclass(frozen=True, slots=True)
class RetrievalResult:
    """Authorized retrieval hits paired with bounded-scan diagnostic counters."""

    hits: tuple[RetrievalHit, ...]
    stats: RetrievalStats

    def __post_init__(self) -> None:
        if not isinstance(self.hits, tuple) or not all(
            isinstance(hit, RetrievalHit) for hit in self.hits
        ):
            raise ValueError("hits must be a tuple of RetrievalHit values")
        if not isinstance(self.stats, RetrievalStats):
            raise ValueError("stats must be a RetrievalStats")
