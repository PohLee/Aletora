"""Aletora's public lifecycle and fail-closed retrieval interface."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Callable

from . import _codec
from .errors import (
    CapabilityUnavailable,
    InvalidTransition,
    NotFound,
    RevisionConflict,
)
from .models import (
    ApplicationReceipt,
    Candidate,
    Citation,
    EmbeddingProfile,
    Evidence,
    EvidenceState,
    EvidenceWithdrawal,
    FormalRef,
    GovernanceEntry,
    GovernanceState,
    IndexHit,
    Proposal,
    RetrievalHit,
    Scope,
    SourceRef,
)
from .ports import EventSink, IndexAction, IndexEvent, RecordResolver, RecordStore, SearchIndex

_EVIDENCE = "evidence"
_CANDIDATE = "candidate"
_MAX_RETURN_LIMIT = 100


class Memory:
    """Lifecycle operations over host-owned persistence and outbox transactions."""

    def __init__(
        self,
        store: RecordStore,
        event_sink: EventSink,
        *,
        index: SearchIndex | None = None,
        resolver: RecordResolver | None = None,
        profile: EmbeddingProfile | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        scan_budget: int = 200,
    ) -> None:
        if isinstance(scan_budget, bool) or not isinstance(scan_budget, int) or scan_budget < 1:
            raise ValueError("scan_budget must be a positive integer")
        if not callable(clock):
            raise ValueError("clock must be callable")
        self._store = store
        self._event_sink = event_sink
        self._index = index
        self._resolver = resolver
        self._profile = profile
        self._clock = clock
        self._scan_budget = scan_budget

    async def record_evidence(
        self,
        scope: Scope,
        source: SourceRef,
        content: str,
        *,
        idempotency_key: str,
        captured_at: datetime | None = None,
    ) -> Evidence:
        self._require_scope(scope)
        self._require_key(idempotency_key)
        if not isinstance(source, SourceRef):
            raise ValueError("source must be a SourceRef")
        if not isinstance(content, str):
            raise ValueError("content must be a string")
        captured = captured_at if captured_at is not None else self._now()
        evidence_id = _codec.stable_id(scope, _EVIDENCE, idempotency_key)
        content_hash = _codec.hash_content(content)
        evidence = Evidence(evidence_id, scope, source, content, content_hash, captured)
        semantic = {
            "source": _codec.source_payload(source),
            "content": content,
        }
        result = await self._store.insert(
            scope,
            _EVIDENCE,
            evidence_id,
            idempotency_key,
            _codec.fingerprint(semantic),
            _codec.encode_evidence(evidence),
        )
        return self._decode_evidence(
            scope, result.record.id, result.record.revision, result.record.payload
        )

    async def get_evidence(self, scope: Scope, evidence_id: str) -> Evidence:
        self._require_scope(scope)
        return await self._load_evidence(scope, evidence_id)

    async def propose(
        self,
        scope: Scope,
        evidence_id: str,
        proposal: Proposal,
        *,
        idempotency_key: str,
    ) -> Candidate:
        self._require_scope(scope)
        self._require_key(idempotency_key)
        if not isinstance(proposal, Proposal):
            raise ValueError("proposal must be a Proposal")
        candidate_id = _codec.stable_id(scope, _CANDIDATE, idempotency_key)
        semantic = {"evidence_id": evidence_id, "proposal": _codec.proposal_payload(proposal)}
        candidate = Candidate(
            id=candidate_id,
            scope=scope,
            evidence_id=evidence_id,
            text=proposal.text,
            epistemic_kind=proposal.epistemic_kind,
            proposer_id=proposal.proposer_id,
            extractor_id=proposal.extractor_id,
            run_id=proposal.run_id,
            proposed_at=proposal.proposed_at,
            valid_from=proposal.valid_from,
            valid_until=proposal.valid_until,
            revision=1,
            state=GovernanceState.PROPOSED,
            history=(),
        )
        existing = await self._store.get(scope, _CANDIDATE, candidate_id)
        if existing is None:
            evidence = await self._load_evidence(scope, evidence_id)
            self._require_active_evidence(evidence)
        result = await self._store.insert(
            scope,
            _CANDIDATE,
            candidate_id,
            idempotency_key,
            _codec.fingerprint(semantic),
            _codec.encode_candidate(candidate),
        )
        return self._decode_candidate(
            scope, result.record.id, result.record.revision, result.record.payload
        )

    async def get_candidate(self, scope: Scope, candidate_id: str) -> Candidate:
        self._require_scope(scope)
        return await self._load_candidate(scope, candidate_id)

    async def review(
        self,
        scope: Scope,
        candidate_id: str,
        decision: GovernanceState,
        actor: str,
        reason: str,
        *,
        expected_revision: int,
    ) -> Candidate:
        candidate = await self._candidate_for_mutation(scope, candidate_id, expected_revision)
        if decision not in (GovernanceState.ACCEPTED, GovernanceState.REJECTED):
            raise InvalidTransition("review decision must be ACCEPTED or REJECTED")
        if not isinstance(decision, GovernanceState):
            raise ValueError("decision must be a GovernanceState")
        self._require_actor_reason(actor, reason)
        if candidate.state is not GovernanceState.PROPOSED:
            raise InvalidTransition("only proposed candidates may be reviewed")
        if actor == candidate.proposer_id:
            raise InvalidTransition("a proposer cannot review their own candidate")
        evidence = await self._load_evidence(scope, candidate.evidence_id)
        self._require_active_evidence(evidence)
        entry = GovernanceEntry(decision, actor, reason, self._now())
        updated = replace(candidate, state=decision, history=candidate.history + (entry,))
        action = (
            IndexAction.REFRESH_CANDIDATE
            if decision is GovernanceState.ACCEPTED
            else IndexAction.DELETE_CANDIDATE
        )
        return await self._save_candidate(candidate, updated, expected_revision, action)

    async def record_application(
        self,
        scope: Scope,
        candidate_id: str,
        ref: FormalRef,
        actor: str,
        *,
        expected_revision: int,
    ) -> Candidate:
        candidate = await self._candidate_for_mutation(scope, candidate_id, expected_revision)
        if not isinstance(ref, FormalRef):
            raise ValueError("ref must be a FormalRef")
        self._require_actor_reason(actor, "application")
        if candidate.state is not GovernanceState.ACCEPTED:
            raise InvalidTransition("only an accepted candidate may be applied")
        evidence = await self._load_evidence(scope, candidate.evidence_id)
        self._require_active_evidence(evidence)
        if candidate.application is not None:
            if candidate.application.ref == ref:
                return candidate
            raise InvalidTransition("an application receipt cannot be reassigned")
        receipt = ApplicationReceipt(ref, actor, self._now())
        updated = replace(candidate, application=receipt)
        return await self._save_candidate(
            candidate, updated, expected_revision, IndexAction.REFRESH_CANDIDATE
        )

    async def withdraw(
        self,
        scope: Scope,
        candidate_id: str,
        actor: str,
        reason: str,
        *,
        expected_revision: int,
    ) -> Candidate:
        candidate = await self._candidate_for_mutation(scope, candidate_id, expected_revision)
        self._require_actor_reason(actor, reason)
        if candidate.state in (GovernanceState.WITHDRAWN, GovernanceState.SUPERSEDED):
            raise InvalidTransition("candidate is already terminal")
        entry = GovernanceEntry(GovernanceState.WITHDRAWN, actor, reason, self._now())
        updated = replace(
            candidate,
            state=GovernanceState.WITHDRAWN,
            history=candidate.history + (entry,),
        )
        return await self._save_candidate(
            candidate, updated, expected_revision, IndexAction.DELETE_CANDIDATE
        )

    async def supersede(
        self,
        scope: Scope,
        candidate_id: str,
        replacement_id: str,
        actor: str,
        reason: str,
        *,
        expected_revision: int,
    ) -> Candidate:
        candidate = await self._candidate_for_mutation(scope, candidate_id, expected_revision)
        self._require_actor_reason(actor, reason)
        if candidate_id == replacement_id:
            raise InvalidTransition("a candidate cannot supersede itself")
        if candidate.state is not GovernanceState.ACCEPTED:
            raise InvalidTransition("only accepted candidates may be superseded")
        replacement = await self._load_candidate(scope, replacement_id)
        if replacement.state is not GovernanceState.ACCEPTED:
            raise InvalidTransition("replacement candidate must be accepted")
        replacement_evidence = await self._load_evidence(scope, replacement.evidence_id)
        self._require_active_evidence(replacement_evidence)
        entry = GovernanceEntry(GovernanceState.SUPERSEDED, actor, reason, self._now())
        updated = replace(
            candidate,
            state=GovernanceState.SUPERSEDED,
            history=candidate.history + (entry,),
            replacement_id=replacement.id,
        )
        return await self._save_candidate(
            candidate, updated, expected_revision, IndexAction.DELETE_CANDIDATE
        )

    async def withdraw_evidence(
        self,
        scope: Scope,
        evidence_id: str,
        actor: str,
        reason: str,
        *,
        expected_revision: int,
    ) -> Evidence:
        self._require_scope(scope)
        self._require_revision(expected_revision)
        self._require_actor_reason(actor, reason)
        evidence = await self._load_evidence(scope, evidence_id)
        self._check_revision(evidence.revision, expected_revision, _EVIDENCE, evidence_id)
        if evidence.state is EvidenceState.WITHDRAWN:
            raise InvalidTransition("evidence is already withdrawn")
        withdrawal = EvidenceWithdrawal(actor, reason, self._now())
        updated = replace(
            evidence,
            revision=evidence.revision + 1,
            state=EvidenceState.WITHDRAWN,
            withdrawal=withdrawal,
        )
        stored = await self._store.update(
            scope, _EVIDENCE, evidence_id, expected_revision, _codec.encode_evidence(updated)
        )
        result = self._decode_evidence(scope, stored.id, stored.revision, stored.payload)
        await self._append_event(
            scope, IndexAction.INVALIDATE_EVIDENCE, _EVIDENCE, evidence_id, result.revision
        )
        return result

    async def retrieve(self, scope: Scope, query: str, *, limit: int) -> tuple[RetrievalHit, ...]:
        self._require_scope(scope)
        if not isinstance(query, str) or not query.strip():
            raise ValueError("query must be a non-empty string")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= _MAX_RETURN_LIMIT
        ):
            raise ValueError(f"limit must be an integer from 1 to {_MAX_RETURN_LIMIT}")
        if self._index is None:
            raise CapabilityUnavailable("retrieval requires a configured SearchIndex")
        if self._resolver is None:
            raise CapabilityUnavailable("retrieval requires a configured RecordResolver")
        if self._profile is None:
            raise CapabilityUnavailable("retrieval requires a configured EmbeddingProfile")

        hits: list[RetrievalHit] = []
        returned_ids: set[str] = set()
        offset = 0
        scanned = 0
        now = self._now()
        while scanned < self._scan_budget and len(hits) < limit:
            page_size = min(max(limit * 2, 1), self._scan_budget - scanned)
            page = await self._index.search(scope, query, limit=page_size, offset=offset)
            batch = tuple(page)[:page_size]
            if not batch:
                break
            scanned += len(batch)
            offset += page_size
            for hit in batch:
                if not isinstance(hit, IndexHit):
                    raise TypeError("SearchIndex.search must return IndexHit values")
                if hit.candidate_id in returned_ids or hit.profile_id != self._profile.profile_id:
                    continue
                try:
                    candidate = await self._load_candidate(scope, hit.candidate_id)
                except NotFound:
                    continue
                if (
                    candidate.revision != hit.revision
                    or candidate.state is not GovernanceState.ACCEPTED
                ):
                    continue
                if candidate.application is None:
                    continue
                if candidate.valid_from is not None and now < candidate.valid_from:
                    continue
                if candidate.valid_until is not None and now >= candidate.valid_until:
                    continue
                try:
                    evidence = await self._load_evidence(scope, candidate.evidence_id)
                except NotFound:
                    continue
                if evidence.state is not EvidenceState.ACTIVE:
                    continue
                resolved = await self._resolver.resolve(scope, candidate.application.ref)
                if resolved is None or resolved.ref != candidate.application.ref:
                    continue
                citation = Citation(evidence.id, evidence.source, evidence.content_hash)
                hits.append(
                    RetrievalHit(
                        candidate.id,
                        candidate.revision,
                        candidate.application.ref,
                        resolved.content,
                        (citation,),
                        hit.score,
                        candidate.epistemic_kind,
                        candidate.state,
                        self._profile.profile_id,
                    )
                )
                returned_ids.add(candidate.id)
                if len(hits) == limit:
                    break
            if len(batch) < page_size:
                break
        return tuple(hits)

    async def _load_evidence(self, scope: Scope, evidence_id: str) -> Evidence:
        self._require_key(evidence_id)
        stored = await self._store.get(scope, _EVIDENCE, evidence_id)
        if stored is None:
            raise NotFound(f"evidence {evidence_id!r} was not found")
        return self._decode_evidence(scope, stored.id, stored.revision, stored.payload)

    async def _load_candidate(self, scope: Scope, candidate_id: str) -> Candidate:
        self._require_key(candidate_id)
        stored = await self._store.get(scope, _CANDIDATE, candidate_id)
        if stored is None:
            raise NotFound(f"candidate {candidate_id!r} was not found")
        return self._decode_candidate(scope, stored.id, stored.revision, stored.payload)

    async def _candidate_for_mutation(
        self, scope: Scope, candidate_id: str, expected_revision: int
    ) -> Candidate:
        self._require_scope(scope)
        self._require_revision(expected_revision)
        candidate = await self._load_candidate(scope, candidate_id)
        self._check_revision(candidate.revision, expected_revision, _CANDIDATE, candidate_id)
        return candidate

    async def _save_candidate(
        self,
        current: Candidate,
        updated: Candidate,
        expected_revision: int,
        action: IndexAction,
    ) -> Candidate:
        versioned = replace(updated, revision=current.revision + 1)
        stored = await self._store.update(
            current.scope,
            _CANDIDATE,
            current.id,
            expected_revision,
            _codec.encode_candidate(versioned),
        )
        candidate = self._decode_candidate(
            current.scope, stored.id, stored.revision, stored.payload
        )
        await self._append_event(current.scope, action, _CANDIDATE, current.id, candidate.revision)
        return candidate

    async def _append_event(
        self, scope: Scope, action: IndexAction, kind: str, record_id: str, revision: int
    ) -> None:
        stable_event_id = _codec.event_id(scope, action.value, kind, record_id, revision)
        await self._event_sink.append(
            IndexEvent(stable_event_id, scope, action, kind, record_id, revision)
        )

    @staticmethod
    def _decode_evidence(
        scope: Scope, record_id: str, revision: int, payload: dict[str, object]
    ) -> Evidence:
        evidence = _codec.decode_evidence(payload)
        if evidence.scope != scope or evidence.id != record_id or evidence.revision != revision:
            raise ValueError("stored evidence disagrees with its scoped record key or revision")
        return evidence

    @staticmethod
    def _decode_candidate(
        scope: Scope, record_id: str, revision: int, payload: dict[str, object]
    ) -> Candidate:
        candidate = _codec.decode_candidate(payload)
        if candidate.scope != scope or candidate.id != record_id or candidate.revision != revision:
            raise ValueError("stored candidate disagrees with its scoped record key or revision")
        return candidate

    def _now(self) -> datetime:
        now = self._clock()
        if (
            not isinstance(now, datetime)
            or now.tzinfo is None
            or now.utcoffset() != timezone.utc.utcoffset(now)
        ):
            raise ValueError("clock must return a UTC-aware datetime")
        return now

    @staticmethod
    def _require_scope(scope: Scope) -> None:
        if not isinstance(scope, Scope):
            raise ValueError("scope must be a Scope")

    @staticmethod
    def _require_key(key: str) -> None:
        if not isinstance(key, str) or not key.strip():
            raise ValueError("identifier and idempotency key must be non-empty strings")

    @staticmethod
    def _require_revision(revision: int) -> None:
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
            raise ValueError("expected_revision must be a positive integer")

    @staticmethod
    def _check_revision(actual: int, expected: int, kind: str, record_id: str) -> None:
        if actual != expected:
            raise RevisionConflict(
                f"{kind} {record_id!r} revision is {actual}, expected {expected}"
            )

    @staticmethod
    def _require_actor_reason(actor: str, reason: str) -> None:
        if not isinstance(actor, str) or not actor.strip():
            raise ValueError("actor must be a non-empty string")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")

    @staticmethod
    def _require_active_evidence(evidence: Evidence) -> None:
        if evidence.state is not EvidenceState.ACTIVE:
            raise InvalidTransition("evidence has been withdrawn")
