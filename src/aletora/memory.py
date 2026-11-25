"""Aletora's public lifecycle and fail-closed retrieval interface."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from . import _codec
from .errors import (
    CapabilityUnavailable,
    EvidenceIntegrityError,
    EvidenceTooLarge,
    IdempotencyConflict,
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
    ResolvedRecord,
    RetrievalHit,
    RetrievalResult,
    RetrievalStats,
    Scope,
    SourceRef,
    _governance_state_values,
)
from .ports import (
    BatchRecordResolver,
    BatchRecordStore,
    CandidateQueryStore,
    EnumerableRecordStore,
    EventSink,
    IndexAction,
    IndexEvent,
    RecordResolver,
    RecordStore,
    SearchIndex,
    ShareLockableRecordStore,
    StoredRecord,
)

_LOGGER = logging.getLogger(__name__)

_EVIDENCE = "evidence"
_CANDIDATE = "candidate"
_MAX_RETURN_LIMIT = 100
_MAX_LIST_LIMIT = 1000


class Memory:
    """Lifecycle operations over host-owned persistence and outbox transactions.

    Governance mutations verify the digest of evidence they load for a new
    transition and raise ``EvidenceIntegrityError`` on a mismatch. Evidence
    withdrawal remains available for a mismatched digest.
    """

    def __init__(
        self,
        store: RecordStore,
        event_sink: EventSink,
        *,
        index: SearchIndex | None = None,
        resolver: RecordResolver | None = None,
        profile: EmbeddingProfile | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        scan_budget: int = 200,
        max_evidence_bytes: int | None = None,
        on_retrieval: Callable[[RetrievalStats, float], None] | None = None,
    ) -> None:
        if isinstance(scan_budget, bool) or not isinstance(scan_budget, int) or scan_budget < 1:
            raise ValueError("scan_budget must be a positive integer")
        if not callable(clock):
            raise ValueError("clock must be callable")
        if max_evidence_bytes is not None and (
            isinstance(max_evidence_bytes, bool)
            or not isinstance(max_evidence_bytes, int)
            or max_evidence_bytes < 1
        ):
            raise ValueError("max_evidence_bytes must be a positive integer")
        if on_retrieval is not None and not callable(on_retrieval):
            raise ValueError("on_retrieval must be callable")
        self._max_evidence_bytes = max_evidence_bytes
        self._on_retrieval = on_retrieval
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
        """Record an immutable, idempotently keyed source evidence snapshot.

        Retries with the same ``idempotency_key`` and identical ``source`` + ``content``
        return the first recorded snapshot (including its original ``captured_at``).

        Raises:
            ValueError: On invalid arguments or NUL bytes.
            IdempotencyConflict: When ``idempotency_key`` was reused with different
                source metadata or content.
        """
        self._require_scope(scope)
        self._require_key(idempotency_key)
        if not isinstance(source, SourceRef):
            raise ValueError("source must be a SourceRef")
        if not isinstance(content, str):
            raise ValueError("content must be a string")
        if "\x00" in content:
            raise ValueError("content must not contain NUL bytes")
        maximum = self._max_evidence_bytes
        if maximum is not None and len(content.encode("utf-8")) > maximum:
            raise EvidenceTooLarge(f"evidence exceeds the {maximum}-byte UTF-8 limit")
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
        """Fetch an immutable evidence snapshot within ``scope`` without rehashing content.

        Raises:
            ValueError: On invalid scope/key or corrupt stored payload.
            NotFound: When the evidence does not exist in ``scope``.
        """
        self._require_scope(scope)
        return await self._load_evidence(scope, evidence_id)

    async def verify_evidence(self, scope: Scope, evidence_id: str) -> bool:
        """Check whether stored evidence content matches its stored SHA-256 digest."""
        evidence = await self.get_evidence(scope, evidence_id)
        return _codec.hash_content(evidence.content) == evidence.content_hash

    async def propose(
        self,
        scope: Scope,
        evidence_id: str,
        proposal: Proposal,
        *,
        idempotency_key: str,
    ) -> Candidate:
        """Record an unreviewed candidate proposal tied to active evidence in ``scope``.

        Acquires a shared lock on the referenced evidence (or exclusive lock if the
        store does not implement ``ShareLockableRecordStore``) before inserting new
        candidates so concurrent evidence withdrawal cannot commit alongside it.

        Raises:
            ValueError: On invalid arguments or NUL bytes.
            NotFound: When ``evidence_id`` does not exist in ``scope`` for a new proposal.
            InvalidTransition: When the referenced evidence was withdrawn before creation.
            IdempotencyConflict: When ``idempotency_key`` was reused with different semantics.
        """
        self._require_scope(scope)
        self._require_key(evidence_id)
        self._require_key(idempotency_key)
        if not isinstance(proposal, Proposal):
            raise ValueError("proposal must be a Proposal")
        candidate_id, fp, payload = self._build_candidate_record(
            scope, evidence_id, proposal, idempotency_key
        )
        existing: dict[str, Candidate | None] = {}
        await self._fetch_candidates_batch(scope, [candidate_id], existing)
        candidate = existing[candidate_id]
        if candidate is not None:
            self._check_proposal_fingerprint(candidate, fp, idempotency_key)
            return candidate
        try:
            evidence = await self._load_evidence(scope, evidence_id, lock=True)
            self._require_active_evidence(evidence)
        except (NotFound, InvalidTransition):
            existing.clear()
            await self._fetch_candidates_batch(scope, [candidate_id], existing)
            candidate = existing[candidate_id]
            if candidate is None:
                raise
            self._check_proposal_fingerprint(candidate, fp, idempotency_key)
            return candidate
        result = await self._store.insert(
            scope,
            _CANDIDATE,
            candidate_id,
            idempotency_key,
            fp,
            payload,
        )
        return self._decode_candidate(
            scope, result.record.id, result.record.revision, result.record.payload
        )

    async def propose_many(
        self,
        scope: Scope,
        evidence_id: str,
        items: Sequence[tuple[Proposal, str]],
    ) -> tuple[Candidate, ...]:
        """Record a batch of proposals against one active evidence record in ``scope``.

        Validates every ``(proposal, idempotency_key)`` upfront and locks/loads the
        parent evidence once when the batch contains new proposals. Existing
        fingerprints are checked before any insert. The host must roll back the
        entire transaction on any exception, including concurrent insert conflicts.

        Raises:
            ValueError: On invalid inputs, empty keys, or NUL bytes.
            NotFound: When ``evidence_id`` does not exist in ``scope`` for any new proposal.
            InvalidTransition: When the evidence was withdrawn before any new proposal.
            IdempotencyConflict: When any ``idempotency_key`` conflicts with prior semantics.
        """
        self._require_scope(scope)
        self._require_key(evidence_id)
        items_obj: object = items
        if isinstance(items_obj, (str, bytes, bytearray)) or not isinstance(items_obj, Sequence):
            raise ValueError("items must be a sequence of (Proposal, idempotency_key) tuples")
        prepared: list[tuple[str, str, str, dict[str, Any]]] = []
        seen_fingerprints: dict[str, str] = {}
        for entry in items:
            if not isinstance(entry, tuple) or len(entry) != 2:
                raise ValueError("each item must be a (Proposal, idempotency_key) tuple")
            proposal, idempotency_key = entry
            if not isinstance(proposal, Proposal):
                raise ValueError("proposal must be a Proposal")
            self._require_key(idempotency_key)
            candidate_id, fp, payload = self._build_candidate_record(
                scope, evidence_id, proposal, idempotency_key
            )
            prior_fp = seen_fingerprints.get(idempotency_key)
            if prior_fp is not None and prior_fp != fp:
                raise IdempotencyConflict(
                    f"idempotency key {idempotency_key!r} conflicts within batch"
                )
            seen_fingerprints[idempotency_key] = fp
            prepared.append((idempotency_key, candidate_id, fp, payload))
        if not prepared:
            return ()

        candidate_ids = [candidate_id for _, candidate_id, _, _ in prepared]
        existing: dict[str, Candidate | None] = {}
        await self._fetch_candidates_batch(scope, candidate_ids, existing)
        self._check_proposal_batch(prepared, existing)
        if any(candidate is None for candidate in existing.values()):
            try:
                evidence = await self._load_evidence(scope, evidence_id, lock=True)
                self._require_active_evidence(evidence)
            except (NotFound, InvalidTransition):
                # A concurrent proposal may have committed before withdrawal.
                existing = {cid: value for cid, value in existing.items() if value is not None}
                await self._fetch_candidates_batch(scope, candidate_ids, existing)
                self._check_proposal_batch(prepared, existing)
                if any(candidate is None for candidate in existing.values()):
                    raise

        candidates = {cid: value for cid, value in existing.items() if value is not None}
        for idempotency_key, candidate_id, fp, payload in sorted(
            prepared, key=lambda item: item[1]
        ):
            if candidate_id in candidates:
                continue
            result = await self._store.insert(
                scope, _CANDIDATE, candidate_id, idempotency_key, fp, payload
            )
            candidates[candidate_id] = self._decode_candidate(
                scope, result.record.id, result.record.revision, result.record.payload
            )
        return tuple(candidates[cid] for cid in candidate_ids)

    def _check_proposal_batch(
        self,
        prepared: Sequence[tuple[str, str, str, dict[str, Any]]],
        existing: dict[str, Candidate | None],
    ) -> None:
        for key, candidate_id, fingerprint, _ in prepared:
            candidate = existing[candidate_id]
            if candidate is not None:
                self._check_proposal_fingerprint(candidate, fingerprint, key)

    @staticmethod
    def _check_proposal_fingerprint(
        candidate: Candidate, fingerprint: str, idempotency_key: str
    ) -> None:
        semantic = {
            "evidence_id": candidate.evidence_id,
            "proposal": _codec.proposal_payload(candidate),
        }
        if _codec.fingerprint(semantic) != fingerprint:
            raise IdempotencyConflict(
                f"idempotency key {idempotency_key!r} already has different semantics"
            )

    async def get_candidate(self, scope: Scope, candidate_id: str) -> Candidate:
        """Fetch a candidate snapshot within ``scope``.

        Raises:
            ValueError: On invalid scope/key or corrupt stored payload.
            NotFound: When the candidate does not exist in ``scope``.
        """
        self._require_scope(scope)
        return await self._load_candidate(scope, candidate_id)

    async def list_candidates(
        self,
        scope: Scope,
        *,
        after_id: str | None = None,
        limit: int = 100,
    ) -> tuple[Candidate, ...]:
        """List scoped candidates in ascending ``record_id`` order for projection rebuilds.

        Raises:
            ValueError: On invalid scope, cursor, limit, or corrupt stored payload.
            CapabilityUnavailable: When the configured store does not implement
                ``EnumerableRecordStore``.
        """
        self._validate_candidate_page(scope, after_id, limit)
        if not isinstance(self._store, EnumerableRecordStore):
            raise CapabilityUnavailable(
                "listing candidates requires a store implementing EnumerableRecordStore"
            )
        records = await self._store.list_records(scope, _CANDIDATE, after_id=after_id, limit=limit)
        return self._decode_candidate_page(scope, records)

    def _validate_candidate_page(self, scope: Scope, after_id: str | None, limit: int) -> None:
        self._require_scope(scope)
        if after_id is not None:
            self._require_key(after_id)
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= _MAX_LIST_LIMIT
        ):
            raise ValueError(f"limit must be an integer from 1 to {_MAX_LIST_LIMIT}")

    def _candidate_query_store(
        self, scope: Scope, after_id: str | None, limit: int
    ) -> CandidateQueryStore:
        self._validate_candidate_page(scope, after_id, limit)
        if not isinstance(self._store, CandidateQueryStore):
            raise CapabilityUnavailable(
                "filtered candidate listing requires a store implementing CandidateQueryStore"
            )
        return self._store

    def _decode_candidate_page(
        self, scope: Scope, records: Sequence[StoredRecord]
    ) -> tuple[Candidate, ...]:
        return tuple(
            self._decode_candidate(scope, record.id, record.revision, record.payload)
            for record in records
        )

    async def list_candidates_by_state(
        self,
        scope: Scope,
        states: Sequence[GovernanceState],
        *,
        after_id: str | None = None,
        limit: int = 100,
    ) -> tuple[Candidate, ...]:
        """List candidates matching any of the supplied governance states."""
        _governance_state_values(states)
        store = self._candidate_query_store(scope, after_id, limit)
        records = await store.list_candidates_by_state(
            scope, states, after_id=after_id, limit=limit
        )
        return self._decode_candidate_page(scope, records)

    async def list_candidates_by_evidence(
        self,
        scope: Scope,
        evidence_id: str,
        *,
        after_id: str | None = None,
        limit: int = 100,
    ) -> tuple[Candidate, ...]:
        """List candidates referencing the given evidence ID."""
        self._require_key(evidence_id)
        store = self._candidate_query_store(scope, after_id, limit)
        records = await store.list_candidates_by_evidence(
            scope, evidence_id, after_id=after_id, limit=limit
        )
        return self._decode_candidate_page(scope, records)

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
        """Record an attributable acceptance or rejection decision on a proposed candidate.

        Raises:
            ValueError: On invalid arguments, non-``GovernanceState`` decision, or NUL bytes.
            NotFound: When the candidate or its evidence is missing in ``scope``.
            RevisionConflict: When ``expected_revision`` does not match the current revision.
            InvalidTransition: On non-review decision states, self-review, non-proposed
                candidates, or withdrawn parent evidence.
        """
        self._require_scope(scope)
        self._require_key(candidate_id)
        self._require_revision(expected_revision)
        if not isinstance(decision, GovernanceState):
            raise ValueError("decision must be a GovernanceState")
        if decision not in (GovernanceState.ACCEPTED, GovernanceState.REJECTED):
            raise InvalidTransition("review decision must be ACCEPTED or REJECTED")
        self._require_actor_reason(actor, reason)
        candidate = await self._candidate_for_mutation(scope, candidate_id, expected_revision)
        if candidate.state is not GovernanceState.PROPOSED:
            raise InvalidTransition("only proposed candidates may be reviewed")
        if actor == candidate.proposer_id:
            raise InvalidTransition("a proposer cannot review their own candidate")
        evidence = await self._load_evidence(scope, candidate.evidence_id, lock=True)
        self._require_active_evidence(evidence)
        entry = GovernanceEntry(decision, actor, reason, self._now())
        updated = replace(candidate, state=decision, history=(*candidate.history, entry))
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
        """Record the host's formal object receipt for an accepted candidate.

        Idempotent retries against an already-applied candidate must supply the
        candidate's current post-application ``expected_revision`` and the same
        ``ref``; supplying a different ``ref`` raises ``InvalidTransition``, while a
        stale pre-application revision raises ``RevisionConflict``. When returning an
        already-applied snapshot for the same ``ref``, the original receipt ``actor``
        and ``applied_at`` are preserved.

        Raises:
            ValueError: On invalid arguments or NUL bytes.
            NotFound: When the candidate or its evidence is missing in ``scope``.
            RevisionConflict: When ``expected_revision`` does not match the current revision.
            InvalidTransition: When the candidate is not ``ACCEPTED``, its evidence is
                withdrawn, or a different ``FormalRef`` was already recorded.
        """
        self._require_scope(scope)
        self._require_key(candidate_id)
        self._require_revision(expected_revision)
        if not isinstance(ref, FormalRef):
            raise ValueError("ref must be a FormalRef")
        self._require_actor_reason(actor, "application")
        candidate = await self._candidate_for_mutation(scope, candidate_id, expected_revision)
        if candidate.state is not GovernanceState.ACCEPTED:
            raise InvalidTransition("only an accepted candidate may be applied")
        if candidate.application is not None:
            if candidate.application.ref == ref:
                return candidate
            raise InvalidTransition("an application receipt cannot be reassigned")
        evidence = await self._load_evidence(scope, candidate.evidence_id, lock=True)
        self._require_active_evidence(evidence)
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
        """Withdraw a non-terminal candidate while retaining its audit snapshot.

        Raises:
            ValueError: On invalid arguments or NUL bytes.
            NotFound: When the candidate does not exist in ``scope``.
            RevisionConflict: When ``expected_revision`` does not match the current revision.
            InvalidTransition: When the candidate is already ``WITHDRAWN`` or ``SUPERSEDED``.
        """
        self._require_scope(scope)
        self._require_key(candidate_id)
        self._require_revision(expected_revision)
        self._require_actor_reason(actor, reason)
        candidate = await self._candidate_for_mutation(scope, candidate_id, expected_revision)
        if candidate.state in (GovernanceState.WITHDRAWN, GovernanceState.SUPERSEDED):
            raise InvalidTransition("candidate is already terminal")
        entry = GovernanceEntry(GovernanceState.WITHDRAWN, actor, reason, self._now())
        updated = replace(
            candidate,
            state=GovernanceState.WITHDRAWN,
            history=(*candidate.history, entry),
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
        """Mark an accepted candidate superseded by another accepted candidate in ``scope``.

        Locks both candidates in sorted ``record_id`` order before inspecting state,
        then share-locks the replacement's evidence, preventing reciprocal replacement
        cycles and withdrawn-evidence replacements under concurrent transactions.

        Raises:
            ValueError: On invalid arguments or NUL bytes.
            NotFound: When either candidate or replacement evidence is missing.
            RevisionConflict: When ``expected_revision`` does not match ``candidate_id``.
            InvalidTransition: When ``candidate_id == replacement_id``, either candidate
                is not ``ACCEPTED``, or replacement evidence is withdrawn.
        """
        self._require_scope(scope)
        self._require_key(candidate_id)
        self._require_key(replacement_id)
        self._require_revision(expected_revision)
        self._require_actor_reason(actor, reason)
        if candidate_id == replacement_id:
            raise InvalidTransition("a candidate cannot supersede itself")
        # Lock both rows in one stable order before validating their states.
        # Opposing supersessions must observe the winner's committed transition.
        locked = {}
        for record_id in sorted((candidate_id, replacement_id)):
            locked[record_id] = await self._load_candidate(scope, record_id, for_update=True)
        candidate = locked[candidate_id]
        replacement = locked[replacement_id]
        self._check_revision(candidate.revision, expected_revision, _CANDIDATE, candidate_id)
        if candidate.state is not GovernanceState.ACCEPTED:
            raise InvalidTransition("only accepted candidates may be superseded")
        if replacement.state is not GovernanceState.ACCEPTED:
            raise InvalidTransition("replacement candidate must be accepted")
        replacement_evidence = await self._load_evidence(scope, replacement.evidence_id, lock=True)
        self._require_active_evidence(replacement_evidence)
        entry = GovernanceEntry(GovernanceState.SUPERSEDED, actor, reason, self._now())
        updated = replace(
            candidate,
            state=GovernanceState.SUPERSEDED,
            history=(*candidate.history, entry),
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
        """Withdraw evidence so future proposals/applications fail and retrieval drops hits.

        Withdrawal is allowed when stored content does not match its digest.

        Raises:
            ValueError: On invalid arguments or NUL bytes.
            NotFound: When ``evidence_id`` does not exist in ``scope``.
            RevisionConflict: When ``expected_revision`` does not match the current revision.
            InvalidTransition: When the evidence is already withdrawn.
        """
        self._require_scope(scope)
        self._require_key(evidence_id)
        self._require_revision(expected_revision)
        self._require_actor_reason(actor, reason)
        evidence = await self._load_evidence(scope, evidence_id, for_update=True)
        self._check_revision(evidence.revision, expected_revision, _EVIDENCE, evidence_id)
        if evidence.state is EvidenceState.WITHDRAWN:
            raise InvalidTransition("evidence is already withdrawn")
        withdrawal = EvidenceWithdrawal(actor, reason, self._now())
        updated = _codec.withdrawn_evidence(evidence, withdrawal)
        stored = await self._store.update(
            scope, _EVIDENCE, evidence_id, expected_revision, _codec.encode_evidence(updated)
        )
        result = self._decode_evidence(scope, stored.id, stored.revision, stored.payload)
        await self._append_event(
            scope, IndexAction.INVALIDATE_EVIDENCE, _EVIDENCE, evidence_id, result.revision
        )
        return result

    async def retrieve(self, scope: Scope, query: str, *, limit: int) -> tuple[RetrievalHit, ...]:
        """Retrieve authorized, currently valid formal records matching ``query``.

        Citations carry the digest recorded at write time. Retrieval skips rehashing.
        Use ``verify_evidence`` to check content integrity.

        Raises:
            ValueError: On invalid scope, query, limit, or corrupt stored records.
            CapabilityUnavailable: When index, resolver, or profile is not configured.
        """
        return (await self.retrieve_with_stats(scope, query, limit=limit)).hits

    async def retrieve_with_stats(self, scope: Scope, query: str, *, limit: int) -> RetrievalResult:
        """Retrieve authorized formal records paired with bounded-scan diagnostics.

        Citations carry the digest recorded at write time. Retrieval skips rehashing.
        Use ``verify_evidence`` to check content integrity.

        Uses per-call memoization across duplicate/stale hits and shared evidence, and
        automatically uses ``BatchRecordStore.get_many`` / ``BatchRecordResolver.resolve_many``
        when configured, while never consuming more than ``limit - len(hits)`` eligible
        discoveries per batch. Profile and revision mismatches take precedence over
        duplicate classification in both modes.

        Raises:
            ValueError: On invalid scope, query, limit, or corrupt stored records.
            CapabilityUnavailable: When index, resolver, or profile is not configured.
            TypeError: When ``SearchIndex.scan`` yields a non-``IndexHit`` value.
        """
        started = time.perf_counter() if self._on_retrieval is not None else 0.0
        self._require_scope(scope)
        if not isinstance(query, str) or not query.strip():
            raise ValueError("query must be a non-empty string")
        if "\x00" in query:
            raise ValueError("query must not contain NUL bytes")
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

        index = self._index
        resolver = self._resolver
        profile = self._profile
        use_batch = isinstance(self._store, BatchRecordStore) or isinstance(
            resolver, BatchRecordResolver
        )
        now = self._now()

        hits: list[RetrievalHit] = []
        returned_ids: set[str] = set()
        candidates_by_id: dict[str, Candidate | None] = {}
        evidence_by_id: dict[str, Evidence | None] = {}
        resolved_by_ref: dict[FormalRef, ResolvedRecord | None] = {}
        counts = {
            "hits_examined": 0,
            "duplicate_skipped": 0,
            "profile_mismatch_skipped": 0,
            "not_found_skipped": 0,
            "stale_revision_skipped": 0,
            "ineligible_state_skipped": 0,
            "unapplied_skipped": 0,
            "validity_window_skipped": 0,
            "inactive_evidence_skipped": 0,
            "resolver_denied_skipped": 0,
        }

        pending: list[IndexHit] = []
        async with index.scan(scope, query, budget=self._scan_budget) as discovery:
            async for hit in discovery:
                if not isinstance(hit, IndexHit):
                    raise TypeError("SearchIndex.scan must yield IndexHit values")
                counts["hits_examined"] += 1
                if hit.profile_id != profile.profile_id:
                    counts["profile_mismatch_skipped"] += 1
                    continue
                pending.append(hit)
                batch_target = limit - len(hits) if use_batch else 1
                if len(pending) >= batch_target:
                    await self._process_retrieval_batch(
                        scope,
                        pending,
                        now=now,
                        limit=limit,
                        profile=profile,
                        resolver=resolver,
                        hits=hits,
                        returned_ids=returned_ids,
                        candidates_by_id=candidates_by_id,
                        evidence_by_id=evidence_by_id,
                        resolved_by_ref=resolved_by_ref,
                        counts=counts,
                    )
                    pending.clear()
                    if len(hits) == limit:
                        break
            if pending and len(hits) < limit:
                await self._process_retrieval_batch(
                    scope,
                    pending,
                    now=now,
                    limit=limit,
                    profile=profile,
                    resolver=resolver,
                    hits=hits,
                    returned_ids=returned_ids,
                    candidates_by_id=candidates_by_id,
                    evidence_by_id=evidence_by_id,
                    resolved_by_ref=resolved_by_ref,
                    counts=counts,
                )

        stats = RetrievalStats(
            hits_examined=counts["hits_examined"],
            duplicate_skipped=counts["duplicate_skipped"],
            profile_mismatch_skipped=counts["profile_mismatch_skipped"],
            not_found_skipped=counts["not_found_skipped"],
            stale_revision_skipped=counts["stale_revision_skipped"],
            ineligible_state_skipped=counts["ineligible_state_skipped"],
            unapplied_skipped=counts["unapplied_skipped"],
            validity_window_skipped=counts["validity_window_skipped"],
            inactive_evidence_skipped=counts["inactive_evidence_skipped"],
            resolver_denied_skipped=counts["resolver_denied_skipped"],
            returned=len(hits),
            budget=self._scan_budget,
            limit_reached=len(hits) == limit,
        )
        result = RetrievalResult(tuple(hits), stats)
        if self._on_retrieval is not None:
            try:
                self._on_retrieval(stats, time.perf_counter() - started)
            except Exception:
                _LOGGER.warning("Retrieval observer failed", exc_info=True)
        return result

    async def _process_retrieval_batch(
        self,
        scope: Scope,
        batch: list[IndexHit],
        *,
        now: datetime,
        limit: int,
        profile: EmbeddingProfile,
        resolver: RecordResolver,
        hits: list[RetrievalHit],
        returned_ids: set[str],
        candidates_by_id: dict[str, Candidate | None],
        evidence_by_id: dict[str, Evidence | None],
        resolved_by_ref: dict[FormalRef, ResolvedRecord | None],
        counts: dict[str, int],
    ) -> None:
        await self._fetch_candidates_batch(
            scope, [item.candidate_id for item in batch], candidates_by_id
        )
        with_candidate: list[tuple[IndexHit, Candidate]] = []
        for item in batch:
            candidate = candidates_by_id[item.candidate_id]
            if candidate is None:
                counts["not_found_skipped"] += 1
                continue
            if candidate.revision != item.revision:
                counts["stale_revision_skipped"] += 1
                continue
            if candidate.state is not GovernanceState.ACCEPTED:
                counts["ineligible_state_skipped"] += 1
                continue
            if candidate.application is None:
                counts["unapplied_skipped"] += 1
                continue
            if (candidate.valid_from is not None and now < candidate.valid_from) or (
                candidate.valid_until is not None and now >= candidate.valid_until
            ):
                counts["validity_window_skipped"] += 1
                continue
            with_candidate.append((item, candidate))

        if not with_candidate:
            return
        await self._fetch_evidence_batch(
            scope, [candidate.evidence_id for _, candidate in with_candidate], evidence_by_id
        )
        with_evidence: list[tuple[IndexHit, Candidate, Evidence]] = []
        for item, candidate in with_candidate:
            evidence = evidence_by_id[candidate.evidence_id]
            if evidence is None or evidence.state is not EvidenceState.ACTIVE:
                counts["inactive_evidence_skipped"] += 1
                continue
            with_evidence.append((item, candidate, evidence))

        if not with_evidence:
            return
        if isinstance(resolver, BatchRecordResolver):
            pending_refs = [
                candidate.application.ref
                for _, candidate, _ in with_evidence
                if candidate.application is not None
                and candidate.application.ref not in resolved_by_ref
            ]
            if pending_refs:
                unique_refs = list(dict.fromkeys(pending_refs))
                resolved_map = await resolver.resolve_many(scope, unique_refs)
                for ref in unique_refs:
                    resolved_by_ref[ref] = resolved_map.get(ref)

        for item, candidate, evidence in with_evidence:
            if item.candidate_id in returned_ids:
                counts["duplicate_skipped"] += 1
                continue
            assert candidate.application is not None
            expected_ref = candidate.application.ref
            if expected_ref in resolved_by_ref:
                resolved = resolved_by_ref[expected_ref]
            else:
                resolved = await resolver.resolve(scope, expected_ref)
                resolved_by_ref[expected_ref] = resolved
            if (
                resolved is None
                or not isinstance(resolved, ResolvedRecord)
                or resolved.ref != expected_ref
            ):
                counts["resolver_denied_skipped"] += 1
                continue
            citation = Citation(evidence.id, evidence.source, evidence.content_hash)
            hits.append(
                RetrievalHit(
                    candidate.id,
                    candidate.revision,
                    expected_ref,
                    resolved.content,
                    (citation,),
                    item.score,
                    candidate.epistemic_kind,
                    candidate.state,
                    profile.profile_id,
                )
            )
            returned_ids.add(candidate.id)
            if len(hits) == limit:
                break

    async def _fetch_candidates_batch(
        self,
        scope: Scope,
        candidate_ids: list[str],
        cache: dict[str, Candidate | None],
    ) -> None:
        missing = [cid for cid in dict.fromkeys(candidate_ids) if cid not in cache]
        if not missing:
            return
        if isinstance(self._store, BatchRecordStore):
            stored_map = await self._store.get_many(scope, _CANDIDATE, missing)
            for cid in missing:
                stored = stored_map.get(cid)
                cache[cid] = (
                    self._decode_candidate(scope, stored.id, stored.revision, stored.payload)
                    if stored is not None
                    else None
                )
            return
        for cid in missing:
            stored = await self._store.get(scope, _CANDIDATE, cid)
            cache[cid] = (
                self._decode_candidate(scope, stored.id, stored.revision, stored.payload)
                if stored is not None
                else None
            )

    async def _fetch_evidence_batch(
        self,
        scope: Scope,
        evidence_ids: list[str],
        cache: dict[str, Evidence | None],
    ) -> None:
        missing = [eid for eid in dict.fromkeys(evidence_ids) if eid not in cache]
        if not missing:
            return
        if isinstance(self._store, BatchRecordStore):
            stored_map = await self._store.get_many(scope, _EVIDENCE, missing)
            for eid in missing:
                stored = stored_map.get(eid)
                cache[eid] = (
                    self._decode_evidence(scope, stored.id, stored.revision, stored.payload)
                    if stored is not None
                    else None
                )
            return
        for eid in missing:
            stored = await self._store.get(scope, _EVIDENCE, eid)
            cache[eid] = (
                self._decode_evidence(scope, stored.id, stored.revision, stored.payload)
                if stored is not None
                else None
            )

    @staticmethod
    def _build_candidate_record(
        scope: Scope, evidence_id: str, proposal: Proposal, idempotency_key: str
    ) -> tuple[str, str, dict[str, Any]]:
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
        return candidate_id, _codec.fingerprint(semantic), _codec.encode_candidate(candidate)

    async def _load_evidence(
        self, scope: Scope, evidence_id: str, *, lock: bool = False, for_update: bool = False
    ) -> Evidence:
        self._require_key(evidence_id)
        if lock:
            if isinstance(self._store, ShareLockableRecordStore):
                stored = await self._store.get_for_share(scope, _EVIDENCE, evidence_id)
            else:
                stored = await self._store.get(scope, _EVIDENCE, evidence_id, for_update=True)
        else:
            stored = await self._store.get(scope, _EVIDENCE, evidence_id, for_update=for_update)
        if stored is None:
            raise NotFound(f"evidence {evidence_id!r} was not found")
        evidence = self._decode_evidence(scope, stored.id, stored.revision, stored.payload)
        # Only governance paths using lock=True require a matching digest.
        if lock and _codec.hash_content(evidence.content) != evidence.content_hash:
            raise EvidenceIntegrityError(
                f"evidence {evidence_id!r} content_hash does not match content"
            )
        return evidence

    async def _load_candidate(
        self, scope: Scope, candidate_id: str, *, for_update: bool = False
    ) -> Candidate:
        self._require_key(candidate_id)
        stored = await self._store.get(scope, _CANDIDATE, candidate_id, for_update=for_update)
        if stored is None:
            raise NotFound(f"candidate {candidate_id!r} was not found")
        return self._decode_candidate(scope, stored.id, stored.revision, stored.payload)

    async def _candidate_for_mutation(
        self, scope: Scope, candidate_id: str, expected_revision: int
    ) -> Candidate:
        self._require_scope(scope)
        self._require_revision(expected_revision)
        # Acquire candidate row lock before loading/locking parent evidence so all
        # mutation paths share the unidirectional Candidate -> Evidence lock order.
        candidate = await self._load_candidate(scope, candidate_id, for_update=True)
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
            or now.utcoffset() != UTC.utcoffset(now)
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
        if "\x00" in key:
            raise ValueError("identifier and idempotency key must not contain NUL bytes")

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
        if "\x00" in actor:
            raise ValueError("actor must not contain NUL bytes")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("reason must be a non-empty string")
        if "\x00" in reason:
            raise ValueError("reason must not contain NUL bytes")

    @staticmethod
    def _require_active_evidence(evidence: Evidence) -> None:
        if evidence.state is not EvidenceState.ACTIVE:
            raise InvalidTransition("evidence has been withdrawn")
