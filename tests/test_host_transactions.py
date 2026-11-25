"""Host transaction guarantees exercised identically through both real adapters."""

from datetime import UTC, datetime

import pytest
from postgres_host import HostTransaction, PostgresHost

from aletora import (
    Candidate,
    EpistemicKind,
    Evidence,
    FormalRef,
    GovernanceState,
    Proposal,
    Scope,
    SourceKind,
    SourceRef,
)
from aletora.errors import NotFound
from aletora.ports import IndexAction

pytestmark = [pytest.mark.postgres, pytest.mark.asyncio]
SCOPE = Scope("tenant", "host-transactions")
STAMP = datetime(2026, 10, 13, tzinfo=UTC)
REF = FormalRef("host", "rule", "formal-1", "v1")


async def accept_candidate(transaction: HostTransaction) -> tuple[Evidence, Candidate]:
    memory = transaction.memory
    evidence = await memory.record_evidence(
        SCOPE,
        SourceRef(SourceKind.EXPLICIT_USER, "user-1", "v1", "chat"),
        "A documented assertion",
        idempotency_key="evidence-key",
        captured_at=STAMP,
    )
    candidate = await memory.propose(
        SCOPE,
        evidence.id,
        Proposal(
            "A candidate assertion", EpistemicKind.ASSERTED, "author", "extractor", "run", STAMP
        ),
        idempotency_key="candidate-key",
    )
    accepted = await memory.review(
        SCOPE,
        candidate.id,
        GovernanceState.ACCEPTED,
        "reviewer",
        "verified",
        expected_revision=candidate.revision,
    )
    return evidence, accepted


async def apply_candidate(transaction: HostTransaction, candidate: Candidate) -> Candidate:
    await transaction.write_formal(REF)
    return await transaction.memory.record_application(
        SCOPE, candidate.id, REF, "writer", expected_revision=candidate.revision
    )


@pytest.mark.parametrize("failure", ["sink", "host"], ids=["outbox-append", "host-exception"])
async def test_failed_promotion_rolls_back_and_can_be_retried(
    postgres_host: PostgresHost, failure: str
) -> None:
    initial = await postgres_host.committed()
    with pytest.raises(RuntimeError, match="sink failed" if failure == "sink" else "host failed"):
        async with postgres_host.transaction(
            fail_on_append=2 if failure == "sink" else None
        ) as transaction:
            evidence, accepted = await accept_candidate(transaction)
            await apply_candidate(transaction, accepted)
            raise RuntimeError("host failed")

    assert await postgres_host.committed() == initial
    async with postgres_host.transaction() as transaction:
        with pytest.raises(NotFound):
            await transaction.memory.get_evidence(SCOPE, evidence.id)
        with pytest.raises(NotFound):
            await transaction.memory.get_candidate(SCOPE, accepted.id)

    # Retry the same idempotency keys and formal reference after host rollback.
    async with postgres_host.transaction() as transaction:
        evidence, accepted = await accept_candidate(transaction)
        applied = await apply_candidate(transaction, accepted)
        assert applied.application is not None
        assert applied.application.ref == REF
        assert await postgres_host.committed() == initial

    committed = await postgres_host.committed()
    assert committed.records == 2
    assert committed.formal_refs == (REF,)
    assert len(committed.events) == 2
    assert len({event.id for event in committed.events}) == 2
    assert [
        (event.scope, event.action, event.kind, event.record_id, event.revision)
        for event in committed.events
    ] == [
        (SCOPE, IndexAction.REFRESH_CANDIDATE, "candidate", accepted.id, accepted.revision),
        (SCOPE, IndexAction.REFRESH_CANDIDATE, "candidate", applied.id, applied.revision),
    ]
    async with postgres_host.transaction() as transaction:
        assert await transaction.memory.get_evidence(SCOPE, evidence.id) == evidence
        assert await transaction.memory.get_candidate(SCOPE, applied.id) == applied


async def test_filtered_candidate_queries(postgres_host: PostgresHost) -> None:
    async with postgres_host.transaction() as transaction:
        memory = transaction.memory
        evidence, accepted = await accept_candidate(transaction)
        proposal = Proposal(
            "Another claim",
            EpistemicKind.ASSERTED,
            "author",
            "extractor",
            "run",
            STAMP,
        )
        proposed = await memory.propose(SCOPE, evidence.id, proposal, idempotency_key="other")
        other_scope = Scope(SCOPE.tenant_id, "other")
        other_evidence = await memory.record_evidence(
            other_scope,
            evidence.source,
            evidence.content,
            idempotency_key="other",
        )
        await memory.propose(other_scope, other_evidence.id, proposal, idempotency_key="other")
        assert await memory.list_candidates_by_state(SCOPE, [GovernanceState.PROPOSED]) == (
            proposed,
        )
        assert await memory.list_candidates_by_state(SCOPE, []) == ()
        expected = tuple(sorted((accepted, proposed), key=lambda candidate: candidate.id))
        states = [GovernanceState.ACCEPTED, GovernanceState.PROPOSED]
        assert await memory.list_candidates_by_state(SCOPE, states, limit=1) == expected[:1]
        assert (
            await memory.list_candidates_by_state(
                SCOPE,
                states,
                after_id=expected[0].id,
            )
            == expected[1:]
        )
        assert (
            await memory.list_candidates_by_evidence(
                SCOPE,
                evidence.id,
                limit=1,
            )
            == expected[:1]
        )
        assert (
            await memory.list_candidates_by_evidence(
                SCOPE,
                evidence.id,
                after_id=expected[0].id,
            )
            == expected[1:]
        )
        assert await memory.list_candidates_by_evidence(other_scope, evidence.id) == ()
        assert await memory.list_candidates_by_evidence(SCOPE, other_evidence.id) == ()
