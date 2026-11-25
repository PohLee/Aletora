"""Public lifecycle regression tests against independently owned PostgreSQL transactions."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from postgres_host import HostTransaction, PostgresHost

from aletora import (
    Candidate,
    EpistemicKind,
    Evidence,
    EvidenceState,
    FormalRef,
    GovernanceState,
    Proposal,
    Scope,
    SourceKind,
    SourceRef,
)
from aletora.errors import InvalidTransition


@pytest.mark.postgres
@pytest.mark.asyncio
@pytest.mark.parametrize("reverse_ids", [False, True], ids=["ascending", "descending"])
async def test_opposing_supersessions_preserve_an_accepted_replacement(
    postgres_host: PostgresHost, reverse_ids: bool
) -> None:
    scope = Scope("tenant", "supersession")
    stamp = datetime(2026, 10, 13, tzinfo=UTC)
    async with postgres_host.transaction() as transaction:
        memory = transaction.memory
        evidence = await memory.record_evidence(
            scope,
            SourceRef(SourceKind.DOCUMENT, "document", "1", "page:1"),
            "The original evidence",
            idempotency_key="evidence",
            captured_at=stamp,
        )
        accepted = []
        for name in ("first", "second"):
            candidate = await memory.propose(
                scope,
                evidence.id,
                Proposal(name, EpistemicKind.ASSERTED, "author", "extractor", "run", stamp),
                idempotency_key=name,
            )
            accepted.append(
                await memory.review(
                    scope,
                    candidate.id,
                    GovernanceState.ACCEPTED,
                    "reviewer",
                    "verified",
                    expected_revision=candidate.revision,
                )
            )
    first, second = sorted(accepted, key=lambda candidate: candidate.id, reverse=reverse_ids)
    start_rival = asyncio.Event()
    rival_transaction: asyncio.Future[HostTransaction] = asyncio.get_running_loop().create_future()

    async def opposing_supersession() -> Candidate | InvalidTransition:
        async with postgres_host.transaction() as transaction:
            memory = transaction.memory
            rival_transaction.set_result(transaction)
            await start_rival.wait()
            try:
                return await memory.supersede(
                    scope,
                    second.id,
                    first.id,
                    "reviewer",
                    "opposing replacement",
                    expected_revision=second.revision,
                )
            except InvalidTransition as error:
                return error

    # Create the task outside the first transaction so Tortoise cannot inherit
    # its transaction context and silently turn the rival into a nested savepoint.
    rival = asyncio.create_task(opposing_supersession())
    try:
        async with postgres_host.transaction() as transaction:
            memory = transaction.memory
            await memory.supersede(
                scope,
                first.id,
                second.id,
                "reviewer",
                "replacement",
                expected_revision=first.revision,
            )
            start_rival.set()
            async with asyncio.timeout(10):
                second_transaction = await rival_transaction
                # Hold the first transaction until the rival either finishes
                # (the broken implementation) or waits on its PostgreSQL locks.
                # The query is scheduling only; outcomes use the public Memory API.
                while not rival.done():
                    blocked = await postgres_host.is_blocked(second_transaction, by=transaction)
                    if blocked:
                        break
        outcome = await asyncio.wait_for(rival, timeout=10)
        async with postgres_host.transaction() as transaction:
            memory = transaction.memory
            superseded = await memory.get_candidate(scope, first.id)
            replacement = await memory.get_candidate(scope, second.id)
        assert superseded.state is GovernanceState.SUPERSEDED
        assert superseded.replacement_id == second.id
        assert replacement == second
        assert isinstance(outcome, InvalidTransition)
    finally:
        if not rival.done():
            rival.cancel()
        await asyncio.gather(rival, return_exceptions=True)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_concurrent_evidence_withdrawal_blocks_application_across_transactions(
    postgres_host: PostgresHost,
) -> None:
    scope = Scope("tenant", "lineage-lock")
    stamp = datetime(2026, 10, 13, tzinfo=UTC)
    ref = FormalRef("host", "Policy", "formal-lineage", "v1")
    async with postgres_host.transaction() as transaction:
        memory = transaction.memory
        evidence = await memory.record_evidence(
            scope,
            SourceRef(SourceKind.DOCUMENT, "document", "1", "page:1"),
            "The original evidence",
            idempotency_key="lineage-evidence",
            captured_at=stamp,
        )
        proposed = await memory.propose(
            scope,
            evidence.id,
            Proposal("claim", EpistemicKind.ASSERTED, "author", "extractor", "run", stamp),
            idempotency_key="lineage-candidate",
        )
        accepted = await memory.review(
            scope,
            proposed.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "verified",
            expected_revision=proposed.revision,
        )

    start_apply = asyncio.Event()
    apply_tx: asyncio.Future[HostTransaction] = asyncio.get_running_loop().create_future()

    async def apply_worker() -> Candidate | InvalidTransition:
        try:
            async with postgres_host.transaction() as transaction:
                apply_tx.set_result(transaction)
                await start_apply.wait()
                await transaction.write_formal(ref)
                return await transaction.memory.record_application(
                    scope,
                    accepted.id,
                    ref,
                    "host-writer",
                    expected_revision=accepted.revision,
                )
        except InvalidTransition as error:
            return error

    applier = asyncio.create_task(apply_worker())
    try:
        async with postgres_host.transaction() as transaction:
            await transaction.memory.withdraw_evidence(
                scope,
                evidence.id,
                "owner",
                "revoked concurrently",
                expected_revision=evidence.revision,
            )
            start_apply.set()
            async with asyncio.timeout(10):
                rival = await apply_tx
                while not applier.done():
                    if await postgres_host.is_blocked(rival, by=transaction):
                        break
        outcome = await asyncio.wait_for(applier, timeout=10)
        assert isinstance(outcome, InvalidTransition)
        committed = await postgres_host.committed()
        assert committed.formal_refs == ()
        async with postgres_host.transaction() as transaction:
            final_ev = await transaction.memory.get_evidence(scope, evidence.id)
            final_cand = await transaction.memory.get_candidate(scope, accepted.id)
        assert final_ev.state is EvidenceState.WITHDRAWN
        assert final_cand.application is None
    finally:
        if not applier.done():
            applier.cancel()
        await asyncio.gather(applier, return_exceptions=True)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_shared_evidence_lock_blocks_concurrent_withdrawal_until_application_commits(
    postgres_host: PostgresHost,
) -> None:
    scope = Scope("tenant", "lineage-share-lock")
    stamp = datetime(2026, 10, 13, tzinfo=UTC)
    ref = FormalRef("host", "Policy", "formal-share", "v1")
    async with postgres_host.transaction() as transaction:
        memory = transaction.memory
        evidence = await memory.record_evidence(
            scope,
            SourceRef(SourceKind.DOCUMENT, "document", "1", "page:1"),
            "The original evidence",
            idempotency_key="share-evidence",
            captured_at=stamp,
        )
        proposed = await memory.propose(
            scope,
            evidence.id,
            Proposal("claim", EpistemicKind.ASSERTED, "author", "extractor", "run", stamp),
            idempotency_key="share-candidate",
        )
        accepted = await memory.review(
            scope,
            proposed.id,
            GovernanceState.ACCEPTED,
            "reviewer",
            "verified",
            expected_revision=proposed.revision,
        )

    start_withdraw = asyncio.Event()
    withdraw_tx: asyncio.Future[HostTransaction] = asyncio.get_running_loop().create_future()

    async def withdraw_worker() -> Evidence:
        async with postgres_host.transaction() as transaction:
            withdraw_tx.set_result(transaction)
            await start_withdraw.wait()
            return await transaction.memory.withdraw_evidence(
                scope,
                evidence.id,
                "owner",
                "revoked after apply",
                expected_revision=evidence.revision,
            )

    withdrawer = asyncio.create_task(withdraw_worker())
    try:
        async with postgres_host.transaction() as transaction:
            await transaction.write_formal(ref)
            applied = await transaction.memory.record_application(
                scope,
                accepted.id,
                ref,
                "host-writer",
                expected_revision=accepted.revision,
            )
            assert applied.application is not None
            start_withdraw.set()
            async with asyncio.timeout(10):
                rival = await withdraw_tx
                while not await postgres_host.is_blocked(rival, by=transaction):
                    assert not withdrawer.done(), "withdrawal completed before application commit"
                    await asyncio.sleep(0.01)
                assert not withdrawer.done()
        withdrawn_ev = await asyncio.wait_for(withdrawer, timeout=10)
        assert withdrawn_ev.state is EvidenceState.WITHDRAWN
        committed = await postgres_host.committed()
        assert committed.formal_refs == (ref,)
    finally:
        if not withdrawer.done():
            withdrawer.cancel()
        await asyncio.gather(withdrawer, return_exceptions=True)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_reversed_proposal_batches_lock_in_record_id_order(
    postgres_host: PostgresHost,
) -> None:
    from aletora._codec import stable_id

    scope = Scope("tenant", "batch-lock-order")
    stamp = datetime(2026, 10, 13, tzinfo=UTC)
    proposal = Proposal("claim", EpistemicKind.ASSERTED, "author", "extractor", "run", stamp)
    keys = sorted(("a", "b"), key=lambda key: stable_id(scope, "candidate", key))
    items = [(proposal, key) for key in keys]
    async with postgres_host.transaction() as transaction:
        evidence = await transaction.memory.record_evidence(
            scope,
            SourceRef(SourceKind.DOCUMENT, "document", "1", "page:1"),
            "Evidence for both batches",
            idempotency_key="evidence",
            captured_at=stamp,
        )

    start_rival = asyncio.Event()
    rival_tx: asyncio.Future[HostTransaction] = asyncio.get_running_loop().create_future()

    async def reverse_batch() -> tuple[Candidate, ...]:
        await start_rival.wait()
        async with postgres_host.transaction() as transaction:
            rival_tx.set_result(transaction)
            return await transaction.memory.propose_many(scope, evidence.id, items[::-1])

    worker = asyncio.create_task(reverse_batch())
    try:
        async with asyncio.timeout(10):
            async with postgres_host.transaction() as transaction:
                await transaction.memory.propose(
                    scope, evidence.id, proposal, idempotency_key=keys[0]
                )
                start_rival.set()
                rival = await rival_tx
                while not await postgres_host.is_blocked(rival, by=transaction):
                    assert not worker.done()
                    await asyncio.sleep(0.01)
                assert not worker.done()
                forward = await transaction.memory.propose_many(scope, evidence.id, items)
            assert await worker == forward[::-1]
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
