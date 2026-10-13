"""Public lifecycle regression tests against independently owned PostgreSQL transactions."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from postgres_host import HostTransaction, PostgresHost

from aletora import (
    Candidate,
    EpistemicKind,
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
    stamp = datetime(2026, 10, 13, tzinfo=timezone.utc)
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
