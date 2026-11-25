"""A host-owned SQLAlchemy outbox batch for Qdrant reconciliation."""

from collections.abc import Awaitable, Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aletora import Candidate, EventSink, IndexAction, IndexEvent, Memory, Scope
from aletora.adapters.qdrant import QdrantIndex, VectorLike, reconcile_event
from aletora.adapters.sqlalchemy import SQLAlchemyRecordStore

_MAX_ATTEMPTS = 3


async def run_batch(
    session: AsyncSession,
    scope: Scope,
    event_sink: EventSink,
    index: QdrantIndex,
    embed_candidate: Callable[[Candidate], Awaitable[VectorLike]],
    *,
    limit: int = 20,
    schema: str | None = None,
) -> int:
    """Process one batch using an idle, host-provided session."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")

    async with session.begin():
        await session.execute(
            text("SELECT set_config('app.current_tenant_id', :tenant_id, true)"),
            {"tenant_id": scope.tenant_id},
        )
        memory = Memory(SQLAlchemyRecordStore(session, schema=schema), event_sink)
        rows = await session.execute(
            text("""
                SELECT id, action, kind, record_id, revision
                FROM host_outbox
                WHERE tenant_id = :tenant_id AND namespace = :namespace
                  AND processed_at IS NULL AND dead_lettered_at IS NULL
                ORDER BY queued_at, id
                LIMIT :limit FOR UPDATE SKIP LOCKED
            """),
            {"tenant_id": scope.tenant_id, "namespace": scope.namespace, "limit": limit},
        )
        count = 0
        for row in rows.mappings():
            try:
                async with session.begin_nested():
                    event = IndexEvent(
                        row["id"],
                        scope,
                        IndexAction(row["action"]),
                        row["kind"],
                        row["record_id"],
                        row["revision"],
                    )
                    await reconcile_event(memory, index, embed_candidate, event)
                    await session.execute(
                        text(
                            "UPDATE host_outbox SET processed_at = clock_timestamp() WHERE id = :id"
                        ),
                        {"id": event.id},
                    )
            except Exception as exc:
                await session.execute(
                    text("""
                        UPDATE host_outbox
                        SET attempts = attempts + 1,
                            last_error = :error,
                            dead_lettered_at = CASE WHEN attempts + 1 >= :max_attempts
                                THEN clock_timestamp() END
                        WHERE id = :id
                    """),
                    {"id": row["id"], "error": type(exc).__name__, "max_attempts": _MAX_ATTEMPTS},
                )
                continue
            count += 1
        return count
