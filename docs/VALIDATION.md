# V1 validation record

Date: 2026-10-13. Scope: this Aletora checkout only. No host repository was modified and no package was published.

## Architecture deepening (2026-10-13)

Both selected architecture improvements are implemented in the working tree:

- `SearchIndex.scan(scope, query, budget=...)` returns one async context-managed discovery traversal. Qdrant prepares the query once and owns raw pagination, budget accounting and exhaustion; Memory retains governance, authorization, deduplication and early stopping. Context exit closes local iteration without closing the host client.
- The shared real-PostgreSQL test host centralizes schema setup, transactions, formal writes, durable outbox failure injection and independent committed-state observations. Common rollback/retry/commit scenarios run against both SQLAlchemy and Tortoise. The previous supersession schedules and outcome assertions remain; adapter-specific transaction-mode, CAS, RLS and search-path tests are retained.

| Current gate | Python 3.12.13 | Python 3.14.2 |
| --- | --- | --- |
| Full suite with isolated PostgreSQL 18 | 112 passed, 0 skipped | 112 passed, 0 skipped |
| Strict mypy, 13 source files | Pass | Pass |

Ruff lint, format checks (25 source/test files) and Git whitespace checks passed. The sdist
and wheel built from that sdist passed. A clean core-only wheel install confirmed the public
scan interface, absence of optional ORM/Qdrant dependencies and both packaged SQL templates.

A local Qdrant regression first reproduced repeated embedding under the old page interface.
New composed retrieval tests cover a fully malformed raw page followed by stale, denied and
authorized candidates; budgets of 32/33/34/35/100; exactly one embedding per traversal; and
early stopping before the next page. Additional tests cover forced interleaving of concurrent
queries, bounded cancellation coordination, iterator cleanup, client ownership and explicit
backend/invalid-score failures. Independent review found no remaining implementation or
coverage findings after strengthening the concurrency/cancellation test scheduling.

The initial attempt to reuse an older Python 3.14 environment failed because its Tortoise
installation was incomplete. The authoritative 3.14 result above used a fresh temporary
environment installed offline from the frozen lockfile with all extras; no source change was
needed for that environment failure.

Compatibility: this replaces the unreleased 0.1 `search(..., limit, offset)` / `SearchPage`
interface. Custom index adapters must implement `scan`; `Memory.retrieve` is unchanged.
Existing lifecycle/concurrency/AUTOCOMMIT corrections were preserved. No source commit,
push, release or host-application change was made. The temporary PostgreSQL cluster was
stopped after validation. Production performance and actual Impulsira/ServexaOS integration
remain unverified.

## Post-review corrections (2026-10-13)

A subsequent independent review reproduced two defects beyond the initial 79-test gate: reciprocal supersession could commit a terminal replacement cycle, and SQLAlchemy AUTOCOMMIT could pass the logical transaction guard while persisting writes after rollback. Both are now fixed in the working tree.

- Supersession locks both scoped candidates in sorted ID order before validating their current states/revision. Locks belong to the host transaction. Four real PostgreSQL regressions cover SQLAlchemy/Tortoise and ascending/descending candidate IDs; all four failed before the fix and now pass.
- SQLAlchemy checks the actual DBAPI autocommit state on the connection selected for the exact SQL clause, then executes on that same connection. Nine regressions cover engine/options/bound/session/routed connection configurations and valid transactional overrides. The original and routed AUTOCOMMIT cases each reproduced persisted candidate/outbox writes before their fixes.
- The low-priority duplicated validity-window check is consolidated without changing validation behavior.

| Correction gate | Python 3.12 | Python 3.14.2 |
| --- | --- | --- |
| Full suite with real PostgreSQL 18 | 92 passed, 0 skipped | 92 passed, 0 skipped |
| Strict mypy, 13 source files | Pass | Pass |

Ruff lint and format checks passed for the complete source/test tree. The sdist and a wheel built from that sdist passed; a clean core-only wheel install verified imports and both packaged SQL templates without optional ORM/Qdrant dependencies. A separate independent review of the complete fix found no remaining correctness or specification findings. The isolated loopback PostgreSQL instance was stopped after verification. No source commit, push, release or host-application change was made during these corrections.

The historical gate below records the initial implementation; the 92-test gate records the subsequent corrections. The current architecture gate is recorded at the top. Linux CI, production Qdrant and actual host integration remain outside this local verification.

## Initial implementation gates

| Gate | Python 3.12.13 | Python 3.14.2 |
| --- | --- | --- |
| Frozen all-extras development install | Pass | Pass |
| Ruff lint and format check | Pass | Pass |
| Strict mypy, 13 source files | Pass | Pass |
| Full suite including PostgreSQL | 79 passed, 0 skipped | 79 passed, 0 skipped |
| Clean core-only wheel install | Pass | Pass |
| README lifecycle example from installed wheel | Pass | Pass |
| Packaged dedicated/shared SQL resources | Pass | Pass |

Wheel and source distribution were built successfully; the wheel was built from the sdist. No optional ORM or Qdrant dependency was present in either clean core-only environment.

The full gate was executed using `scripts/verify.ps1` with absolute Python interpreter paths and `ALETORA_TEST_POSTGRES_DSN` pointing to an independently created temporary PostgreSQL cluster bound only to loopback, not the existing host database service. An initial format-check failure in a Qdrant test was fixed before rerunning the whole gate.

The temporary PostgreSQL instance was stopped after final tests and independent closure review. Its data directory and uniquely named verification environments were retained for inspection, not recursively deleted.

## Concrete integration evidence

- Real PostgreSQL 18.6, uniquely named test schemas/roles; SQLAlchemy 2.1.4, Tortoise ORM 0.25.4 and asyncpg 0.32.0 (locked versions).
- Scoped idempotent inserts and compare-and-swap with concurrent transactions.
- Both ORM adapters exercised through `Memory`, with successful commit and host formal/outbox rollback behavior.
- Tortoise rejects bare, unbegun, finalized, released and non-PostgreSQL transaction wrappers.
- Shared tenant isolation checked with a restricted non-superuser/non-BYPASSRLS role; dedicated schema/search_path behavior checked using real database connections.
- Qdrant client 1.19.1 used real local in-memory mode: projection retry/rebuild, scope/profile filtering, collection compatibility and scoped invalidation/deletion.
- Delayed old upserts cannot replace a newer point; delayed old deletes cannot delete newer revisions. Tests use revision-isolated point IDs and revision-bounded filters against real local-mode Qdrant, including `Memory` retrieval of the latest formal record.
- A page shortened by discarded raw points does not end retrieval. Raw accounting was initially exposed through `SearchPage`; the current bounded `SearchIndex.scan` keeps that accounting inside the adapter. Real local-mode Qdrant tests cover mixed/entirely malformed pages, remaining budgets and governed retrieval across pages.
- A composition test commits a PostgreSQL-backed lifecycle and durable host outbox/formal write, projects through real Qdrant, and retrieves through a request-bound PostgreSQL resolver. Version changes/deletion, roles and lineage withdrawal remain fail-closed before projection cleanup.
- Public-interface lifecycle/retrieval and independent golden cases cover acceptance versus application, lineage withdrawal, terminal states, validity intervals, roles, stale profile/revision/ref, idempotency conflicts, finite scan budget and failures without insecure fallback.

## Review

Fixed baseline: `ddb606e624f9758843f2f0c4ba4afeafe5857e66` (user-confirmed initial commit).

### Standards

Independent review of the first implementation commit found no documented-standard breaches or meaningful Fowler smells. Lifecycle behavior is concentrated in the `Memory` module; tests exercise its interface; concrete in-memory/SQLAlchemy/Tortoise adapters justify the persistence seam. The fix/composition delta was independently re-reviewed with the same result. The later Low suggestion to share one identifier/table-name rule between the migration renderer and the PostgreSQL contract has been implemented (`migrations` reuses `qualified_table`). Findings: **0**, open findings: **0**.

### Spec

Independent review found **1** issue: delayed Qdrant upserts/deletes could regress a newer projection. Revision-isolated point identities and revision-bounded deletes were implemented, with real local-mode delayed-job regression tests. Independent re-review confirmed the finding closed and reran Qdrant/composition tests (**25 passed**). A second independent review then found **1** low-severity partial implementation (a page shortened by discarded raw points ended pagination before the scan budget was spent) plus 2 contract-clarification observations (`captured_at` replay keeps the first snapshot; application additionally requires active evidence). The pagination defect was fixed via `SearchPage` raw scan accounting with Memory-level and real-Qdrant regression tests; both observations are now explicit spec statements. The entire 79-test matrix and build/lint/type gates were rerun successfully on both Python versions. Independent closure review confirmed: Standards **0** open, Spec **0** open.

Reviewed implementation commits: `d8f2e4f` (V1), `41b56e6` (revision fencing and composition) and `9a9b604` (raw scan accounting and shared table-name rule). That review's closure was superseded by the subsequent findings and corrections recorded above.

## Limits of this evidence

- Local runs were on Windows. The Linux Python 3.12/3.14 GitHub Actions matrix with PostgreSQL 16 is configured, but was not dispatched or observed in this session.
- No actual Impulsira/ServexaOS integration, deployment, production Qdrant service, recall-quality benchmark or performance SLO was tested.
- Hosts still own authentication, object authorization, formal promotion, retention/erasure, durable outbox and worker ordering. Scope and transaction adapters do not replace those responsibilities.
- Physical erasure, document parsing, automatic promotion and graph reasoning are explicitly outside V1.
- Research files were pre-existing untracked user work and were not added to the implementation commits.
