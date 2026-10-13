# V1 validation record

Date: 2026-10-13. Scope: this Aletora checkout only. No host repository was modified and no package was published.

## Executed gates

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
- A page shortened by discarded raw points no longer ends pagination: `SearchIndex` returns a `SearchPage` with raw `scanned`/`exhausted` accounting, and retrieval advances by raw scanned counts until exhaustion. Covered by a Memory-level regression test plus a real local-mode Qdrant mixed valid/malformed test.
- A composition test commits a PostgreSQL-backed lifecycle and durable host outbox/formal write, projects through real Qdrant, and retrieves through a request-bound PostgreSQL resolver. Version changes/deletion, roles and lineage withdrawal remain fail-closed before projection cleanup.
- Public-interface lifecycle/retrieval and independent golden cases cover acceptance versus application, lineage withdrawal, terminal states, validity intervals, roles, stale profile/revision/ref, idempotency conflicts, finite scan budget and failures without insecure fallback.

## Review

Fixed baseline: `ddb606e624f9758843f2f0c4ba4afeafe5857e66` (user-confirmed initial commit).

### Standards

Independent review of the first implementation commit found no documented-standard breaches or meaningful Fowler smells. Lifecycle behavior is concentrated in the `Memory` module; tests exercise its interface; concrete in-memory/SQLAlchemy/Tortoise adapters justify the persistence seam. The fix/composition delta was independently re-reviewed with the same result. The later Low suggestion to share one identifier/table-name rule between the migration renderer and the PostgreSQL contract has been implemented (`migrations` reuses `qualified_table`). Findings: **0**, open findings: **0**.

### Spec

Independent review found **1** issue: delayed Qdrant upserts/deletes could regress a newer projection. Revision-isolated point identities and revision-bounded deletes were implemented, with real local-mode delayed-job regression tests. Independent re-review confirmed the finding closed and reran Qdrant/composition tests (**25 passed**). A second independent review then found **1** low-severity partial implementation (a page shortened by discarded raw points ended pagination before the scan budget was spent) plus 2 contract-clarification observations (`captured_at` replay keeps the first snapshot; application additionally requires active evidence). The pagination defect was fixed via `SearchPage` raw scan accounting with Memory-level and real-Qdrant regression tests; both observations are now explicit spec statements. The entire 79-test matrix and build/lint/type gates were rerun successfully on both Python versions. Independent closure review confirmed: Standards **0** open, Spec **0** open.

Reviewed implementation commits: `d8f2e4f` (V1), `41b56e6` (revision fencing and composition) and `9a9b604` (raw scan accounting and shared table-name rule). Final implementation outcome: approved V1 scope complete and locally verified; no unresolved review blocker.

## Limits of this evidence

- Local runs were on Windows. The Linux Python 3.12/3.14 GitHub Actions matrix with PostgreSQL 16 is configured, but was not dispatched or observed in this session.
- No actual Impulsira/ServexaOS integration, deployment, production Qdrant service, recall-quality benchmark or performance SLO was tested.
- Hosts still own authentication, object authorization, formal promotion, retention/erasure, durable outbox and worker ordering. Scope and transaction adapters do not replace those responsibilities.
- Physical erasure, document parsing, automatic promotion and graph reasoning are explicitly outside V1.
- Research files were pre-existing untracked user work and were not added to the implementation commits.
