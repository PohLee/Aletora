# V1 validation record

Date: 2026-10-13. Scope: this Aletora checkout only. No host repository was modified and no package was published.

## Executed gates

| Gate | Python 3.12.13 | Python 3.14.2 |
| --- | --- | --- |
| Frozen all-extras development install | Pass | Pass |
| Ruff lint and format check | Pass | Pass |
| Strict mypy, 13 source files | Pass | Pass |
| Full suite including PostgreSQL | 70 passed, 0 skipped | 70 passed, 0 skipped |
| Clean core-only wheel install | Pass | Pass |
| README lifecycle example from installed wheel | Pass | Pass |
| Packaged dedicated/shared SQL resources | Pass | Pass |

Wheel and source distribution were built successfully; the wheel was built from the sdist. No optional ORM or Qdrant dependency was present in either clean core-only environment.

The full gate was executed using `scripts/verify.ps1` with absolute Python interpreter paths and `ALETORA_TEST_POSTGRES_DSN` pointing to an independently created temporary PostgreSQL cluster bound only to loopback, not the existing host database service. An initial format-check failure in a Qdrant test was fixed before rerunning the whole gate.

## Concrete integration evidence

- Real PostgreSQL 18.6, uniquely named test schemas/roles; SQLAlchemy 2.1.4, Tortoise ORM 0.25.4 and asyncpg 0.32.0 (locked versions).
- Scoped idempotent inserts and compare-and-swap with concurrent transactions.
- Both ORM adapters exercised through `Memory`, with successful commit and host formal/outbox rollback behavior.
- Tortoise rejects bare, unbegun, finalized, released and non-PostgreSQL transaction wrappers.
- Shared tenant isolation checked with a restricted non-superuser/non-BYPASSRLS role; dedicated schema/search_path behavior checked using real database connections.
- Qdrant client 1.19.1 used real local in-memory mode: projection retry/rebuild, scope/profile filtering, collection compatibility and scoped invalidation/deletion.
- Public-interface lifecycle/retrieval and independent golden cases cover acceptance versus application, lineage withdrawal, terminal states, validity intervals, roles, stale profile/revision/ref, idempotency conflicts, finite scan budget and failures without insecure fallback.

## Review

Fixed baseline: `ddb606e624f9758843f2f0c4ba4afeafe5857e66` (user-confirmed initial commit).
Separate Standards and Spec reviews are required before final completion; findings and closure will be recorded here after that review.

## Limits of this evidence

- Local runs were on Windows. The Linux Python 3.12/3.14 GitHub Actions matrix with PostgreSQL 16 is configured, but was not dispatched or observed in this session.
- No actual Impulsira/ServexaOS integration, deployment, production Qdrant service, recall-quality benchmark or performance SLO was tested.
- Hosts still own authentication, object authorization, formal promotion, retention/erasure, durable outbox and worker ordering. Scope and transaction adapters do not replace those responsibilities.
- Physical erasure, document parsing, automatic promotion and graph reasoning are explicitly outside V1.
- Research files were pre-existing untracked user work and were not added to the implementation commits.
