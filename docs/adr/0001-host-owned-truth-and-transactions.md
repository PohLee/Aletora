# ADR-0001: Host-owned truth and transactions

Status: Accepted for V1 (2026-10-13).

## Context

Impulsira and ServexaOS own different business knowledge, authorization policies and transaction mechanisms. An independently committing memory store would create dual truth and prevent atomic promotion. Installing a whole agent runtime would also make the shared package unnecessarily costly.

## Decision

Aletora owns evidence, candidates and their governance history, not published business knowledge. Review acceptance and host application are separate. Application is recorded only using a host receipt, within the same host transaction as the formal write and outbox append.

The core is standard-library-only. Concrete SQLAlchemy and Tortoise adapters use an already-open host transaction, never commit, rollback or close it. Host migrations apply packaged PostgreSQL SQL templates. V1 stores versioned JSON aggregates in tenant/namespace-scoped records with compare-and-swap updates, rather than maintaining two ORM model hierarchies. This keeps governance implementation local while allowing both concrete transaction adapters to share one SQL contract.

Qdrant is optional and rebuildable. Retrieval requires accepted and applied candidates, checks current record revisions and evidence validity, then asks the host to authorize and read the exact current formal object version. Aletora is an in-process library, not a security sandbox against a malicious host.

## Consequences

Hosts retain migration, retention, authorization and business promotion responsibilities. SQL templates are integrated by the host; no package-owned connection pool, broker or migration runner is provided. Shared RLS and dedicated schema behavior require real PostgreSQL tests. JSON aggregate storage favors a compact V1 over broad relational reporting; new query requirements may justify normalized projections later.

The module earns depth by concentrating provenance, idempotency, lifecycle transitions and fail-closed retrieval behind one interface. Deleting it would distribute that complexity across the hosts. Future adapters must correspond to demonstrated variation, not speculative seams.
