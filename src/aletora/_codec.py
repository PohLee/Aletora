"""The single JSON aggregate codec used by core persistence adapters."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any
from uuid import UUID, uuid5

from .models import (
    ApplicationReceipt,
    Candidate,
    EpistemicKind,
    Evidence,
    EvidenceState,
    EvidenceWithdrawal,
    FormalRef,
    GovernanceEntry,
    GovernanceState,
    Proposal,
    Scope,
    SourceKind,
    SourceRef,
)

_ID_NAMESPACE = UUID("169f1a70-e215-4c66-9d68-7a23f73b017d")
_SCHEMA_VERSION = 1


def stable_id(scope: Scope, kind: str, key: str) -> str:
    """Make a deterministic ID from an unambiguous structured tuple."""
    name = json.dumps(
        [scope.tenant_id, scope.namespace, kind, key],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return str(uuid5(_ID_NAMESPACE, name))


def event_id(scope: Scope, action: str, kind: str, record_id: str, revision: int) -> str:
    name = json.dumps(
        [scope.tenant_id, scope.namespace, action, kind, record_id, revision],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return str(uuid5(_ID_NAMESPACE, name))


def hash_content(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def fingerprint(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def source_payload(source: SourceRef) -> dict[str, Any]:
    return _source_dict(source)


def proposal_payload(proposal: Proposal) -> dict[str, Any]:
    return {
        "text": proposal.text,
        "epistemic_kind": proposal.epistemic_kind.value,
        "proposer_id": proposal.proposer_id,
        "extractor_id": proposal.extractor_id,
        "run_id": proposal.run_id,
        "proposed_at": _time(proposal.proposed_at),
        "valid_from": _time(proposal.valid_from),
        "valid_until": _time(proposal.valid_until),
    }


def _time(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


def _required_time(value: str | None) -> datetime:
    parsed = _parse_time(value)
    if parsed is None:
        raise ValueError("stored timestamp is missing")
    return parsed


def _require_schema_version(value: dict[str, Any], aggregate: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"stored {aggregate} payload must be a JSON object")
    version = value.get("schema_version")
    if type(version) is not int or version != _SCHEMA_VERSION:
        shown = "missing" if "schema_version" not in value else repr(version)
        raise ValueError(
            f"unsupported {aggregate} schema_version {shown}; expected integer {_SCHEMA_VERSION}"
        )


def _source_dict(source: SourceRef) -> dict[str, Any]:
    return {
        "kind": source.kind.value,
        "source_id": source.source_id,
        "version": source.version,
        "locator": source.locator,
        "uri": source.uri,
    }


def _source(value: dict[str, Any]) -> SourceRef:
    return SourceRef(
        SourceKind(value["kind"]),
        value["source_id"],
        value["version"],
        value["locator"],
        value["uri"],
    )


def _ref_dict(ref: FormalRef) -> dict[str, str]:
    return {
        "owner": ref.owner,
        "object_type": ref.object_type,
        "object_id": ref.object_id,
        "version": ref.version,
    }


def _ref(value: dict[str, str]) -> FormalRef:
    return FormalRef(value["owner"], value["object_type"], value["object_id"], value["version"])


def encode_evidence(evidence: Evidence) -> dict[str, Any]:
    withdrawal = evidence.withdrawal
    return {
        "schema_version": _SCHEMA_VERSION,
        "id": evidence.id,
        "scope": {"tenant_id": evidence.scope.tenant_id, "namespace": evidence.scope.namespace},
        "source": _source_dict(evidence.source),
        "content": evidence.content,
        "content_hash": evidence.content_hash,
        "captured_at": _time(evidence.captured_at),
        "revision": evidence.revision,
        "state": evidence.state.value,
        "withdrawal": (
            {
                "actor": withdrawal.actor,
                "reason": withdrawal.reason,
                "recorded_at": _time(withdrawal.recorded_at),
            }
            if withdrawal
            else None
        ),
    }


def decode_evidence(value: dict[str, Any]) -> Evidence:
    _require_schema_version(value, "evidence")
    withdrawal_value = value["withdrawal"]
    withdrawal = (
        EvidenceWithdrawal(
            withdrawal_value["actor"],
            withdrawal_value["reason"],
            _required_time(withdrawal_value["recorded_at"]),
        )
        if withdrawal_value
        else None
    )
    return Evidence(
        id=value["id"],
        scope=Scope(**value["scope"]),
        source=_source(value["source"]),
        content=value["content"],
        content_hash=value["content_hash"],
        captured_at=_required_time(value["captured_at"]),
        revision=value["revision"],
        state=EvidenceState(value["state"]),
        withdrawal=withdrawal,
    )


def encode_candidate(candidate: Candidate) -> dict[str, Any]:
    application = candidate.application
    return {
        "schema_version": _SCHEMA_VERSION,
        "id": candidate.id,
        "scope": {"tenant_id": candidate.scope.tenant_id, "namespace": candidate.scope.namespace},
        "evidence_id": candidate.evidence_id,
        "text": candidate.text,
        "epistemic_kind": candidate.epistemic_kind.value,
        "proposer_id": candidate.proposer_id,
        "extractor_id": candidate.extractor_id,
        "run_id": candidate.run_id,
        "proposed_at": _time(candidate.proposed_at),
        "valid_from": _time(candidate.valid_from),
        "valid_until": _time(candidate.valid_until),
        "revision": candidate.revision,
        "state": candidate.state.value,
        "history": [
            {
                "state": entry.state.value,
                "actor": entry.actor,
                "reason": entry.reason,
                "recorded_at": _time(entry.recorded_at),
            }
            for entry in candidate.history
        ],
        "application": (
            {
                "ref": _ref_dict(application.ref),
                "actor": application.actor,
                "applied_at": _time(application.applied_at),
            }
            if application
            else None
        ),
        "replacement_id": candidate.replacement_id,
    }


def decode_candidate(value: dict[str, Any]) -> Candidate:
    _require_schema_version(value, "candidate")
    application_value = value["application"]
    application = (
        ApplicationReceipt(
            _ref(application_value["ref"]),
            application_value["actor"],
            _required_time(application_value["applied_at"]),
        )
        if application_value
        else None
    )
    history = tuple(
        GovernanceEntry(
            GovernanceState(entry["state"]),
            entry["actor"],
            entry["reason"],
            _required_time(entry["recorded_at"]),
        )
        for entry in value["history"]
    )
    return Candidate(
        id=value["id"],
        scope=Scope(**value["scope"]),
        evidence_id=value["evidence_id"],
        text=value["text"],
        epistemic_kind=EpistemicKind(value["epistemic_kind"]),
        proposer_id=value["proposer_id"],
        extractor_id=value["extractor_id"],
        run_id=value["run_id"],
        proposed_at=_required_time(value["proposed_at"]),
        valid_from=_parse_time(value["valid_from"]),
        valid_until=_parse_time(value["valid_until"]),
        revision=value["revision"],
        state=GovernanceState(value["state"]),
        history=history,
        application=application,
        replacement_id=value["replacement_id"],
    )
