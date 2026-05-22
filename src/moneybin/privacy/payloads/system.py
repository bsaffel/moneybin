"""Typed payload dataclasses for transform, system, and refresh surfaces.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

These surfaces are all operator-territory (low-sensitivity infrastructure
data only): model states, audit check results, pipeline counts, durations.
No PII or financial amounts appear in any of these payloads.

Tier derivation summary:
  All payloads in this module derive Tier.LOW — every field maps to
  TXN_TYPE, AGGREGATE, TIMESTAMP_OBSERVABILITY, RECORD_ID, or DESCRIPTION.
  DESCRIPTION is Tier.MEDIUM, but is used only for error/detail strings
  (audit failure messages, validation error messages). Payloads that
  include a DESCRIPTION field derive Tier.MEDIUM.

  - ``TransformStatusPayload``      → Tier.LOW (RECORD_ID + TXN_TYPE + TIMESTAMP_OBSERVABILITY)
  - ``TransformPlanPayload``        → Tier.LOW (model name lists = RECORD_ID, bool = TXN_TYPE)
  - ``TransformValidationError``    → Tier.MEDIUM (message = DESCRIPTION)
  - ``TransformValidatePayload``    → Tier.MEDIUM (via TransformValidationError)
  - ``TransformAuditRow``           → Tier.MEDIUM (detail = DESCRIPTION)
  - ``TransformAuditPayload``       → Tier.MEDIUM (via TransformAuditRow)
  - ``SystemStatusAccountsInfo``    → Tier.LOW (AGGREGATE only)
  - ``SystemStatusTransactionsInfo``→ Tier.LOW (AGGREGATE + TIMESTAMP_OBSERVABILITY only)
  - ``SchemaDriftTable``            → Tier.LOW (RECORD_ID + TXN_TYPE)
  - ``SystemStatusPayload``         → Tier.LOW (no DESCRIPTION fields)
  - ``InvariantResultPayload``      → Tier.MEDIUM (detail = DESCRIPTION, affected_ids = RECORD_ID)
  - ``SystemDoctorPayload``         → Tier.MEDIUM (via InvariantResultPayload)
  - ``RefreshRunPayload``           → Tier.MEDIUM (error = DESCRIPTION)
  - ``SystemAuditEventPayload``     → Tier.HIGH (before_value, after_value = TXN_AMOUNT)
  - ``SystemAuditPayload``          → Tier.HIGH (via SystemAuditEventPayload)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

from moneybin.privacy.taxonomy import DataClass

# ---------------------------------------------------------------------------
# transform_status payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TransformStatusPayload:
    """Payload for ``transform_status`` — SQLMesh environment + freshness."""

    environment: Annotated[str, DataClass.RECORD_ID]
    initialized: Annotated[bool, DataClass.TXN_TYPE]
    last_apply_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    pending: Annotated[bool, DataClass.TXN_TYPE]
    latest_import_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]


# ---------------------------------------------------------------------------
# transform_plan payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TransformPlanPayload:
    """Payload for ``transform_plan`` — pending SQLMesh model change sets."""

    has_changes: Annotated[bool, DataClass.TXN_TYPE]
    directly_modified: Annotated[list[str], DataClass.RECORD_ID]
    indirectly_modified: Annotated[list[str], DataClass.RECORD_ID]
    added: Annotated[list[str], DataClass.RECORD_ID]
    removed: Annotated[list[str], DataClass.RECORD_ID]


# ---------------------------------------------------------------------------
# transform_validate payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TransformValidationError:
    """One parse/resolve error from ``transform_validate``."""

    model: Annotated[str, DataClass.RECORD_ID]
    message: Annotated[str, DataClass.DESCRIPTION]


@dataclass(frozen=True, slots=True)
class TransformValidatePayload:
    """Payload for ``transform_validate`` — parse/resolve check result."""

    valid: Annotated[bool, DataClass.TXN_TYPE]
    errors: list[TransformValidationError]


# ---------------------------------------------------------------------------
# transform_audit payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TransformAuditRow:
    """One per-audit result row from ``transform_audit``."""

    name: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    detail: Annotated[str | None, DataClass.DESCRIPTION]


@dataclass(frozen=True, slots=True)
class TransformAuditPayload:
    """Payload for ``transform_audit`` — SQLMesh data-quality audit results."""

    passed: Annotated[int, DataClass.AGGREGATE]
    failed: Annotated[int, DataClass.AGGREGATE]
    audits: list[TransformAuditRow]


# ---------------------------------------------------------------------------
# system_status payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SystemStatusAccountsInfo:
    """Account count sub-object inside SystemStatusPayload."""

    count: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class SystemStatusTransactionsInfo:
    """Transaction count + range sub-object inside SystemStatusPayload."""

    count: Annotated[int, DataClass.AGGREGATE]
    # AGGREGATE not TXN_DATE: this is min/max of every transaction date — a
    # 2-element summary of the dataset's span, not individual transaction dates.
    date_range: Annotated[list[str | None], DataClass.AGGREGATE]
    last_import_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]


@dataclass(frozen=True, slots=True)
class SystemStatusMatchesInfo:
    """Match queue sub-object inside SystemStatusPayload."""

    pending_review: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class SystemStatusCategorizationInfo:
    """Categorization queue sub-object inside SystemStatusPayload."""

    uncategorized: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class SystemStatusTransformsInfo:
    """Transform freshness sub-object inside SystemStatusPayload."""

    pending: Annotated[bool, DataClass.TXN_TYPE]
    last_apply_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]


@dataclass(frozen=True, slots=True)
class SchemaDriftTable:
    """One drifted table entry inside SystemStatusPayload.schema_drift."""

    name: Annotated[str, DataClass.RECORD_ID]
    missing_columns: Annotated[list[str], DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class SystemStatusSchemaDrift:
    """Schema drift info inside SystemStatusPayload, present only when drift detected."""

    tables: list[SchemaDriftTable]
    remediation: Annotated[str, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class SystemStatusGsheetRow:
    """One Google Sheets connection needing attention inside the gsheet block."""

    connection_id: Annotated[str, DataClass.RECORD_ID]
    # workbook_name / sheet_name are user-chosen labels identifying the connected
    # source — treated like an institution/source name (LOW), not financial PII,
    # matching the app.gsheet_connections registry block. The sheet's *contents*
    # never appear here, only the connection's metadata.
    workbook_name: Annotated[str | None, DataClass.INSTITUTION]
    sheet_name: Annotated[str | None, DataClass.INSTITUTION]
    status: Annotated[str, DataClass.TXN_TYPE]
    reason: Annotated[str | None, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class SystemStatusGsheetInfo:
    """Google Sheets connection-health sub-object inside SystemStatusPayload."""

    total_connections: Annotated[int, DataClass.AGGREGATE]
    by_status: Annotated[dict[str, int], DataClass.AGGREGATE]
    needs_attention: list[SystemStatusGsheetRow]


@dataclass(frozen=True, slots=True)
class SystemStatusPayload:
    """Payload for ``system_status`` — data inventory snapshot."""

    accounts: SystemStatusAccountsInfo
    transactions: SystemStatusTransactionsInfo
    matches: SystemStatusMatchesInfo
    categorization: SystemStatusCategorizationInfo
    transforms: SystemStatusTransformsInfo
    schema_drift: SystemStatusSchemaDrift | None
    gsheet: SystemStatusGsheetInfo


# ---------------------------------------------------------------------------
# system_doctor payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InvariantResultPayload:
    """One pipeline invariant check result inside SystemDoctorPayload."""

    name: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    detail: Annotated[str | None, DataClass.DESCRIPTION]
    affected_ids: Annotated[list[str], DataClass.RECORD_ID]


@dataclass(frozen=True, slots=True)
class SystemDoctorPayload:
    """Payload for ``system_doctor`` — pipeline integrity check results."""

    passing: Annotated[int, DataClass.AGGREGATE]
    failing: Annotated[int, DataClass.AGGREGATE]
    warning: Annotated[int, DataClass.AGGREGATE]
    skipped: Annotated[int, DataClass.AGGREGATE]
    transaction_count: Annotated[int, DataClass.AGGREGATE]
    invariants: list[InvariantResultPayload]


# ---------------------------------------------------------------------------
# refresh_run payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RefreshRunPayload:
    """Payload for ``refresh_run`` — pipeline execution result.

    ``error`` is DESCRIPTION (Tier.MEDIUM): SQLMesh error type names are
    non-PII but we conservatively classify them as DESCRIPTION since
    error strings in adjacent tooling sometimes embed model paths.
    """

    applied: Annotated[bool, DataClass.TXN_TYPE]
    duration_seconds: Annotated[float | None, DataClass.AGGREGATE]
    error: Annotated[str | None, DataClass.DESCRIPTION]


# ---------------------------------------------------------------------------
# system_audit payload (batch 4 carryover → typed in this batch)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SystemAuditEventPayload:
    """One audit log event row inside SystemAuditPayload.

    ``before_value`` and ``after_value`` are TXN_AMOUNT (Tier.HIGH) per
    taxonomy.py ("app", "audit_log") block — audit deltas can carry
    financial values (e.g. a budget monthly_amount change).
    """

    audit_id: Annotated[str, DataClass.RECORD_ID]
    occurred_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    actor: Annotated[str, DataClass.TXN_TYPE]
    action: Annotated[str, DataClass.TXN_TYPE]
    target_schema: Annotated[str | None, DataClass.RECORD_ID]
    target_table: Annotated[str | None, DataClass.RECORD_ID]
    target_id: Annotated[str | None, DataClass.RECORD_ID]
    before_value: Annotated[Any, DataClass.TXN_AMOUNT]
    after_value: Annotated[Any, DataClass.TXN_AMOUNT]
    parent_audit_id: Annotated[str | None, DataClass.RECORD_ID]
    context_json: Annotated[Any, DataClass.DESCRIPTION]


@dataclass(frozen=True, slots=True)
class SystemAuditPayload:
    """Payload for ``system_audit`` — filtered audit log events."""

    events: list[SystemAuditEventPayload]
