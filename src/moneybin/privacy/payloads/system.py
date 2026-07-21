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
  - ``SystemStatusWriter``          → Tier.LOW (RECORD_ID + TXN_TYPE + TIMESTAMP_OBSERVABILITY)
  - ``SystemStatusReader``          → Tier.LOW (RECORD_ID + TXN_TYPE)
  - ``SystemStatusDatabaseConnectionsInfo`` → Tier.LOW (composition only)
  - ``SystemStatusPayload``         → Tier.LOW (no DESCRIPTION fields)
  - ``InvariantResultPayload``      → Tier.MEDIUM (detail = DESCRIPTION, affected_ids = RECORD_ID)
  - ``SystemDoctorPayload``         → Tier.MEDIUM (via InvariantResultPayload)
  - ``RefreshRunPayload``           → Tier.MEDIUM (error = DESCRIPTION)
  - ``SystemAuditEventPayload``     → Tier.HIGH (before_value, after_value = TXN_AMOUNT)
  - ``SystemAuditPayload``          → Tier.HIGH (via SystemAuditEventPayload)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from moneybin.privacy.payloads.categorize import (
    CategorizeStatsPayload,
    CategorizeStatsWithAutoPayload,
)
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
class SystemStatusAccountLinksInfo:
    """Account-link review queue sub-object inside SystemStatusPayload."""

    pending_review: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class SystemStatusMerchantLinksInfo:
    """Merchant-link review queue sub-object inside SystemStatusPayload."""

    pending_review: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class SystemStatusSecurityLinksInfo:
    """Security-link review queue sub-object inside SystemStatusPayload."""

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
class SystemStatusWriter:
    """One active writer holding the per-profile write critical-section file lock.

    Populated from the lock-file metadata payload written by
    ``moneybin.db_lock.lock.write_lock``. At most one writer is present at a
    time — the file lock is exclusive.
    """

    pid: Annotated[int, DataClass.RECORD_ID]
    command: Annotated[str, DataClass.TXN_TYPE]
    started_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    operation_type: Annotated[str, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class SystemStatusReader:
    """One concurrent reader process identified via lsof + ps enumeration."""

    pid: Annotated[int, DataClass.RECORD_ID]
    command: Annotated[str, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class SystemStatusDatabaseConnectionsInfo:
    """Per-profile inventory of active database connections.

    Writers come from the file-lock metadata (rich: ``started_at``,
    ``operation_type``). Readers come from lsof + ps (best-effort; pid +
    cmdline only). At most one writer is present at a time — the file lock
    is exclusive.
    """

    writers: list[SystemStatusWriter]
    readers: list[SystemStatusReader]


@dataclass(frozen=True, slots=True)
class SystemStatusPayload:
    """Payload for ``system_status`` — data inventory snapshot."""

    accounts: SystemStatusAccountsInfo
    transactions: SystemStatusTransactionsInfo
    matches: SystemStatusMatchesInfo
    account_links: SystemStatusAccountLinksInfo
    merchant_links: SystemStatusMerchantLinksInfo
    security_links: SystemStatusSecurityLinksInfo
    categorization: SystemStatusCategorizationInfo
    transforms: SystemStatusTransformsInfo
    schema_drift: SystemStatusSchemaDrift | None
    gsheet: SystemStatusGsheetInfo
    # Always populated — system_status reports it on both the normal path and
    # the degraded "database locked" path (it reads the lock file + lsof, no DB
    # connection required).
    database_connections: SystemStatusDatabaseConnectionsInfo


# ---------------------------------------------------------------------------
# system_doctor payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InvariantResultPayload:
    """One pipeline invariant check result inside SystemDoctorPayload.

    ``recovery_actions`` carries the doctor-recipe-produced
    :class:`RecoveryActionPayload` list for failing/warning invariants — pass
    and skipped invariants always carry an empty list. Mirrors the precedent
    set by :class:`SystemAuditHistoryEntryPayload.recovery_actions`.
    """

    name: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    detail: Annotated[str | None, DataClass.DESCRIPTION]
    affected_ids: Annotated[list[str], DataClass.RECORD_ID]
    recovery_actions: list[RecoveryActionPayload]


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
# dormant coarse system_status payload
# ---------------------------------------------------------------------------


class OverviewStatus(BaseModel):
    """The existing data-inventory projection inside sectioned system status."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["overview"] = "overview"
    overview: SystemStatusPayload


class DoctorStatus(BaseModel):
    """The existing integrity-check projection inside sectioned system status."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["doctor"] = "doctor"
    doctor: SystemDoctorPayload


class CategorizationStatus(BaseModel):
    """The existing categorization statistics inside sectioned system status."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["categorization"] = "categorization"
    statistics: CategorizeStatsPayload | CategorizeStatsWithAutoPayload


class SystemStatusExportDestination(BaseModel):
    """Privacy-safe readiness for one configured export destination."""

    model_config = ConfigDict(frozen=True)

    name: Annotated[str, DataClass.USER_NOTE]
    kind: Annotated[Literal["local", "sheets"], DataClass.TXN_TYPE]
    ready: Annotated[bool, DataClass.TXN_TYPE]
    write_capable: Annotated[bool, DataClass.TXN_TYPE]
    reasons: Annotated[list[str], DataClass.TXN_TYPE]


class ExportsStatus(BaseModel):
    """Export destination readiness inside sectioned system status."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["exports"] = "exports"
    destinations: list[SystemStatusExportDestination]


@dataclass(frozen=True, slots=True)
class SystemStatusCLIPayload:
    """Flat typed payload for the established ``system status`` CLI JSON shape."""

    accounts_count: Annotated[int, DataClass.AGGREGATE]
    transactions_count: Annotated[int, DataClass.AGGREGATE]
    transactions_date_range: Annotated[list[str | None], DataClass.AGGREGATE]
    last_import_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    matches_pending: Annotated[int, DataClass.AGGREGATE]
    categorize_pending: Annotated[int, DataClass.AGGREGATE]
    exports: list[SystemStatusExportDestination]


SystemStatusSection = Annotated[
    OverviewStatus | DoctorStatus | CategorizationStatus | ExportsStatus,
    Field(discriminator="kind"),
]


class SystemStatusCoarsePayload(BaseModel):
    """Selected status sections in deterministic request order."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["sections"] = "sections"
    sections: list[SystemStatusSection]


# ---------------------------------------------------------------------------
# refresh_run payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SelfHealActionRow:
    """One self-heal recipe execution inside RefreshRunPayload.

    Carrier ships ahead of the self-heal safelist (M2D PR 7); the list is
    empty until that lands. All fields are operational metadata (Tier.LOW).
    """

    recipe_id: Annotated[str, DataClass.RECORD_ID]
    rows_affected: Annotated[int, DataClass.AGGREGATE]
    operation_id: Annotated[str, DataClass.RECORD_ID]
    timestamp: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]


@dataclass(frozen=True, slots=True)
class RefreshRunPayload:
    """Payload for ``refresh_run`` — pipeline execution result.

    The ``*_error`` fields are DESCRIPTION (Tier.MEDIUM): SQLMesh / step error
    type names are non-PII but we conservatively classify them as DESCRIPTION
    since error strings in adjacent tooling sometimes embed model paths.
    ``identity_errors`` contains only fixed domain labels and is therefore
    TXN_TYPE (Tier.LOW). These fields are emitted as stable keys so agents see a
    consistent shape — matching the ``self_heal_actions`` stable-key intent.
    """

    applied: Annotated[bool, DataClass.TXN_TYPE]
    duration_seconds: Annotated[float | None, DataClass.AGGREGATE]
    error: Annotated[str | None, DataClass.DESCRIPTION]
    matching_error: Annotated[str | None, DataClass.DESCRIPTION]
    categorization_error: Annotated[str | None, DataClass.DESCRIPTION]
    identity_errors: Annotated[list[str], DataClass.TXN_TYPE]
    self_heal_actions: list[SelfHealActionRow]


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
    operation_id: Annotated[str, DataClass.RECORD_ID]
    context_json: Annotated[Any, DataClass.DESCRIPTION]
    is_undo: Annotated[bool, DataClass.TXN_TYPE]
    undoes_operation_id: Annotated[str | None, DataClass.RECORD_ID]


@dataclass(frozen=True, slots=True)
class SystemAuditPayload:
    """Payload for ``system_audit`` — filtered audit log events."""

    events: list[SystemAuditEventPayload]


# ---------------------------------------------------------------------------
# system_audit_undo / _history / _get payloads (REC-PR3)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SystemAuditUndoPayload:
    """Payload for ``system_audit_undo`` — outcome of reversing one operation.

    Carries no financial values (only ids + counts), so it derives Tier.LOW even
    though the rows it reversed may have. The fresh ``undo_operation_id`` lets the
    agent undo the undo.
    """

    undo_operation_id: Annotated[str, DataClass.RECORD_ID]
    undone_operation_id: Annotated[str, DataClass.RECORD_ID]
    reversed_row_count: Annotated[int, DataClass.AGGREGATE]
    tables: Annotated[list[str], DataClass.RECORD_ID]


@dataclass(frozen=True, slots=True)
class RecoveryActionPayload:
    """A structured next-step action mirroring :class:`moneybin.errors.RecoveryAction`.

    Lets a read surface carry the same pre-built ``tool(**arguments)`` the error
    envelope carries — all low-sensitivity (a tool name, ids, and prose), so it
    never raises the enclosing payload's tier.
    """

    tool: Annotated[str, DataClass.RECORD_ID]
    arguments: Annotated[dict[str, Any], DataClass.RECORD_ID]
    rationale: Annotated[str, DataClass.DESCRIPTION]
    # AGGREGATE (not TXN_TYPE) only to match the name-based privacy registry, which
    # already classifies the `confidence` column (match-proposal score). This field
    # is the action-confidence literal ("certain"/"suggested") — a different concept
    # sharing the name — but both classes are Tier.LOW, so the wire key stays
    # `confidence` (consistent with the error envelope's recovery_actions).
    confidence: Annotated[str, DataClass.AGGREGATE]
    idempotent: Annotated[bool, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class SystemAuditHistoryEntryPayload:
    """One operation in ``system_audit_history``, grouped by ``operation_id``.

    Undoability is expressed structurally via ``can_undo`` + ``undo_blocked_by``
    (the blocker operation ids the agent must undo first); ``recovery_actions``
    carries the pre-built undo call(s) for this operation's state (undo it, undo
    the blockers, or undo the undo). No financial values appear, so the entry
    derives Tier.LOW.
    """

    operation_id: Annotated[str, DataClass.RECORD_ID]
    occurred_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    actor: Annotated[str, DataClass.TXN_TYPE]
    actions: Annotated[list[str], DataClass.TXN_TYPE]
    tables: Annotated[list[str], DataClass.RECORD_ID]
    row_count: Annotated[int, DataClass.AGGREGATE]
    is_undo: Annotated[bool, DataClass.TXN_TYPE]
    undoes_operation_id: Annotated[str | None, DataClass.RECORD_ID]
    can_undo: Annotated[bool, DataClass.TXN_TYPE]
    undo_blocked_by: Annotated[list[str] | None, DataClass.RECORD_ID]
    recovery_actions: list[RecoveryActionPayload]


@dataclass(frozen=True, slots=True)
class SystemAuditHistoryPayload:
    """Payload for ``system_audit_history`` — recent operations, newest first."""

    operations: list[SystemAuditHistoryEntryPayload]


@dataclass(frozen=True, slots=True)
class SystemAuditGetPayload:
    """Payload for ``system_audit_get`` — full before/after for one operation.

    Reuses :class:`SystemAuditEventPayload`, whose ``before_value`` /
    ``after_value`` are TXN_AMOUNT, so this payload derives the same high tier as
    ``system_audit`` — the agent can pre-check exactly what an undo would change.
    """

    operation_id: Annotated[str, DataClass.RECORD_ID]
    events: list[SystemAuditEventPayload]
    can_undo: Annotated[bool, DataClass.TXN_TYPE]
    undo_blocked_by: Annotated[list[str] | None, DataClass.RECORD_ID]


# ---------------------------------------------------------------------------
# dormant coarse system_audit payload
# ---------------------------------------------------------------------------


class AuditEvents(BaseModel):
    """Recent audit events."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["events"] = "events"
    events: list[SystemAuditEventPayload]


class AuditHistory(BaseModel):
    """Recent audited operations with undoability metadata."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["history"] = "history"
    operations: list[SystemAuditHistoryEntryPayload]


class AuditDetail(BaseModel):
    """One operation or one parent audit event and its child chain."""

    model_config = ConfigDict(frozen=True)

    kind: Literal["detail"] = "detail"
    operation_id: Annotated[str | None, DataClass.RECORD_ID]
    audit_id: Annotated[str | None, DataClass.RECORD_ID]
    events: list[SystemAuditEventPayload]
    can_undo: Annotated[bool | None, DataClass.TXN_TYPE]
    undo_blocked_by: Annotated[list[str] | None, DataClass.RECORD_ID]


SystemAuditCoarsePayload = Annotated[
    AuditEvents | AuditHistory | AuditDetail,
    Field(discriminator="kind"),
]
