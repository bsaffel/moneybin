"""system_* tools — data status meta-view."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from typing import Annotated, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import Field

from moneybin import error_codes
from moneybin.adapters import system_status_adapters as status_adapters
from moneybin.errors import (
    UserError,
    classify_user_error,
    exception_origin,
)
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.classified_envelope import build_classified_envelope
from moneybin.privacy.payloads.system import (
    AuditDetail,
    AuditEvents,
    AuditHistory,
    CategorizationStatus,
    DoctorStatus,
    ExportsStatus,
    InvariantResultPayload,
    OverviewStatus,
    RecoveryActionPayload,
    SectionUnavailable,
    SystemAuditCoarsePayload,
    SystemAuditEventPayload,
    SystemAuditGetPayload,
    SystemAuditHistoryEntryPayload,
    SystemAuditHistoryPayload,
    SystemAuditUndoPayload,
    SystemDoctorPayload,
    SystemStatusCoarsePayload,
    SystemStatusGsheetRow,
    SystemStatusPayload,
)
from moneybin.privacy.sensitivity import Sensitivity
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.protocol.pagination import (
    KeysetPosition,
    SortDirection,
    canonical_iso_timestamp,
    decode_keyset_cursor,
    encode_keyset_cursor,
    reject_inverted_keyset,
    validate_keyset_shape,
)

logger = logging.getLogger(__name__)

# Display order of both audit views: `ORDER BY occurred_at DESC, <id> DESC`.
_AUDIT_KEY_DIRECTIONS: tuple[SortDirection, ...] = ("desc", "desc")


def _gsheet_action_hints(needs_attention: list[SystemStatusGsheetRow]) -> list[str]:
    """Generate per-row action hints for connections that need attention.

    drift_detected → gsheet_reconnect hint (MCP-invokable). auth_expired →
    CLI re-auth message (the OAuth flow opens a browser, no MCP equivalent).
    Other non-healthy statuses (unreachable, rate_limited) get a generic
    gsheet_status hint pointing at the diagnostic tool.
    """
    hints: list[str] = []
    for row in needs_attention:
        status = row.status
        cid = row.connection_id
        if status == "drift_detected":
            hints.append(
                f"Run gsheet_connect(connection_id='{cid}') to re-detect "
                "the sheet structure and re-pin the column mapping."
            )
        elif status == "auth_expired":
            hints.append(
                "Re-authenticate with gsheet_connect(force_reauth=True), or run "
                "`moneybin gsheet auth` (CLI). Both drive the same "
                "in-process OAuth flow."
            )
        else:
            hints.append(
                f"Run gsheet(view='status', connection_id='{cid}') to inspect the "
                f"failure detail (status={status})."
            )
    return hints


def system_status() -> ResponseEnvelope[SystemStatusPayload]:
    """Return data inventory, pending review queue counts, and transforms freshness.

    Use this tool to understand what data exists in MoneyBin and what
    needs user attention before suggesting any analytical query. The
    ``gsheet`` block summarizes Google Sheets connection health: drift-detected
    connections surface a paired ``gsheet_reconnect`` hint in ``actions[]``.
    The ``build`` block names the package version and source revision this
    server is running: cite it before concluding that any MCP behaviour
    contradicts the code, because a long-running server can predate it.
    """
    from moneybin.config import get_settings
    from moneybin.database import DatabaseLockError, get_database
    from moneybin.services.system_service import SystemService

    # Collect the file-lock / lsof connection view BEFORE opening the DB: it
    # reads the lock file and lsof (no DB connection needed) and must stay
    # reachable when a writer holds the lock — the DatabaseLockError recovery
    # action points here precisely to identify that holder. Opening the DB
    # first would let a read-only open retry-then-fail under contention, so the
    # diagnostic would time out exactly when it is needed.
    db_path = get_settings().database.path
    db_connections = status_adapters.database_connections_info(db_path)

    try:
        # Short max_wait: this is the DatabaseLockError recovery tool, so under
        # contention it must degrade fast rather than burn a third of the 30 s
        # MCP dispatch budget retrying the read. The diagnostic does not need the
        # read to succeed — db_connections is captured before the open and
        # recomputed in the except branch below.
        with get_database(read_only=True, max_wait=2.0) as db:
            status = SystemService(db).status()
            gsheet = status_adapters.gsheet_info(db)
    except DatabaseLockError:
        # Re-snapshot before degrading: a writer can acquire the lock between the
        # preflight snapshot above and this read failing, so the preflight view
        # may predate the writer and report no holder. Recomputing here names the
        # writer that actually caused the lock — exactly what the
        # DatabaseLockError recovery action sends the agent to system_status for.
        # Only database_connections is real in this payload; the inventory is
        # zero-filled and flagged degraded so the agent trusts nothing else.
        return build_envelope(
            data=status_adapters.locked_overview_payload(
                status_adapters.database_connections_info(db_path)
            ),
            degraded=True,
            degraded_reason=(
                "A writer holds the database lock; the data inventory is "
                "unavailable until it releases. See database_connections for "
                "the holder."
            ),
            actions=[
                "Inspect database_connections for the writer holding the lock, "
                "then wait and retry or surface the contention to the user",
            ],
        )

    actions = [
        "Use reviews for per-queue review counts",
        "Use reports(report_id='core:spending_trend') for a spending trend snapshot",
    ]
    if status.schema_drift:
        actions.append(
            "Run refresh_run to rebuild stale models — "
            f"{len(status.schema_drift)} core table(s) drifted"
        )

    if status.transforms_pending:
        # Name the cause that actually fired. "raw imports are newer" is wrong
        # when a model was never built, and the payload already knows which.
        cause = (
            f"{len(status.transforms_missing_models)} registered model(s) are not built"
            if status.transforms_missing_models
            else "raw imports are newer than the last refresh"
        )
        actions.append(f"Run refresh_run to refresh derived tables ({cause})")

    actions.extend(_gsheet_action_hints(gsheet.needs_attention))

    return build_envelope(
        data=status_adapters.overview_payload(status, gsheet, db_connections),
        actions=actions,
    )


def system_doctor(full: bool = False) -> ResponseEnvelope[SystemDoctorPayload]:
    """Run pipeline integrity checks across all SQLMesh named audits.

    Returns pass/fail/warn per invariant plus a transaction count.
    Failing and warning invariants include a ``recovery_actions`` list of
    pre-built, directly-executable tool calls (each with ``tool``,
    ``arguments``, ``rationale``, ``confidence``, ``idempotent``) the
    agent can dispatch to remediate the issue without further reasoning.
    May write SQLMesh state tables on first Context init. Call before
    relying on analytical results to confirm the pipeline is self-consistent.

    Args:
        full: Scan every protected app.* row for audit coverage instead of the
            default sampled, recent-rows-only window. Slower; use for a deep
            integrity sweep.
    """
    from moneybin.database import get_database
    from moneybin.services.doctor_service import DoctorService

    with get_database(read_only=False) as db:
        report = DoctorService(db).run_all(verbose=False, full=full)

    actions: list[str] = []
    if report.failing > 0:
        actions.append(
            "Run moneybin system doctor --verbose for affected transaction IDs"
        )

    return build_envelope(
        data=SystemDoctorPayload(
            passing=report.passing,
            failing=report.failing,
            warning=report.warning,
            skipped=report.skipped,
            transaction_count=report.transaction_count,
            invariants=[
                InvariantResultPayload(
                    name=r.name,
                    status=r.status,
                    detail=r.detail,
                    affected_ids=r.affected_ids,
                    recovery_actions=[
                        RecoveryActionPayload(
                            tool=a.tool,
                            arguments=a.arguments,
                            rationale=a.rationale,
                            confidence=a.confidence,
                            idempotent=a.idempotent,
                        )
                        for a in (r.recovery_actions or [])
                    ],
                )
                for r in report.invariants
            ],
        ),
        actions=actions,
    )


@mcp_tool(read_only=False)
def system_audit_undo(operation_id: str) -> ResponseEnvelope[SystemAuditUndoPayload]:
    """Reverse every app.* mutation in one operation as a unit, keyed on operation_id.

    The undo *consumer* for any audited annotation/correction (notes, tags,
    splits, categories, budgets, rules, merchants, match decisions). Synthesizes
    each row's inverse from its audit before/after image and writes new audit
    rows under a fresh operation id — so this undo is itself undoable (its
    ``undo_operation_id`` is returned).

    Block-don't-cascade: if a *later* operation modified the same rows, this
    refuses with ``undo_cascade_blocked`` and lists the blocker operation ids in
    ``recovery_actions`` (newest first) — undo those first, then retry. Other
    refusals: ``undo_operation_not_found``, ``undo_already_undone``,
    ``recovery_no_path`` (the operation touched a table outside the undoable
    app.* surface, e.g. a manual import — re-import to recover instead), and
    ``undo_value_inadmissible`` (restoring the captured row would write back a
    value the write path no longer admits, e.g. a pre-existing blank category —
    not retryable; recreate the entity with a valid value instead).

    Writes app.audit_log plus the reversed app.* rows; revert this undo by
    calling system_audit_undo again on the returned undo_operation_id.
    """
    from moneybin.database import get_database
    from moneybin.services.undo_service import UndoService

    with get_database(read_only=False) as db:
        result = UndoService(db).undo(operation_id, actor="mcp")
    return build_envelope(
        data=SystemAuditUndoPayload(
            undo_operation_id=result.undo_operation_id,
            undone_operation_id=result.undone_operation_id,
            reversed_row_count=result.reversed_row_count,
            tables=result.tables,
        ),
        actions=[
            "Undo this undo with "
            f"system_audit_undo(operation_id='{result.undo_operation_id}')",
        ],
    )


def system_audit_history(
    domain: str | None = None,
    since: str | None = None,
    actor: str | None = None,
    limit: int = 50,
    include_undone: bool = False,
    snapshot: tuple[str, str] | None = None,
    after: tuple[str, str] | None = None,
) -> ResponseEnvelope[SystemAuditHistoryPayload]:
    """List recent audited operations, newest first — the "I changed my mind" surface.

    Pull-discovery companion to system_audit_undo: enumerate operations even when
    no error preceded the regret. Each entry carries ``can_undo`` and, when
    blocked, ``undo_blocked_by`` (the operation ids to undo first). Operator
    territory — for reviewing and reversing recent agent changes.

    ``domain`` filters to an action family (e.g. ``"tag"`` → any tag.* row).
    ``include_undone`` adds the undo operations themselves; by default they are
    hidden and the originals they reversed appear with ``can_undo=False``.
    """
    from moneybin.database import get_database
    from moneybin.services.undo_service import UndoService

    with get_database(read_only=True) as db:
        operations = UndoService(db).history(
            domain=domain,
            since=since,
            actor=actor,
            limit=limit,
            include_undone=include_undone,
            snapshot=snapshot,
            after=after,
        )
    return build_envelope(
        data=SystemAuditHistoryPayload(
            operations=[
                SystemAuditHistoryEntryPayload(
                    operation_id=o.operation_id,
                    occurred_at=o.occurred_at,
                    actor=o.actor,
                    actions=o.actions,
                    tables=o.tables,
                    row_count=o.row_count,
                    is_undo=o.is_undo,
                    undoes_operation_id=o.undoes_operation_id,
                    can_undo=o.can_undo,
                    undo_blocked_by=o.undo_blocked_by,
                    recovery_actions=[
                        RecoveryActionPayload(
                            tool=ra.tool,
                            arguments=ra.arguments,
                            rationale=ra.rationale,
                            confidence=ra.confidence,
                            idempotent=ra.idempotent,
                        )
                        for ra in o.recovery_actions
                    ],
                )
                for o in operations
            ]
        ),
        actions=[
            "Inspect before/after with system_audit(view='detail', "
            "operation_id=...) before undoing",
            "Reverse an operation with system_audit_undo(operation_id=...)",
        ],
    )


def system_audit_get(operation_id: str) -> ResponseEnvelope[SystemAuditGetPayload]:
    """Full before/after for every row of one operation — inspect before undoing.

    Lets the agent pre-check exactly what system_audit_undo would change.
    ``before_value`` / ``after_value`` can carry financial amounts (high
    sensitivity). ``can_undo`` / ``undo_blocked_by`` mirror the undoability the
    undo tool would enforce. Raises ``undo_operation_not_found`` for a bad id.
    """
    from moneybin.database import get_database
    from moneybin.services.undo_service import UndoService

    with get_database(read_only=True) as db:
        detail = UndoService(db).get(operation_id)
    if detail.can_undo:
        hint = f"Reverse with system_audit_undo(operation_id='{operation_id}')"
    elif detail.undo_blocked_by:
        hint = (
            f"Blocked by later operations — undo those first: {detail.undo_blocked_by}"
        )
    elif detail.unresolvable:
        hint = (
            "Cannot be undone — this operation touched data outside the undoable "
            "app.* surface (e.g. a raw import); re-apply manually."
        )
    else:
        hint = (
            "Cannot be undone as-is — it was already reversed or changed no "
            "reversible rows."
        )
    return build_envelope(
        data=SystemAuditGetPayload(
            operation_id=detail.operation_id,
            events=[
                SystemAuditEventPayload(
                    audit_id=e.audit_id,
                    occurred_at=e.occurred_at,
                    actor=e.actor,
                    action=e.action,
                    target_schema=e.target_schema,
                    target_table=e.target_table,
                    target_id=e.target_id,
                    before_value=e.before_value,
                    after_value=e.after_value,
                    parent_audit_id=e.parent_audit_id,
                    operation_id=e.operation_id,
                    context_json=e.context_json,
                    is_undo=e.is_undo,
                    undoes_operation_id=e.undoes_operation_id,
                )
                for e in detail.events
            ],
            can_undo=detail.can_undo,
            undo_blocked_by=detail.undo_blocked_by,
        ),
        actions=[hint],
    )


def _dynamic_coarse_envelope[T](
    data: T,
    *,
    contract_types: list[type[Any]],
    total_count: int,
    returned_count: int,
    next_cursor: str | None = None,
    actions: list[str] | None = None,
    degraded: bool = False,
    degraded_reason: str | None = None,
) -> ResponseEnvelope[T]:
    """Build a runtime-classified coarse envelope from its selected variants."""
    return build_classified_envelope(
        data,
        contract_type=contract_types,
        total_count=total_count,
        returned_count=returned_count,
        next_cursor=next_cursor,
        actions=actions,
        degraded=degraded,
        degraded_reason=degraded_reason,
    )


async def _run_tool_body[T](
    callback: Callable[..., T],
    /,
    *args: Any,
    **kwargs: Any,
) -> T:
    """Delegate without emitting a second public-tool privacy audit."""
    body = cast(Callable[..., T], inspect.unwrap(callback))
    return await asyncio.to_thread(body, *args, **kwargs)


def _unavailable_section(section: str, exc: Exception) -> SectionUnavailable:
    """Mark one status section unavailable instead of failing the whole call.

    ``_run_tool_body`` unwraps the tool decorator, so a section body raises
    rather than returning an error envelope — this is the path a genuinely
    broken section takes.
    """
    classified = classify_user_error(exc)
    if classified is not None:
        # Carry the hint too: classify_user_error already built the remedy
        # (a locked database says to close the other connection), and dropping
        # it leaves the agent a code with no way forward.
        return SectionUnavailable(
            section=cast(Any, section),
            code=classified.code,
            reason=classified.message,
            hint=classified.hint,
        )
    # Frame chain only — an exception message can embed SQL fragments and
    # financial data, and the log is not exempt from that (see exception_origin).
    logger.error(
        f"system_status section {section} raised {type(exc).__name__} "
        f"at {exception_origin(exc)}"
    )
    return SectionUnavailable(
        section=cast(Any, section),
        code=error_codes.INFRA_UNCLASSIFIED_ERROR,
        reason=f"Section failed with an unhandled {type(exc).__name__}",
    )


def _export_status_section() -> ExportsStatus:
    """Load export readiness synchronously inside one database lifetime."""
    from moneybin.database import get_database
    from moneybin.exports.service import ExportService

    with get_database(read_only=True) as db:
        readiness = ExportService(db).status()
    return status_adapters.exports_status(readiness)


def _audit_event_payload(event: Any) -> SystemAuditEventPayload:
    """Project one existing AuditService row into the classified wire row."""
    return SystemAuditEventPayload(
        audit_id=event.audit_id,
        occurred_at=event.occurred_at,
        actor=event.actor,
        action=event.action,
        target_schema=event.target_schema,
        target_table=event.target_table,
        target_id=event.target_id,
        before_value=event.before_value,
        after_value=event.after_value,
        parent_audit_id=event.parent_audit_id,
        operation_id=event.operation_id,
        context_json=event.context_json,
        is_undo=event.is_undo,
        undoes_operation_id=event.undoes_operation_id,
    )


def _audit_position(
    cursor: str | None,
    view: Literal["events", "history"],
) -> KeysetPosition | None:
    """Decode an audit keyset cursor and reject malformed or cross-view reuse."""
    if cursor is None:
        return None
    try:
        return decode_keyset_cursor(
            cursor,
            namespace="system_audit",
            scope={"view": view},
        )
    except ValueError as exc:
        raise ValueError("invalid audit cursor") from exc


def _audit_bounds(
    position: KeysetPosition | None,
) -> tuple[tuple[str, str] | None, tuple[str, str] | None]:
    """Validate and narrow decoded audit keys to timestamp/id string pairs."""
    if position is None:
        return None, None
    try:
        validate_keyset_shape(position, key_types=(str, str))
        snapshot = _canonical_audit_key(position.snapshot)
        after = _canonical_audit_key(position.after)
        reject_inverted_keyset(
            KeysetPosition(snapshot=snapshot, after=after, total=position.total),
            _AUDIT_KEY_DIRECTIONS,
        )
    except ValueError as exc:
        raise ValueError("invalid audit cursor") from exc
    return (
        snapshot,
        after,
    )


def _canonical_audit_key(key: tuple[object, ...]) -> tuple[str, str]:
    """Return one audit key with its timestamp in canonical space-separated ISO.

    ``datetime.fromisoformat`` accepts both ``2025-06-01T01:00:00`` and
    ``2025-06-01 02:00:00``; lexicographically the space form sorts behind the
    ``T`` form even when it is the later instant, so comparing raw keys would
    let a forged cursor mixing the two defeat the ordering guard. An offset
    would break the same ordering, and every timestamp this log stores is
    naive, so an aware one is refused rather than converted.
    """
    occurred_at, row_id = cast(tuple[str, str], key)
    if not row_id:
        raise ValueError("audit cursor carries an empty id")
    return canonical_iso_timestamp(occurred_at), row_id


def _audit_list_actions(
    view: Literal["events", "history"],
    *,
    limit: int,
    next_cursor: str | None,
) -> list[str]:
    """Return hints that remain valid on the consolidated public surface."""
    actions = [
        "Inspect an operation with system_audit(view='detail', operation_id=...)"
    ]
    if view == "history":
        actions.append("Reverse an operation with system_audit_undo(operation_id=...)")
    if next_cursor is not None:
        actions.append(
            f"Continue with system_audit(view='{view}', limit={limit}, "
            f"cursor='{next_cursor}')"
        )
    return actions


@mcp_tool(
    dynamic_classification=True,
    maximum_sensitivity=Sensitivity.MEDIUM,
    read_only=False,
)
async def system_status_coarse(
    sections: list[Literal["overview", "doctor", "categorization", "exports"]]
    | None = None,
    detail: Literal["summary", "full"] = "summary",
) -> ResponseEnvelope[SystemStatusCoarsePayload]:
    """Return selected system overview, integrity, and categorization sections."""
    from moneybin.mcp.tools.transactions_categorize import (
        transactions_categorize_stats,
    )

    requested = (
        ["overview", "doctor", "categorization", "exports"]
        if sections is None
        else sections
    )
    if not requested:
        raise ValueError("At least one system status section is required.")
    if len(set(requested)) != len(requested):
        raise ValueError("System status sections must not contain duplicates.")

    selected: list[
        OverviewStatus
        | DoctorStatus
        | CategorizationStatus
        | ExportsStatus
        | SectionUnavailable
    ] = []
    actions: list[str] = []
    degraded_reasons: list[str] = []
    degraded = False
    # Declared once: each branch assigns a differently-parameterized envelope,
    # and the payload is narrowed by the section it belongs to.
    response: ResponseEnvelope[Any]
    for section in requested:
        if section not in ("overview", "doctor", "categorization", "exports"):
            raise ValueError("Unknown system status section.")
        # One failing section must not destroy the others: every section is
        # produced independently and an unavailable marker takes its place.
        try:
            if section == "exports":
                selected.append(await _run_tool_body(_export_status_section))
                actions.extend([
                    "Deliver a bundle or report with export_run.",
                    "Configure a named destination with exports_set.",
                ])
                continue
            if section == "overview":
                response = await _run_tool_body(system_status)
            elif section == "doctor":
                response = await _run_tool_body(system_doctor, full=detail == "full")
            else:
                response = await _run_tool_body(
                    transactions_categorize_stats, include_auto=detail == "full"
                )
        except Exception as exc:  # degrade this section, keep the rest
            unavailable = _unavailable_section(section, exc)
            selected.append(unavailable)
            degraded_reasons.append(f"{section}: {unavailable.reason}")
            continue

        if response.error is not None:
            selected.append(
                SectionUnavailable(
                    section=cast(Any, section),
                    code=response.error.code,
                    reason=response.error.message,
                )
            )
            degraded_reasons.append(f"{section}: {response.error.message}")
            continue

        if section == "overview":
            selected.append(OverviewStatus(overview=response.data))
        elif section == "doctor":
            selected.append(DoctorStatus(doctor=response.data))
        else:
            selected.append(CategorizationStatus(statistics=response.data))
        actions.extend(response.actions)
        if response.summary.degraded:
            # Track the flag independently of the reason: a section that
            # degrades without supplying one must not be aggregated away as
            # healthy just because it left the reason blank.
            degraded = True
            reason = response.summary.degraded_reason
            if reason is not None:
                degraded_reasons.append(reason)

    payload = SystemStatusCoarsePayload(sections=selected)
    return _dynamic_coarse_envelope(
        payload,
        contract_types=[type(section) for section in selected],
        total_count=len(selected),
        returned_count=len(selected),
        actions=list(dict.fromkeys(actions)),
        degraded=degraded or bool(degraded_reasons),
        degraded_reason=" ".join(dict.fromkeys(degraded_reasons)) or None,
    )


@mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.HIGH)
async def system_audit_coarse(
    view: Literal["events", "history", "detail"] = "events",
    operation_id: str | None = None,
    audit_id: str | None = None,
    limit: Annotated[int, Field(strict=True, ge=1)] = 50,
    cursor: str | None = None,
) -> ResponseEnvelope[SystemAuditCoarsePayload]:
    """List audit events or operations, or inspect one operation/event chain."""
    if view == "detail":
        if (operation_id is None) == (audit_id is None):
            raise UserError(
                "Audit detail requires exactly one of operation_id or audit_id.",
                code=error_codes.AUDIT_IDENTIFIER_REQUIRED,
            )
        if cursor is not None:
            raise UserError(
                "Audit detail does not accept a cursor.",
                code=error_codes.AUDIT_CURSOR_NOT_ALLOWED,
            )
    elif operation_id is not None or audit_id is not None:
        raise UserError(
            "operation_id and audit_id are only valid for audit detail.",
            code=error_codes.AUDIT_IDENTIFIER_NOT_ALLOWED,
        )

    if view == "events":
        from moneybin.database import get_database
        from moneybin.services.audit_service import AuditService

        position = _audit_position(cursor, "events")
        snapshot, after = _audit_bounds(position)
        with get_database(read_only=True) as db:
            service = AuditService(db)
            events = service.list_events(
                limit=limit + 1,
                snapshot=snapshot,
                after=after,
            )
            page = [_audit_event_payload(event) for event in events[:limit]]
            has_more = len(events) > limit
            snapshot_key = (
                snapshot
                if snapshot is not None
                else ((str(page[0].occurred_at), page[0].audit_id) if page else None)
            )
            total_count = (
                position.total
                if position is not None
                else service.count_events(snapshot=snapshot_key)
            )
        if has_more and page and snapshot_key is not None:
            next_cursor = encode_keyset_cursor(
                namespace="system_audit",
                scope={"view": "events"},
                snapshot=snapshot_key,
                after=(str(page[-1].occurred_at), page[-1].audit_id),
                total=total_count,
            )
        else:
            next_cursor = None
        payload = AuditEvents(events=page)
        return _dynamic_coarse_envelope(
            payload,
            contract_types=[AuditEvents],
            total_count=total_count,
            returned_count=len(page),
            next_cursor=next_cursor,
            actions=_audit_list_actions(
                "events",
                limit=limit,
                next_cursor=next_cursor,
            ),
        )

    if view == "history":
        position = _audit_position(cursor, "history")
        snapshot, after = _audit_bounds(position)
        response = await _run_tool_body(
            system_audit_history,
            limit=limit + 1,
            snapshot=snapshot,
            after=after,
        )
        if response.error is not None:
            return cast(ResponseEnvelope[SystemAuditCoarsePayload], response)
        rows = response.data.operations
        page = rows[:limit]
        has_more = len(rows) > limit
        snapshot_key = (
            snapshot
            if snapshot is not None
            else ((str(page[0].occurred_at), page[0].operation_id) if page else None)
        )
        if position is not None:
            total_count = position.total
        else:
            from moneybin.database import get_database
            from moneybin.services.undo_service import UndoService

            with get_database(read_only=True) as db:
                total_count = UndoService(db).history_count(snapshot=snapshot_key)
        if has_more and page and snapshot_key is not None:
            next_cursor = encode_keyset_cursor(
                namespace="system_audit",
                scope={"view": "history"},
                snapshot=snapshot_key,
                after=(str(page[-1].occurred_at), page[-1].operation_id),
                total=total_count,
            )
        else:
            next_cursor = None
        payload = AuditHistory(operations=page)
        return _dynamic_coarse_envelope(
            payload,
            contract_types=[AuditHistory],
            total_count=total_count,
            returned_count=len(page),
            next_cursor=next_cursor,
            actions=_audit_list_actions(
                "history",
                limit=limit,
                next_cursor=next_cursor,
            ),
        )

    if operation_id is not None:
        response = await _run_tool_body(system_audit_get, operation_id)
        if response.error is not None:
            return cast(ResponseEnvelope[SystemAuditCoarsePayload], response)
        payload = AuditDetail(
            operation_id=operation_id,
            audit_id=None,
            events=response.data.events,
            can_undo=response.data.can_undo,
            undo_blocked_by=response.data.undo_blocked_by,
        )
        return _dynamic_coarse_envelope(
            payload,
            contract_types=[AuditDetail],
            total_count=len(payload.events),
            returned_count=len(payload.events),
            actions=response.actions,
        )

    from moneybin.database import get_database
    from moneybin.services.audit_service import AuditService

    audit_id_value = cast(str, audit_id)
    with get_database(read_only=True) as db:
        events = AuditService(db).chain_for(audit_id_value)
    if not events:
        raise UserError(
            "No audit event found for the supplied audit_id.",
            code=error_codes.AUDIT_IDENTIFIER_NOT_FOUND,
        )
    payload = AuditDetail(
        operation_id=None,
        audit_id=audit_id_value,
        events=[_audit_event_payload(event) for event in events],
        can_undo=None,
        undo_blocked_by=None,
    )
    return _dynamic_coarse_envelope(
        payload,
        contract_types=[AuditDetail],
        total_count=len(payload.events),
        returned_count=len(payload.events),
        actions=[
            "Use the event operation_id with system_audit(view='detail', "
            "operation_id=...) to inspect undoability."
        ],
    )


def register_system_coarse_reads(mcp: FastMCP) -> None:
    """Register the standard system reads."""
    register(
        mcp,
        system_status_coarse,
        "system_status",
        "Return selected operator status sections. Sections cover overview "
        "inventory, integrity doctor checks, categorization coverage, and export "
        "readiness. "
        "detail='full' deepens the "
        "doctor scan and includes auto-categorization health. "
        "overview.build names the version and commit this server is running; "
        "cite it before concluding that live behavior contradicts the code, "
        "because a long-lived process can predate the checkout.",
        privacy_actor="system_status",
    )
    register(
        mcp,
        system_audit_coarse,
        "system_audit",
        "List recent audit events or operation history, or inspect one operation "
        "or parent audit event in detail. Detail requires exactly one identifier.",
        privacy_actor="system_audit",
    )


def register_system_tools(mcp: FastMCP) -> None:
    """Register the standard system orientation and recovery boundaries."""
    register_system_coarse_reads(mcp)
    register(
        mcp,
        system_audit_undo,
        "system_audit_undo",
        "Undo one complete audited operation by operation_id. The inverse "
        "mutation is itself audited and undoable; dependency conflicts return "
        "the blocking operation IDs.",
    )


# Internal granular helpers retained for standard-boundary composition and
# parity. They are never individually registered; the surface-budget guard
# derives the complete required inventory from the replaced public cohorts.
_LEGACY_INTERNAL_CALLBACKS = (
    system_status,
    system_doctor,
    system_audit_history,
    system_audit_get,
)
