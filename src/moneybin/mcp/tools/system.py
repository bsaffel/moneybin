"""system_* tools — data status meta-view."""

from __future__ import annotations

import fcntl  # POSIX-only: project targets macOS/Linux
import json
import os
from pathlib import Path
from typing import Any

import duckdb
from fastmcp import FastMCP

from moneybin.db_lock import lock_path_for
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.system import (
    InvariantResultPayload,
    RecoveryActionPayload,
    SchemaDriftTable,
    SystemAuditEventPayload,
    SystemAuditGetPayload,
    SystemAuditHistoryEntryPayload,
    SystemAuditHistoryPayload,
    SystemAuditUndoPayload,
    SystemDoctorPayload,
    SystemStatusAccountsInfo,
    SystemStatusCategorizationInfo,
    SystemStatusDatabaseConnectionsInfo,
    SystemStatusGsheetInfo,
    SystemStatusGsheetRow,
    SystemStatusMatchesInfo,
    SystemStatusPayload,
    SystemStatusReader,
    SystemStatusSchemaDrift,
    SystemStatusTransactionsInfo,
    SystemStatusTransformsInfo,
    SystemStatusWriter,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.utils.db_processes import describe_process, find_blocking_processes

_HEALTHY_STATUSES = frozenset({"healthy"})
_DISCONNECTED_STATUSES = frozenset({"disconnected"})


def _gsheet_block(db: Any) -> dict[str, Any]:
    """Build the gsheet block: counts by status + per-attention rows.

    Returns the zero-connections shape when the table is empty or absent;
    healthy and disconnected connections are excluded from ``needs_attention``.
    """
    try:
        rows = db.execute(
            """
            SELECT connection_id, workbook_name, sheet_name, status, last_status_reason
            FROM app.gsheet_connections
            ORDER BY created_at ASC, connection_id ASC
            """
        ).fetchall()
    except duckdb.CatalogException:
        # Table absent on bare DBs before init_schemas — report empty rather
        # than error. Narrowed from a blanket except so real DB/query problems
        # (corruption, permission, a broken schema) surface instead of being
        # masked as total_connections=0 and suppressing recovery hints.
        return {"total_connections": 0, "by_status": {}, "needs_attention": []}

    by_status: dict[str, int] = {}
    needs_attention: list[dict[str, Any]] = []
    for connection_id, workbook, sheet, status, drift_reason in rows:
        by_status[status] = by_status.get(status, 0) + 1
        if status in _HEALTHY_STATUSES or status in _DISCONNECTED_STATUSES:
            continue
        needs_attention.append({
            "connection_id": connection_id,
            "workbook_name": workbook,
            "sheet_name": sheet,
            "status": status,
            "reason": drift_reason,
        })

    return {
        "total_connections": len(rows),
        "by_status": by_status,
        "needs_attention": needs_attention,
    }


def _gsheet_action_hints(needs_attention: list[dict[str, Any]]) -> list[str]:
    """Generate per-row action hints for connections that need attention.

    drift_detected → gsheet_reconnect hint (MCP-invokable). auth_expired →
    CLI re-auth message (the OAuth flow opens a browser, no MCP equivalent).
    Other non-healthy statuses (unreachable, rate_limited) get a generic
    gsheet_status hint pointing at the diagnostic tool.
    """
    hints: list[str] = []
    for row in needs_attention:
        status = row["status"]
        cid = row["connection_id"]
        if status == "drift_detected":
            hints.append(
                f"Run gsheet_reconnect(connection_id='{cid}') to re-detect "
                "the sheet structure and re-pin the column mapping."
            )
        elif status == "auth_expired":
            hints.append(
                "Re-authenticate: call gsheet_auth() (MCP) or run "
                "`moneybin gsheet auth` (CLI). Both drive the same "
                "in-process OAuth flow."
            )
        else:
            hints.append(
                f"Run gsheet_status(connection_id='{cid}') to inspect the "
                f"failure detail (status={status})."
            )
    return hints


def _writer_is_live(lock_path: Path) -> bool:
    """Return True iff a process currently holds the write lock.

    The ``.write.lock`` metadata file is never unlinked — unlinking races
    with the next opener, and ``fcntl`` auto-releases on crash — so the file
    persists after a clean release carrying the *last* holder's pid, which may
    still be a live process that is no longer writing. The mere existence of
    the file (or a live pid in it) therefore does NOT mean a writer is active.

    The only authoritative test is to try the lock ourselves, non-blocking. A
    shared (``LOCK_SH``) probe is used rather than exclusive so two concurrent
    ``system_status`` / ``db ps`` probes don't block each other and misreport a
    peer prober as a writer; ``LOCK_SH`` still conflicts with a writer's
    ``LOCK_EX``.
    """
    try:
        fd = os.open(lock_path, os.O_RDONLY)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return True  # a writer holds LOCK_EX
        # Acquired — nobody holds an exclusive lock; release immediately.
        fcntl.flock(fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(fd)


def _database_connections_block(db_path: Path) -> dict[str, Any]:
    """Merge file-lock writer metadata with lsof-derived reader enumeration.

    Returns the empty-shape ``{"writers": [], "readers": []}`` when neither
    source reports anything. A writer is reported only when a process actually
    holds the file lock (``_writer_is_live``) — the persisted metadata file
    alone is not enough, since it outlives the holder. Tolerates a corrupted
    lock file by treating it as no-writer-info — the lock-file payload is
    best-effort observability, not a correctness contract. The writer's pid is
    filtered out of the reader list to avoid double-listing the writer process.
    """
    writers: list[dict[str, Any]] = []
    writer_pid: int | None = None
    # Resolve once and use the resolved path for BOTH the lock file (via
    # lock_path_for, which resolves the same way write_lock keys it) and the
    # lsof reader scan, so a symlinked or relative path can't report writers
    # and readers against different inodes.
    resolved = db_path.resolve()
    lock_path = lock_path_for(resolved)
    # No separate exists() check: _writer_is_live opens the lock file and
    # returns False if it is absent, so an exists() guard would be a redundant,
    # TOCTOU-prone stat.
    if _writer_is_live(lock_path):
        try:
            metadata = json.loads(lock_path.read_text(encoding="utf-8"))
            writer_pid = int(metadata["pid"])
            writers.append({
                "pid": writer_pid,
                # The writer command is already a sanitized friendly name —
                # write_lock runs it through describe_process before storing it,
                # so the on-disk lock file never holds a raw argv. Pass it
                # through as-is (re-sanitizing would mangle the friendly name).
                "command": str(metadata["command"]),
                "started_at": str(metadata["started_at"]),
                "operation_type": str(metadata["operation_type"]),
            })
        except (OSError, ValueError, KeyError, TypeError):
            # Corrupted metadata, partial write, or non-dict JSON (null, list,
            # scalar — which makes metadata["pid"] raise TypeError) — treat as
            # no writer.
            writers = []
            writer_pid = None

    readers: list[dict[str, Any]] = []
    for proc in find_blocking_processes(resolved):
        if writer_pid is not None and proc["pid"] == writer_pid:
            continue  # Avoid double-listing the writer as a reader
        readers.append({
            "pid": int(proc["pid"]),
            "command": describe_process(
                str(proc.get("cmdline") or proc.get("command", ""))
            ),
        })

    return {"writers": writers, "readers": readers}


def _locked_status_envelope(
    db_connections: SystemStatusDatabaseConnectionsInfo,
) -> ResponseEnvelope[SystemStatusPayload]:
    """Degraded ``system_status`` for when a writer holds the database lock.

    A read-only open cannot attach while a writer holds the lock, so the data
    inventory is unavailable — but ``database_connections`` (read from the lock
    file + lsof, no DB needed) still names the holder, which is exactly what the
    ``DatabaseLockError`` recovery action needs. The inventory fields are
    zero-filled and the envelope is flagged ``degraded`` so the agent trusts
    only ``database_connections``.
    """
    return build_envelope(
        data=SystemStatusPayload(
            accounts=SystemStatusAccountsInfo(count=0),
            transactions=SystemStatusTransactionsInfo(
                count=0, date_range=[None, None], last_import_at=None
            ),
            matches=SystemStatusMatchesInfo(pending_review=0),
            categorization=SystemStatusCategorizationInfo(uncategorized=0),
            transforms=SystemStatusTransformsInfo(pending=False, last_apply_at=None),
            schema_drift=None,
            gsheet=SystemStatusGsheetInfo(
                total_connections=0, by_status={}, needs_attention=[]
            ),
            database_connections=db_connections,
        ),
        degraded=True,
        degraded_reason=(
            "A writer holds the database lock; the data inventory is unavailable "
            "until it releases. See database_connections for the holder."
        ),
        actions=[
            "Inspect database_connections for the writer holding the lock, then "
            "wait and retry or surface the contention to the user",
        ],
    )


@mcp_tool()
def system_status() -> ResponseEnvelope[SystemStatusPayload]:
    """Return data inventory, pending review queue counts, and transforms freshness.

    Use this tool to understand what data exists in MoneyBin and what
    needs user attention before suggesting any analytical query. The
    ``gsheet`` block summarizes Google Sheets connection health: drift-detected
    connections surface a paired ``gsheet_reconnect`` hint in ``actions[]``.
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
    block = _database_connections_block(get_settings().database.path)
    db_connections = SystemStatusDatabaseConnectionsInfo(
        writers=[SystemStatusWriter(**w) for w in block["writers"]],
        readers=[SystemStatusReader(**r) for r in block["readers"]],
    )

    try:
        with get_database(read_only=True) as db:
            status = SystemService(db).status()
            gsheet = _gsheet_block(db)
    except DatabaseLockError:
        return _locked_status_envelope(db_connections)

    min_date, max_date = status.transactions_date_range

    schema_drift_payload: SystemStatusSchemaDrift | None = None
    actions = [
        "Use transactions_review for per-queue review counts",
        "Use reports_spending for a monthly spending trend snapshot",
    ]
    if status.schema_drift:
        schema_drift_payload = SystemStatusSchemaDrift(
            tables=[
                SchemaDriftTable(name=table, missing_columns=cols)
                for table, cols in sorted(status.schema_drift.items())
            ],
            remediation="moneybin refresh",
        )
        actions.append(
            "Run refresh_run to rebuild stale models — "
            f"{len(status.schema_drift)} core table(s) drifted"
        )

    if status.transforms_pending:
        actions.append(
            "Run refresh_run to refresh derived tables "
            "(raw imports are newer than the last refresh)"
        )

    actions.extend(_gsheet_action_hints(gsheet["needs_attention"]))

    return build_envelope(
        data=SystemStatusPayload(
            accounts=SystemStatusAccountsInfo(count=status.accounts_count),
            transactions=SystemStatusTransactionsInfo(
                count=status.transactions_count,
                date_range=[
                    min_date.isoformat() if min_date else None,
                    max_date.isoformat() if max_date else None,
                ],
                last_import_at=(
                    status.last_import_at.isoformat() if status.last_import_at else None
                ),
            ),
            matches=SystemStatusMatchesInfo(pending_review=status.matches_pending),
            categorization=SystemStatusCategorizationInfo(
                uncategorized=status.categorize_pending
            ),
            transforms=SystemStatusTransformsInfo(
                pending=status.transforms_pending,
                last_apply_at=(
                    status.transforms_last_apply_at.isoformat()
                    if status.transforms_last_apply_at
                    else None
                ),
            ),
            schema_drift=schema_drift_payload,
            gsheet=SystemStatusGsheetInfo(
                total_connections=gsheet["total_connections"],
                by_status=gsheet["by_status"],
                needs_attention=[
                    SystemStatusGsheetRow(
                        connection_id=r["connection_id"],
                        workbook_name=r["workbook_name"],
                        sheet_name=r["sheet_name"],
                        status=r["status"],
                        reason=r["reason"],
                    )
                    for r in gsheet["needs_attention"]
                ],
            ),
            database_connections=db_connections,
        ),
        actions=actions,
    )


@mcp_tool(read_only=False)
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
    refusals: ``undo_operation_not_found``, ``undo_already_undone``, and
    ``recovery_no_path`` (the operation touched a table outside the undoable
    app.* surface, e.g. a manual import — re-import to recover instead).

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


@mcp_tool()
def system_audit_history(
    domain: str | None = None,
    since: str | None = None,
    actor: str | None = None,
    limit: int = 50,
    include_undone: bool = False,
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
            "Inspect before/after with system_audit_get(operation_id=...) "
            "before undoing",
            "Reverse an operation with system_audit_undo(operation_id=...)",
        ],
    )


@mcp_tool()
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


def register_system_tools(mcp: FastMCP) -> None:
    """Register all system namespace tools with the FastMCP server."""
    register(
        mcp,
        system_status,
        "system_status",
        "Return data inventory (accounts, transactions, freshness), pending review queue counts, "
        "and a transforms-pending signal indicating whether derived tables need a refresh. "
        "Call this first to orient before suggesting analytical queries.",
    )
    register(
        mcp,
        system_doctor,
        "system_doctor",
        "Run pipeline integrity checks across all SQLMesh named audits. "
        "Returns pass/fail/warn per invariant plus transaction count. "
        "Failing and warning invariants include a `recovery_actions` list of "
        "pre-built, directly-executable tool calls (tool, arguments, rationale, "
        "confidence, idempotent) the agent can dispatch to remediate the issue "
        "without further reasoning. "
        "May write SQLMesh state tables on first call. Call before relying on analytical results to confirm the pipeline is self-consistent.",
    )
    register(
        mcp,
        system_audit_undo,
        "system_audit_undo",
        "Reverse every app.* mutation in one operation as a unit, keyed on operation_id. "
        "Synthesizes each row's inverse from its audit before/after image; writes new audit rows under a fresh operation_id, so the undo is itself undoable (returned as undo_operation_id). "
        "Block-don't-cascade: if a later operation modified the same rows it refuses with undo_cascade_blocked and lists the blockers (newest first) in recovery_actions — undo those first. "
        "Other refusals: undo_operation_not_found, undo_already_undone, recovery_no_path (op touched a table outside the undoable app.* surface). "
        "Writes app.audit_log + the reversed app.* rows; revert by calling system_audit_undo on the returned undo_operation_id.",
    )
    register(
        mcp,
        system_audit_history,
        "system_audit_history",
        "List recent audited operations, newest first — the pull surface for reversing a change when no error preceded the regret. "
        "Each entry carries can_undo and, when blocked, undo_blocked_by (operation ids to undo first). Operator territory. "
        "domain filters to an action family (e.g. 'tag'); include_undone adds the undo operations themselves (hidden by default).",
    )
    register(
        mcp,
        system_audit_get,
        "system_audit_get",
        "Full before/after for every row of one operation — inspect exactly what system_audit_undo would change before running it. "
        "before_value/after_value can carry financial amounts (high sensitivity). Raises undo_operation_not_found for a bad operation_id.",
    )
