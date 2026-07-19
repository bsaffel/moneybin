"""system_* tools — data status meta-view."""

from __future__ import annotations

import asyncio
import base64
import binascii
import fcntl  # POSIX-only: project targets macOS/Linux
import inspect
import json
import os
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import duckdb
from fastmcp import FastMCP
from pydantic import Field

from moneybin.db_lock import lock_path_for
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import tier_to_sensitivity
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.system import (
    AuditDetail,
    AuditEvents,
    AuditHistory,
    CategorizationStatus,
    DoctorStatus,
    InvariantResultPayload,
    OverviewStatus,
    RecoveryActionPayload,
    SchemaDriftTable,
    SystemAuditCoarsePayload,
    SystemAuditEventPayload,
    SystemAuditGetPayload,
    SystemAuditHistoryEntryPayload,
    SystemAuditHistoryPayload,
    SystemAuditUndoPayload,
    SystemDoctorPayload,
    SystemStatusAccountLinksInfo,
    SystemStatusAccountsInfo,
    SystemStatusCategorizationInfo,
    SystemStatusCoarsePayload,
    SystemStatusDatabaseConnectionsInfo,
    SystemStatusGsheetInfo,
    SystemStatusGsheetRow,
    SystemStatusMatchesInfo,
    SystemStatusMerchantLinksInfo,
    SystemStatusPayload,
    SystemStatusReader,
    SystemStatusSchemaDrift,
    SystemStatusSecurityLinksInfo,
    SystemStatusTransactionsInfo,
    SystemStatusTransformsInfo,
    SystemStatusWriter,
)
from moneybin.privacy.redaction import redact_typed
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.utils.db_processes import describe_process, find_blocking_processes

_HEALTHY_STATUSES = frozenset({"healthy"})
_DISCONNECTED_STATUSES = frozenset({"disconnected"})
# Re-reads of the write-lock metadata file to ride out write_lock's brief
# in-place rewrite (ftruncate-then-write) window before treating it as absent.
_METADATA_READ_ATTEMPTS = 3


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

    Conversely, this probe's brief ``LOCK_SH`` hold can make a writer's
    concurrent ``LOCK_EX`` attempt fail once and take a single backoff retry
    (~50 ms). That is harmless and expected — a spurious retry here is the probe
    doing its job, not a symptom of deeper contention.
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


def _read_writer_metadata(lock_path: Path) -> dict[str, Any] | None:
    """Read + parse the writer metadata, tolerating the brief rewrite window.

    write_lock rewrites the metadata in place — ``ftruncate(0)`` then
    ``write(payload)`` — so a diagnostic reader can momentarily observe an empty
    or partial file and fail to parse it. That window is only a couple of
    syscalls wide, so retry a few times before giving up; without the retry a
    live writer would be reported with no metadata exactly while it is acquiring
    the lock — the contention the DatabaseLockError recovery action sends the
    agent here to diagnose. Returns the normalized writer dict, or None if the
    file is absent or genuinely unparseable after the retries.
    """
    for _ in range(_METADATA_READ_ATTEMPTS):
        try:
            metadata = json.loads(lock_path.read_text(encoding="utf-8"))
            return {
                "pid": int(metadata["pid"]),
                # The writer command is already a sanitized friendly name —
                # write_lock runs it through describe_process before storing it,
                # so the on-disk lock file never holds a raw argv. Pass it
                # through as-is (re-sanitizing would mangle the friendly name).
                "command": str(metadata["command"]),
                "started_at": str(metadata["started_at"]),
                "operation_type": str(metadata["operation_type"]),
            }
        except (OSError, ValueError, KeyError, TypeError):
            # Empty/partial file (mid-rewrite), corrupted JSON, or non-dict JSON
            # (null/list/scalar makes metadata["pid"] raise TypeError). Retry —
            # the next read likely catches the completed write.
            continue
    return None


def _database_connections_info(db_path: Path) -> SystemStatusDatabaseConnectionsInfo:
    """Build the typed database_connections payload from the lock + lsof view."""
    block = _database_connections_block(db_path)
    return SystemStatusDatabaseConnectionsInfo(
        writers=[SystemStatusWriter(**w) for w in block["writers"]],
        readers=[SystemStatusReader(**r) for r in block["readers"]],
    )


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
    # Resolve once for the lsof reader scan; lock_path_for resolves db_path
    # itself for the lock file. Both resolve the same db_path, so the writer
    # probe and the reader scan stay on one inode even for a symlinked or
    # relative path — and neither re-resolves an already-resolved path.
    resolved = db_path.resolve()
    lock_path = lock_path_for(db_path)
    # No separate exists() check: _writer_is_live opens the lock file and
    # returns False if it is absent, so an exists() guard would be a redundant,
    # TOCTOU-prone stat.
    if _writer_is_live(lock_path):
        metadata = _read_writer_metadata(lock_path)
        if metadata is not None:
            writer_pid = metadata["pid"]
            writers.append(metadata)
        # If a live writer's metadata is still unreadable after the retries in
        # _read_writer_metadata (genuinely corrupt, not just mid-rewrite), fall
        # back to no writer entry — the lock-file payload is best-effort
        # observability, not a correctness contract.

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
            account_links=SystemStatusAccountLinksInfo(pending_review=0),
            merchant_links=SystemStatusMerchantLinksInfo(pending_review=0),
            security_links=SystemStatusSecurityLinksInfo(pending_review=0),
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
    db_path = get_settings().database.path
    db_connections = _database_connections_info(db_path)

    try:
        # Short max_wait: this is the DatabaseLockError recovery tool, so under
        # contention it must degrade fast rather than burn a third of the 30 s
        # MCP dispatch budget retrying the read. The diagnostic does not need the
        # read to succeed — db_connections is captured before the open and
        # recomputed in the except branch below.
        with get_database(read_only=True, max_wait=2.0) as db:
            status = SystemService(db).status()
            gsheet = _gsheet_block(db)
    except DatabaseLockError:
        # Re-snapshot before degrading: a writer can acquire the lock between the
        # preflight snapshot above and this read failing, so the preflight view
        # may predate the writer and report no holder. Recomputing here names the
        # writer that actually caused the lock — exactly what the
        # DatabaseLockError recovery action sends the agent to system_status for.
        return _locked_status_envelope(_database_connections_info(db_path))

    min_date, max_date = status.transactions_date_range

    schema_drift_payload: SystemStatusSchemaDrift | None = None
    actions = [
        "Use `review` for per-queue review counts (matches + categorize + account-links + merchant-links)",
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
            account_links=SystemStatusAccountLinksInfo(
                pending_review=status.account_links_pending
            ),
            merchant_links=SystemStatusMerchantLinksInfo(
                pending_review=status.merchant_links_pending
            ),
            security_links=SystemStatusSecurityLinksInfo(
                pending_review=status.security_links_pending
            ),
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
    classes = {
        data_class
        for contract_type in contract_types
        for data_class in extract_data_classes(contract_type)
    }
    tier = max(data_class.tier for data_class in classes)
    redacted = cast(T, redact_typed(data, None))
    return cast(
        ResponseEnvelope[T],
        build_envelope(
            data=redacted,
            sensitivity=cast(Any, tier_to_sensitivity(tier).value),
            total_count=total_count,
            returned_count=returned_count,
            next_cursor=next_cursor,
            actions=actions,
            degraded=degraded,
            degraded_reason=degraded_reason,
            classes_returned=sorted(data_class.value for data_class in classes),
        ),
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


def _audit_cursor(view: Literal["events", "history"], offset: int) -> str:
    """Encode a view-bound opaque audit pagination cursor."""
    raw = json.dumps(
        {"offset": offset, "view": view},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(raw).decode()


def _audit_offset(
    cursor: str | None,
    view: Literal["events", "history"],
) -> int:
    """Decode an audit cursor and reject malformed or cross-view reuse."""
    if cursor is None:
        return 0
    try:
        decoded = base64.b64decode(cursor.encode(), altchars=b"-_", validate=True)
        value = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid audit cursor") from exc
    if not isinstance(value, dict):
        raise ValueError("invalid audit cursor")
    cursor_payload = cast(dict[str, Any], value)
    if set(cursor_payload) != {"offset", "view"}:
        raise ValueError("invalid audit cursor")
    offset = cursor_payload["offset"]
    if (
        cursor_payload["view"] != view
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
    ):
        raise ValueError("invalid audit cursor")
    return offset


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


@mcp_tool(dynamic_classification=True, read_only=False)
async def system_status_coarse(
    sections: list[Literal["overview", "doctor", "categorization"]] | None = None,
    detail: Literal["summary", "full"] = "summary",
) -> ResponseEnvelope[SystemStatusCoarsePayload]:
    """Return selected system overview, integrity, and categorization sections."""
    from moneybin.mcp.tools.transactions_categorize import (
        transactions_categorize_stats,
    )

    requested = (
        ["overview", "doctor", "categorization"] if sections is None else sections
    )
    if not requested:
        raise ValueError("At least one system status section is required.")
    if len(set(requested)) != len(requested):
        raise ValueError("System status sections must not contain duplicates.")

    selected: list[OverviewStatus | DoctorStatus | CategorizationStatus] = []
    actions: list[str] = []
    degraded_reasons: list[str] = []
    for section in requested:
        if section == "overview":
            response = await _run_tool_body(system_status)
            if response.error is not None:
                return cast(ResponseEnvelope[SystemStatusCoarsePayload], response)
            selected.append(OverviewStatus(overview=response.data))
        elif section == "doctor":
            response = await _run_tool_body(system_doctor, full=detail == "full")
            if response.error is not None:
                return cast(ResponseEnvelope[SystemStatusCoarsePayload], response)
            selected.append(DoctorStatus(doctor=response.data))
        else:
            response = await _run_tool_body(
                transactions_categorize_stats, include_auto=detail == "full"
            )
            if response.error is not None:
                return cast(ResponseEnvelope[SystemStatusCoarsePayload], response)
            selected.append(CategorizationStatus(statistics=response.data))
        actions.extend(response.actions)
        if response.summary.degraded:
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
        degraded=bool(degraded_reasons),
        degraded_reason=" ".join(dict.fromkeys(degraded_reasons)) or None,
    )


@mcp_tool(dynamic_classification=True)
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
                code="AUDIT_IDENTIFIER_REQUIRED",
            )
        if cursor is not None:
            raise UserError(
                "Audit detail does not accept a cursor.",
                code="AUDIT_CURSOR_NOT_ALLOWED",
            )
    elif operation_id is not None or audit_id is not None:
        raise UserError(
            "operation_id and audit_id are only valid for audit detail.",
            code="AUDIT_IDENTIFIER_NOT_ALLOWED",
        )

    if view == "events":
        from moneybin.mcp.tools.curation import system_audit

        offset = _audit_offset(cursor, "events")
        response = await _run_tool_body(system_audit, limit=offset + limit + 1)
        if response.error is not None:
            return cast(ResponseEnvelope[SystemAuditCoarsePayload], response)
        rows = response.data.events
        page = rows[offset : offset + limit]
        has_more = len(rows) > offset + limit
        next_cursor = _audit_cursor("events", offset + limit) if has_more else None
        payload = AuditEvents(events=page)
        return _dynamic_coarse_envelope(
            payload,
            contract_types=[AuditEvents],
            total_count=offset + len(page) + (1 if has_more else 0),
            returned_count=len(page),
            next_cursor=next_cursor,
            actions=_audit_list_actions(
                "events",
                limit=limit,
                next_cursor=next_cursor,
            ),
        )

    if view == "history":
        offset = _audit_offset(cursor, "history")
        response = await _run_tool_body(
            system_audit_history,
            limit=offset + limit + 1,
        )
        if response.error is not None:
            return cast(ResponseEnvelope[SystemAuditCoarsePayload], response)
        rows = response.data.operations
        page = rows[offset : offset + limit]
        has_more = len(rows) > offset + limit
        next_cursor = _audit_cursor("history", offset + limit) if has_more else None
        payload = AuditHistory(operations=page)
        return _dynamic_coarse_envelope(
            payload,
            contract_types=[AuditHistory],
            total_count=offset + len(page) + (1 if has_more else 0),
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
            code="AUDIT_IDENTIFIER_NOT_FOUND",
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
    """Register the dormant Plan 6 replacement system reads."""
    register(
        mcp,
        system_status_coarse,
        "system_status",
        "Return selected operator status sections: overview inventory, integrity "
        "doctor checks, and categorization coverage. detail='full' deepens the "
        "doctor scan and includes auto-categorization health.",
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
    # Plan 6 cutover removals: system_doctor, transactions_categorize_stats,
    # system_audit_history, and system_audit_get. Their live registrations stay
    # untouched until the complete standard registry activates atomically.


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
