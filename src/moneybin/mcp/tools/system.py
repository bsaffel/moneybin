"""system_* tools — data status meta-view."""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

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
            SELECT connection_id, workbook_name, sheet_name, status, last_drift_reason
            FROM app.gsheet_connections
            ORDER BY created_at ASC, connection_id ASC
            """
        ).fetchall()
    except Exception:  # noqa: BLE001 — table may not exist on bare DBs before init_schemas
        return {"total_connections": 0, "by_status": {}, "needs_attention": []}

    by_status: dict[str, int] = {}
    needs_attention: list[dict[str, Any]] = []
    for connection_id, workbook, sheet, status, drift_reason in rows:
        by_status[status] = by_status.get(status, 0) + 1
        if status in _HEALTHY_STATUSES or status in _DISCONNECTED_STATUSES:
            continue
        needs_attention.append({
            "connection_id": connection_id,
            "workbook": workbook,
            "sheet": sheet,
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
                "Re-authenticate with the CLI: `moneybin gsheet auth` "
                "(OAuth flow opens a browser; not available via MCP)."
            )
        else:
            hints.append(
                f"Run gsheet_status(connection_id='{cid}') to inspect the "
                f"failure detail (status={status})."
            )
    return hints


@mcp_tool(sensitivity="low")
def system_status() -> ResponseEnvelope:
    """Return data inventory, pending review queue counts, and transforms freshness.

    Use this tool to understand what data exists in MoneyBin and what
    needs user attention before suggesting any analytical query. The
    ``gsheet`` block summarizes Google Sheets connection health: drift-detected
    connections surface a paired ``gsheet_reconnect`` hint in ``actions[]``.
    """
    from moneybin.database import get_database
    from moneybin.services.system_service import SystemService

    with get_database(read_only=True) as db:
        status = SystemService(db).status()
        gsheet = _gsheet_block(db)

    min_date, max_date = status.transactions_date_range
    data: dict[str, Any] = {
        "accounts": {"count": status.accounts_count},
        "transactions": {
            "count": status.transactions_count,
            "date_range": [
                min_date.isoformat() if min_date else None,
                max_date.isoformat() if max_date else None,
            ],
            "last_import_at": status.last_import_at.isoformat()
            if status.last_import_at
            else None,
        },
        "matches": {"pending_review": status.matches_pending},
        "categorization": {"uncategorized": status.categorize_pending},
        "transforms": {
            "pending": status.transforms_pending,
            "last_apply_at": status.transforms_last_apply_at.isoformat()
            if status.transforms_last_apply_at
            else None,
        },
        "gsheet": gsheet,
    }

    actions = [
        "Use transactions_review for per-queue review counts",
        "Use reports_spending for a monthly spending trend snapshot",
    ]
    if status.schema_drift:
        data["schema_drift"] = {
            "tables": [
                {"name": table, "missing_columns": cols}
                for table, cols in sorted(status.schema_drift.items())
            ],
            "remediation": "moneybin refresh",
        }
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
        data=data,
        sensitivity="low",
        actions=actions,
    )


@mcp_tool(sensitivity="low", read_only=False)
def system_doctor() -> ResponseEnvelope:
    """Run pipeline integrity checks across all SQLMesh named audits.

    Returns pass/fail/warn per invariant plus a transaction count.
    May write SQLMesh state tables on first Context init. Call before
    relying on analytical results to confirm the pipeline is self-consistent.
    """
    from moneybin.database import get_database
    from moneybin.services.doctor_service import DoctorService

    with get_database() as db:
        report = DoctorService(db).run_all(verbose=False)

    failing = report.failing
    warning = report.warning
    passing = report.passing

    actions: list[str] = []
    if failing > 0:
        actions.append(
            "Run moneybin system doctor --verbose for affected transaction IDs"
        )

    return build_envelope(
        data={
            "passing": passing,
            "failing": failing,
            "warning": warning,
            "skipped": report.skipped,
            "transaction_count": report.transaction_count,
            "invariants": [
                {
                    "name": r.name,
                    "status": r.status,
                    "detail": r.detail,
                    "affected_ids": r.affected_ids,
                }
                for r in report.invariants
            ],
        },
        sensitivity="low",
        actions=actions,
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
        "May write SQLMesh state tables on first call. Call before relying on analytical results to confirm the pipeline is self-consistent.",
    )
