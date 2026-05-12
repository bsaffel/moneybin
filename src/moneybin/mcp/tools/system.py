"""system_* tools — data status meta-view."""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP

from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope


@mcp_tool(sensitivity="low")
def system_status() -> ResponseEnvelope:
    """Return data inventory and pending review queue counts.

    Use this tool to understand what data exists in MoneyBin and what
    needs user attention before suggesting any analytical query.
    """
    from moneybin.database import get_database
    from moneybin.services.system_service import SystemService

    status = SystemService(get_database()).status()

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
    }

    return build_envelope(
        data=data,
        sensitivity="low",
        actions=[
            "Use transactions_review_status for per-queue review counts",
            "Use reports_spending_get for a monthly spending trend snapshot",
        ],
    )


@mcp_tool(sensitivity="low", read_only=True)
def system_doctor() -> ResponseEnvelope:
    """Run pipeline integrity checks across all SQLMesh named audits.

    Returns pass/fail/warn per invariant plus a transaction count.
    Read-only — never writes. Call before relying on analytical results
    to confirm the pipeline is self-consistent.
    """
    from moneybin.database import get_database
    from moneybin.services.doctor_service import DoctorService

    db = get_database()
    report = DoctorService(db).run_all(verbose=False)

    failing = sum(1 for r in report.invariants if r.status == "fail")
    warning = sum(1 for r in report.invariants if r.status == "warn")
    passing = sum(1 for r in report.invariants if r.status == "pass")

    actions: list[str] = []
    if failing > 0:
        actions.append("Run moneybin doctor --verbose for affected transaction IDs")

    return build_envelope(
        data={
            "passing": passing,
            "failing": failing,
            "warning": warning,
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
        total_count=len(report.invariants),
        actions=actions,
    )


def register_system_tools(mcp: FastMCP) -> None:
    """Register all system namespace tools with the FastMCP server."""
    register(
        mcp,
        system_status,
        "system_status",
        "Return data inventory (accounts, transactions, freshness) and pending review queue counts. "
        "Call this first to orient before suggesting analytical queries.",
    )
    register(
        mcp,
        system_doctor,
        "system_doctor",
        "Run pipeline integrity checks across all SQLMesh named audits. "
        "Returns pass/fail/warn per invariant plus transaction count. "
        "Read-only. Call before relying on analytical results to confirm the pipeline is self-consistent.",
    )
