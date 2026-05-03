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
            "Use reports_spending_summary for an income vs expenses snapshot",
        ],
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
