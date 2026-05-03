"""Reports namespace tools — v2 per docs/specs/mcp-tool-surface.md.

Cross-domain analytical views (read-only, all medium sensitivity).
Future report specs (spending, cashflow, tax, budget vs actual) add
tools to this same module per cli-restructure.md v2.

Read tools:
  - reports_networth_get
  - reports_networth_history

All tools delegate to NetworthService — no business logic here.
"""

from __future__ import annotations

from datetime import date as _date

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.networth_service import NetworthService


@mcp_tool(sensitivity="medium")
def reports_networth_get(
    as_of_date: str | None = None,
    account_ids: list[str] | None = None,
) -> ResponseEnvelope:
    """Current or as-of net worth snapshot with per-account breakdown.

    Net worth = sum of balances across accounts where include_in_net_worth=True
    AND archived=False. Excluded/archived accounts do not contribute.

    Args:
        as_of_date: ISO date (YYYY-MM-DD); shows networth on or before this
            date. Default: latest available.
        account_ids: Filter the per-account breakdown to specific account IDs.
            The headline net_worth total still reflects all included accounts.
    """
    parsed_date = _date.fromisoformat(as_of_date) if as_of_date else None
    snapshot = NetworthService(get_database()).current(
        as_of_date=parsed_date, account_ids=account_ids
    )
    return build_envelope(
        data={
            "balance_date": snapshot.balance_date.isoformat(),
            "net_worth": snapshot.net_worth,
            "total_assets": snapshot.total_assets,
            "total_liabilities": snapshot.total_liabilities,
            "account_count": snapshot.account_count,
            "per_account": snapshot.per_account,
        },
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium")
def reports_networth_history(
    from_date: str,
    to_date: str,
    interval: str = "monthly",
) -> ResponseEnvelope:
    """Net worth time series with period-over-period change.

    Args:
        from_date: ISO date (YYYY-MM-DD); inclusive start
        to_date: ISO date (YYYY-MM-DD); inclusive end
        interval: 'daily' | 'weekly' | 'monthly' (default: monthly)

    Returns a list of {period, net_worth, change_abs, change_pct} dicts.
    The first period has change_abs=None and change_pct=None (no prior period).
    """
    parsed_from = _date.fromisoformat(from_date)
    parsed_to = _date.fromisoformat(to_date)
    rows = NetworthService(get_database()).history(
        parsed_from, parsed_to, interval=interval
    )
    return build_envelope(data=rows, sensitivity="medium")


def register_reports_tools(mcp: FastMCP) -> None:
    """Register all reports namespace tools with the FastMCP server."""
    register(
        mcp,
        reports_networth_get,
        "reports_networth_get",
        "Current or historical net worth snapshot with per-account breakdown.",
    )
    register(
        mcp,
        reports_networth_history,
        "reports_networth_history",
        "Net worth time series with period-over-period change (daily/weekly/monthly).",
    )
