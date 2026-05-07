"""Reports namespace tools — v2 per docs/specs/mcp-tool-surface.md.

Cross-domain analytical views (read-only). Combines net worth reporting
(from net-worth.md) and spending/budget analysis (from mcp-tool-surface.md v2).

Read tools:
  - reports_networth_get (medium)
  - reports_networth_history (medium)
  - reports_spending_summary (low)
  - reports_spending_by_category (low)
  - reports_budget_status (low)

All tools delegate to services — no business logic here.
"""

from __future__ import annotations

from datetime import date as _date

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.budget_service import BudgetService
from moneybin.services.networth_service import NetworthService
from moneybin.services.spending_service import SpendingService


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
    return build_envelope(data=snapshot.to_dict(), sensitivity="medium")


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


@mcp_tool(sensitivity="low")
def reports_spending_summary(
    months: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    account_id: list[str] | None = None,
) -> ResponseEnvelope:
    """Get income vs expense totals by month.

    Returns time-series data suitable for charting. Use ``months`` for
    recent history or ``start_date``/``end_date`` for a specific range.
    """
    service = SpendingService(get_database())
    result = service.summary(
        months=months,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
    )
    return result.to_envelope()


@mcp_tool(sensitivity="low")
def reports_spending_by_category(
    months: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    account_id: list[str] | None = None,
    top_n: int = 10,
    include_uncategorized: bool = True,
) -> ResponseEnvelope:
    """Get spending breakdown by category for a period.

    Requires transactions to be categorized. Use ``transactions_categorize_pending_list``
    and ``transactions_categorize_apply`` to categorize transactions first.
    """
    service = SpendingService(get_database())
    result = service.by_category(
        months=months,
        start_date=start_date,
        end_date=end_date,
        account_id=account_id,
        top_n=top_n,
        include_uncategorized=include_uncategorized,
    )
    return result.to_envelope()


@mcp_tool(sensitivity="low", domain="budget")
def reports_budget_status(
    month: str | None = None,
) -> ResponseEnvelope:
    """Get budget vs actual spending comparison for a month.

    Shows each budgeted category with its target, actual spending,
    remaining amount, and status (OK / WARNING / OVER).

    Args:
        month: Month to check (YYYY-MM). Defaults to current month.
    """
    service = BudgetService(get_database())
    result = service.status(month=month)
    return result.to_envelope()


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
    register(
        mcp,
        reports_spending_summary,
        "reports_spending_summary",
        "Get income vs expense totals by month. Returns time-series "
        "data suitable for charting.",
    )
    register(
        mcp,
        reports_spending_by_category,
        "reports_spending_by_category",
        "Get spending breakdown by category for a period. "
        "Requires transactions to be categorized.",
    )
    register(
        mcp,
        reports_budget_status,
        "reports_budget_status",
        "Get budget vs actual spending comparison for a month. "
        "Shows target, spent, remaining, and status for each category.",
    )
