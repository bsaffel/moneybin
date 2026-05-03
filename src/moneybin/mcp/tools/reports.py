"""Reports namespace tools — spending analysis, budget vs actual, and financial summaries.

Tools:
    - reports_spending_summary — Income vs expense totals by month (low sensitivity)
    - reports_spending_by_category — Spending breakdown by category (low sensitivity)
    - reports_budget_status — Budget vs actual spending comparison (low sensitivity)
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.budget_service import BudgetService
from moneybin.services.spending_service import SpendingService


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
    and ``transactions_categorize_bulk_apply`` to categorize transactions first.
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


# Keep a deprecated shim so any code that still imports from spending.py
# (e.g. tests importing by name) finds the symbols. Remove after Task 11.
register_spending_tools = register_reports_tools
