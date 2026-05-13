"""Budget namespace tools — budget mutation.

Tools:
    - budget_set — Create or update a budget target (low sensitivity)

Note: reports_budget_status (read) lives in reports.py per the v2 read/write split.
"""

from __future__ import annotations

from decimal import Decimal

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.budget_service import BudgetService


@mcp_tool(sensitivity="low", domain="budget", read_only=False)
def budget_set(
    category: str,
    monthly_amount: str,
    start_month: str | None = None,
) -> ResponseEnvelope:
    """Create or update a monthly budget target for a category.

    If a budget already exists for this category with an overlapping
    date range, it is updated. Otherwise a new budget is created.

    Args:
        category: Spending category name (should match transaction categories).
        monthly_amount: Monthly spending target in USD (as string, e.g. "200.00").
        start_month: First active month (YYYY-MM). Defaults to current month.
    """
    with get_database() as db:
        service = BudgetService(db)
        result = service.set_budget(
            category=category,
            monthly_amount=Decimal(monthly_amount),
            start_month=start_month,
        )
    return result.to_envelope()


def register_budget_tools(mcp: FastMCP) -> None:
    """Register all budget namespace tools with the FastMCP server."""
    register(
        mcp,
        budget_set,
        "budget_set",
        "Create or update a monthly budget target for a spending category. "
        "Amounts are in the currency named by `summary.display_currency`. "
        "Writes app.budgets (insert or update on date-range overlap); revert by calling again with the prior monthly_amount, or by setting monthly_amount to 0 to disable the target.",
    )
