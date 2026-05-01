# src/moneybin/mcp/tools/budget.py
"""Budget namespace tools — budget targets and spending status.

Tools:
    - budget.set — Create or update a budget target (low sensitivity)
    - budget.status — Budget vs actual spending comparison (low sensitivity)
"""

from __future__ import annotations

from decimal import Decimal

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.budget_service import BudgetService


@mcp_tool(sensitivity="low", domain="budget")
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
    service = BudgetService(get_database())
    result = service.set_budget(
        category=category,
        monthly_amount=Decimal(monthly_amount),
        start_month=start_month,
    )
    return result.to_envelope()


@mcp_tool(sensitivity="low", domain="budget")
def budget_status(
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


def register_budget_tools(mcp: FastMCP) -> None:
    """Register all budget namespace tools with the FastMCP server."""
    register(
        mcp,
        budget_set,
        "budget.set",
        "Create or update a monthly budget target for a spending category.",
    )
    register(
        mcp,
        budget_status,
        "budget.status",
        "Get budget vs actual spending comparison for a month. "
        "Shows target, spent, remaining, and status for each category.",
    )
