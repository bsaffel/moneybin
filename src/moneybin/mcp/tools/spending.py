# src/moneybin/mcp/tools/spending.py
"""Spending namespace tools — expense analysis, trends, category breakdowns.

Tools:
    - spending.summary — Income vs expense totals by month (low sensitivity)
    - spending.by_category — Spending by category for a period (low sensitivity)
"""

from __future__ import annotations

import logging

from moneybin.database import get_database
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.namespaces import NamespaceRegistry, ToolDefinition
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.spending_service import SpendingService

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="low")
def spending_summary(
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
def spending_by_category(
    months: int = 3,
    start_date: str | None = None,
    end_date: str | None = None,
    account_id: list[str] | None = None,
    top_n: int = 10,
    include_uncategorized: bool = True,
) -> ResponseEnvelope:
    """Get spending breakdown by category for a period.

    Requires transactions to be categorized. Use ``categorize.uncategorized``
    and ``categorize.bulk`` to categorize transactions first.
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


def register_spending_tools(registry: NamespaceRegistry) -> list[ToolDefinition]:
    """Register all spending namespace tools with the registry."""
    tools = [
        ToolDefinition(
            name="spending.summary",
            description=(
                "Get income vs expense totals by month. Returns time-series "
                "data suitable for charting."
            ),
            fn=spending_summary,
        ),
        ToolDefinition(
            name="spending.by_category",
            description=(
                "Get spending breakdown by category for a period. "
                "Requires transactions to be categorized."
            ),
            fn=spending_by_category,
        ),
    ]
    for tool in tools:
        registry.register(tool)
    return tools
