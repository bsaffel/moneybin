"""Spending namespace tools — re-exports from reports.py.

The tools previously in this file have been moved to reports.py under the
reports_* naming convention per the v2 taxonomy.
"""

from __future__ import annotations

from moneybin.mcp.tools.reports import (
    register_reports_tools,
    reports_spending_by_category,
    reports_spending_summary,
)

__all__ = [
    "reports_spending_summary",
    "reports_spending_by_category",
    "register_reports_tools",
]

# Alias for backward compat with any code importing register_spending_tools.
register_spending_tools = register_reports_tools
