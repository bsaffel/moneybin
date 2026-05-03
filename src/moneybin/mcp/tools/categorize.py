"""Categorize namespace tools — re-exports from split modules.

The tools previously in this file have been split into:
- categories.py — categories_list, categories_create, categories_toggle
- merchants.py — merchants_list, merchants_create
- transactions_categorize.py — all transactions_categorize_* tools
"""

from __future__ import annotations

from fastmcp import FastMCP

from moneybin.mcp.tools.categories import (
    categories_create,
    categories_list,
    categories_toggle,
    register_categories_tools,
)
from moneybin.mcp.tools.merchants import (
    merchants_create,
    merchants_list,
    register_merchants_tools,
)
from moneybin.mcp.tools.transactions_categorize import (
    register_transactions_categorize_tools,
    transactions_categorize_auto_confirm,
    transactions_categorize_auto_review,
    transactions_categorize_auto_stats,
    transactions_categorize_bulk_apply,
    transactions_categorize_pending_list,
    transactions_categorize_rule_delete,
    transactions_categorize_rules_create,
    transactions_categorize_rules_list,
    transactions_categorize_stats,
)

__all__ = [
    "categories_create",
    "categories_list",
    "categories_toggle",
    "merchants_create",
    "merchants_list",
    "transactions_categorize_auto_confirm",
    "transactions_categorize_auto_review",
    "transactions_categorize_auto_stats",
    "transactions_categorize_bulk_apply",
    "transactions_categorize_pending_list",
    "transactions_categorize_rule_delete",
    "transactions_categorize_rules_create",
    "transactions_categorize_rules_list",
    "transactions_categorize_stats",
]


def register_categorize_tools(mcp: FastMCP) -> None:
    """Register all categorize namespace tools with the FastMCP server."""
    register_categories_tools(mcp)
    register_merchants_tools(mcp)
    register_transactions_categorize_tools(mcp)
