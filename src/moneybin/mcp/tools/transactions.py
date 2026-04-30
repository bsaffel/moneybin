# src/moneybin/mcp/tools/transactions.py
"""Transactions namespace tools — search and recurring pattern detection.

Tools:
    - transactions.search — Search transactions with filters (medium sensitivity)
    - transactions.recurring — Detect recurring transaction patterns (medium sensitivity)
"""

from __future__ import annotations

import logging
from decimal import Decimal

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import tags_for
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.transaction_service import TransactionService

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="medium")
def transactions_search(
    start_date: str | None = None,
    end_date: str | None = None,
    min_amount: str | None = None,
    max_amount: str | None = None,
    description: str | None = None,
    account_id: str | None = None,
    category: str | None = None,
    uncategorized_only: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> ResponseEnvelope:
    """Search transactions with flexible filtering.

    Supports filtering by date range, amount range, description pattern,
    account, category, and categorization status. Results are ordered by
    date descending.

    Args:
        start_date: ISO 8601 start date (inclusive).
        end_date: ISO 8601 end date (inclusive).
        min_amount: Minimum amount as string (use negative for expenses).
        max_amount: Maximum amount as string (use negative for expenses).
        description: Pattern matched against description and memo (case-insensitive).
        account_id: Filter to a specific account.
        category: Filter by assigned category.
        uncategorized_only: Only return uncategorized transactions.
        limit: Maximum rows to return (default 100).
        offset: Number of rows to skip for pagination.
    """
    service = TransactionService(get_database())
    result = service.search(
        start_date=start_date,
        end_date=end_date,
        min_amount=Decimal(min_amount) if min_amount is not None else None,
        max_amount=Decimal(max_amount) if max_amount is not None else None,
        description=description,
        account_id=account_id,
        category=category,
        uncategorized_only=uncategorized_only,
        limit=limit,
        offset=offset,
    )
    return result.to_envelope()


@mcp_tool(sensitivity="medium")
def transactions_recurring(
    min_occurrences: int = 3,
) -> ResponseEnvelope:
    """Detect recurring transaction patterns like subscriptions.

    Groups expense transactions by description and rounded amount to
    identify recurring charges. Useful for finding subscriptions,
    memberships, and regular bills.

    Args:
        min_occurrences: Minimum number of occurrences to consider
            a transaction as recurring (default 3).
    """
    service = TransactionService(get_database())
    result = service.recurring(min_occurrences=min_occurrences)
    return result.to_envelope()


def register_transactions_tools(mcp: FastMCP) -> None:
    """Register all transactions namespace tools with the FastMCP server."""
    mcp.tool(
        name="transactions.search",
        description=(
            "Search transactions with flexible filtering by date, "
            "amount, description, account, and category."
        ),
        tags=tags_for(transactions_search),
    )(transactions_search)
    mcp.tool(
        name="transactions.recurring",
        description=(
            "Detect recurring transaction patterns like subscriptions "
            "and regular charges."
        ),
        tags=tags_for(transactions_recurring),
    )(transactions_recurring)
