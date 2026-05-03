# src/moneybin/mcp/tools/transactions.py
"""Transactions namespace tools — search, recurring patterns, review orientation.

Tools:
    - transactions_search — Search transactions with filters (medium sensitivity)
    - transactions_recurring_list — Detect recurring transaction patterns (medium sensitivity)
    - transactions_review_status — Pending counts across matches and categorize queues (low)
"""

from __future__ import annotations

from decimal import Decimal

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.transaction_service import TransactionService


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
def transactions_recurring_list(
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


@mcp_tool(sensitivity="low")
def transactions_review_status() -> ResponseEnvelope:
    """Return counts of pending reviews across both queues.

    Orientation tool: call this to decide which queue to drain first.
    For categorize, fetch items via ``transactions_categorize_pending_list``.
    Match review is CLI-only today (``moneybin transactions review --type
    matches``); a ``transactions_matches_pending`` MCP tool is planned.
    """
    from moneybin.services.categorization_service import CategorizationService
    from moneybin.services.matching_service import MatchingService
    from moneybin.services.review_service import ReviewService

    db = get_database()
    status = ReviewService(
        match_service=MatchingService(db=db),
        categorize_service=CategorizationService(db=db),
    ).status()

    return build_envelope(
        data={
            "matches_pending": status.matches_pending,
            "categorize_pending": status.categorize_pending,
            "total": status.total,
        },
        sensitivity="low",
        actions=[
            "Use transactions_categorize_pending_list to fetch the categorize queue",
            "For matches, run `moneybin transactions review --type matches` (CLI-only today)",
        ],
    )


def register_transactions_tools(mcp: FastMCP) -> None:
    """Register all transactions namespace tools with the FastMCP server."""
    register(
        mcp,
        transactions_search,
        "transactions_search",
        "Search transactions with flexible filtering by date, "
        "amount, description, account, and category.",
    )
    register(
        mcp,
        transactions_recurring_list,
        "transactions_recurring_list",
        "Detect recurring transaction patterns like subscriptions and regular charges.",
    )
    register(
        mcp,
        transactions_review_status,
        "transactions_review_status",
        "Return pending counts for matches and categorize queues. "
        "Call this to orient before fetching specific queue contents.",
    )
