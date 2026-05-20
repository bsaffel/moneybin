# src/moneybin/mcp/tools/transactions.py
"""Transactions namespace tools.

Tools:
    - transactions_get — Fetch transactions with filters (medium sensitivity)
    - transactions_review — Pending counts across queues (low)
"""

from __future__ import annotations

from decimal import Decimal

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.transactions import (
    ReviewStatusPayload,
    TransactionGetPayload,
    TransactionRow,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.transaction_service import TransactionService


@mcp_tool(read_only=True)
def transactions_get(
    accounts: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    categories: list[str] | None = None,
    amount_min: str | None = None,
    amount_max: str | None = None,
    description: str | None = None,
    uncategorized_only: bool = False,
    limit: int = 50,
    cursor: str | None = None,
) -> ResponseEnvelope[TransactionGetPayload]:
    """Fetch transactions with optional filtering and cursor-based pagination.

    Returns full transaction records including curation metadata (notes, tags,
    splits) from core.fct_transactions. All filters are combinable.

    Args:
        accounts: Account IDs or display names to filter by. Accepts exact
            account_id values or fuzzy display names — use accounts to
            discover IDs. Multiple values are OR-combined.
        date_from: ISO 8601 start date, inclusive (e.g. '2026-01-01').
        date_to: ISO 8601 end date, inclusive.
        categories: Category names to filter by. Multiple values are OR-combined.
        amount_min: Minimum amount as a decimal string (e.g. '-50.00'). Negative
            = expense, positive = income.
        amount_max: Maximum amount as a decimal string.
        description: Case-insensitive pattern matched against description and memo.
        uncategorized_only: Only return transactions with no user/AI/rule
            categorization assigned (includes source-provided categories).
        limit: Maximum rows to return (default 50).
        cursor: Opaque pagination token from a previous response's next_cursor.
    """
    with get_database(read_only=True) as db:
        result = TransactionService(db).get(
            accounts=accounts,
            date_from=date_from,
            date_to=date_to,
            categories=categories,
            amount_min=Decimal(amount_min) if amount_min is not None else None,
            amount_max=Decimal(amount_max) if amount_max is not None else None,
            description=description,
            uncategorized_only=uncategorized_only,
            limit=limit,
            cursor=cursor,
        )
    payload = TransactionGetPayload(
        transactions=[
            TransactionRow(
                transaction_id=t.transaction_id,
                account_id=t.account_id,
                transaction_date=t.transaction_date,
                amount=t.amount,
                description=t.description,
                memo=t.memo,
                source_type=t.source_type,
                category=t.category,
                subcategory=t.subcategory,
                notes=t.notes,
                tags=t.tags,
                splits=t.splits,
            )
            for t in result.transactions
        ],
        next_cursor=result.next_cursor,
    )
    return build_envelope(
        data=payload,
        next_cursor=result.next_cursor,
        actions=[
            "Use transactions_get with the next_cursor value to fetch the next page",
            "Use reports_spending for category breakdowns",
            "Use transactions_categorize_commit to categorize uncategorized transactions",
        ],
    )


@mcp_tool()
def transactions_review() -> ResponseEnvelope[ReviewStatusPayload]:
    """Return counts of pending reviews across both queues.

    Orientation tool: call this to decide which queue to drain first.
    For categorize, fetch items via ``transactions_categorize_pending``.
    Match review is CLI-only today (``moneybin transactions review --type
    matches``); a ``transactions_matches_pending`` MCP tool is planned.
    """
    from moneybin.services.categorization import CategorizationService
    from moneybin.services.matching_service import MatchingService
    from moneybin.services.review_service import ReviewService

    with get_database(read_only=True) as db:
        status = ReviewService(
            match_service=MatchingService(db=db),
            categorize_service=CategorizationService(db=db),
        ).status()

    return build_envelope(
        data=ReviewStatusPayload(
            matches_pending=status.matches_pending,
            categorize_pending=status.categorize_pending,
            total=status.total,
        ),
        actions=[
            "Use transactions_categorize_pending to fetch the categorize queue",
            "For matches, run `moneybin transactions review --type matches` (CLI-only today)",
        ],
    )


def register_transactions_tools(mcp: FastMCP) -> None:
    """Register all transactions namespace tools with the FastMCP server."""
    register(
        mcp,
        transactions_get,
        "transactions_get",
        "Fetch transactions with optional filtering by account, date range, category, "
        "amount, and description pattern. Returns full transaction records including "
        "curation fields (notes, tags, splits). "
        "Amounts use the accounting convention: negative = expense, positive = income; "
        "transfers exempt. Amounts are in the currency named by `summary.display_currency`. "
        "`accounts` accepts display names or exact account IDs — call the `accounts` "
        "tool to discover IDs. Pass `next_cursor` from a previous response to fetch the next page.",
    )
    register(
        mcp,
        transactions_review,
        "transactions_review",
        "Return pending counts for matches and categorize queues. "
        "Call this to orient before fetching specific queue contents.",
    )
