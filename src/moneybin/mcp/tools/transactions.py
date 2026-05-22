# src/moneybin/mcp/tools/transactions.py
"""Transactions namespace tools.

Tools:
    - transactions_get — Fetch transactions with filters (medium sensitivity)
    - transactions_review — Pending counts across queues (low)
    - transactions_matches_pending — List pending match decisions (low)
    - transactions_matches_set — Accept or reject one pending match (low)
    - transactions_matches_history — Recent match decisions, newest first (low)
    - transactions_matches_run — Run the matcher over existing transactions (low)
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.transactions import (
    MatchesHistoryPayload,
    MatchesPendingPayload,
    MatchHistoryRow,
    MatchPendingRow,
    MatchRunPayload,
    MatchSetPayload,
    ReviewStatusPayload,
    TransactionGetPayload,
    TransactionRow,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.matching_service import MatchingService
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
    For matches, fetch the queue via ``transactions_matches_pending`` and
    decide each pair with ``transactions_matches_set``.
    """
    from moneybin.services.categorization import CategorizationService
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
            "Use transactions_matches_pending to fetch the matches queue",
        ],
    )


@mcp_tool(domain="matches", read_only=False)
def transactions_matches_set(
    match_id: str,
    status: Literal["accepted", "rejected"],
) -> ResponseEnvelope[MatchSetPayload]:
    """Accept or reject one pending transaction match by id.

    Mutates app.match_decisions (sets match_status). Only a *pending* decision
    can be set. Rejecting an already-accepted match errors — reverse it via
    `moneybin transactions matches undo` instead (no MCP undo tool yet).
    Find ids with transactions_matches_pending.

    Args:
        match_id: The match decision id (from transactions_matches_pending).
        status: 'accepted' folds the pair via dedup; 'rejected' keeps both and
            prevents re-proposal.
    """
    with get_database() as db:
        MatchingService(db).set_status(match_id, status=status)
    return build_envelope(
        data=MatchSetPayload(match_id=match_id, match_status=status),
        actions=[
            "Use transactions_matches_pending to review remaining pending matches",
            "Use transactions_matches_undo (CLI) to reverse an accepted match",
        ],
    )


@mcp_tool(domain="matches")
def transactions_matches_pending(
    match_type: Literal["dedup", "transfer"] | None = None,
    limit: int = 50,
) -> ResponseEnvelope[MatchesPendingPayload]:
    """List pending transaction matches awaiting accept/reject.

    Returns pair decisions (match_id, type, confidence, the two source ids).
    app.match_decisions carries no descriptions/amounts — call transactions_get
    on a source_transaction_id to inspect the underlying transaction. Use
    transactions_matches_set to accept or reject one.

    Args:
        match_type: Filter to 'dedup' or 'transfer'. Default None returns both.
        limit: Maximum rows (default 50).
    """
    with get_database(read_only=True) as db:
        rows = MatchingService(db).get_pending(match_type=match_type)[:limit]
    return build_envelope(
        data=MatchesPendingPayload(
            matches=[
                MatchPendingRow(
                    match_id=r["match_id"],
                    match_type=r.get("match_type", "dedup"),
                    match_tier=r.get("match_tier"),
                    confidence_score=float(r.get("confidence_score") or 0.0),
                    source_type_a=r["source_type_a"],
                    source_transaction_id_a=r["source_transaction_id_a"],
                    source_type_b=r["source_type_b"],
                    source_transaction_id_b=r["source_transaction_id_b"],
                    match_status=r["match_status"],
                )
                for r in rows
            ]
        ),
        actions=[
            "Use transactions_matches_set to accept or reject one match",
            "Use transactions_get on a source_transaction_id to see the transaction",
        ],
    )


@mcp_tool(domain="matches", read_only=True)
def transactions_matches_history(
    limit: int = 20,
    match_type: Literal["dedup", "transfer"] | None = None,
) -> ResponseEnvelope[MatchesHistoryPayload]:
    """Recent match decisions (accepted/rejected/reversed), newest first.

    Args:
        limit: Maximum rows (default 20).
        match_type: Filter to 'dedup' or 'transfer'. Default None returns both.
    """
    with get_database(read_only=True) as db:
        rows = MatchingService(db).get_log(limit=limit, match_type=match_type)
    return build_envelope(
        data=MatchesHistoryPayload(
            matches=[
                MatchHistoryRow(
                    match_id=r["match_id"],
                    match_type=r.get("match_type", "dedup"),
                    match_status=r["match_status"],
                    confidence_score=float(r.get("confidence_score") or 0.0),
                    decided_by=r["decided_by"],
                )
                for r in rows
            ]
        ),
        actions=["Use transactions_matches_pending for the active queue"],
    )


@mcp_tool(domain="matches", read_only=False)
def transactions_matches_run() -> ResponseEnvelope[MatchRunPayload]:
    """Run the matcher (dedup + transfer detection) over existing transactions.

    Operator-territory: a granular alternative to refresh_run. Proposes pending
    matches for review via transactions_matches_pending. Does not auto-accept.
    """
    with get_database() as db:
        result = MatchingService(db).run()
    return build_envelope(
        data=MatchRunPayload(
            auto_merged=result.auto_merged,
            pending_review=result.pending_review,
            pending_transfers=result.pending_transfers,
        ),
        actions=["Use transactions_matches_pending to review proposed matches"],
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
    register(
        mcp,
        transactions_matches_set,
        "transactions_matches_set",
        "Accept or reject one pending transaction match by match_id. "
        "Mutation of app.match_decisions; only pending decisions are settable. "
        "Rejecting an already-accepted match errors — reverse via the CLI "
        "`moneybin transactions matches undo`. Discover ids with "
        "transactions_matches_pending.",
    )
    register(
        mcp,
        transactions_matches_pending,
        "transactions_matches_pending",
        "List pending transaction matches (dedup/transfer pairs) awaiting "
        "accept/reject. Returns pair ids and confidence — no amounts/descriptions; "
        "use transactions_get on a source id for those. Pair with "
        "transactions_matches_set to decide.",
    )
    register(
        mcp,
        transactions_matches_run,
        "transactions_matches_run",
        "Run the matcher (dedup + transfer detection) over existing transactions, "
        "proposing pending matches for review. Operator-level granular alternative "
        "to refresh_run; does not auto-accept. Review results with "
        "transactions_matches_pending.",
    )
    register(
        mcp,
        transactions_matches_history,
        "transactions_matches_history",
        "Recent transaction match decisions (accepted/rejected/reversed), newest "
        "first. Filter by match_type (dedup/transfer) and limit. Read-only. Use "
        "transactions_matches_pending for the active queue.",
    )
