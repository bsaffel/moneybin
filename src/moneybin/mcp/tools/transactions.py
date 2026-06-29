# src/moneybin/mcp/tools/transactions.py
"""Transactions namespace tools.

Tools:
    - review — Pending counts across all review queues (low); top-level orientation
    - transactions_get — Fetch transactions with filters (medium sensitivity)
    - transactions_review — DEPRECATED alias for `review`; removed after one minor release
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


def _build_review_envelope() -> ResponseEnvelope[ReviewStatusPayload]:
    """Shared impl for the `review` tool and the deprecated `transactions_review` alias.

    Opens a short-lived read-only DB connection, calls ReviewService.status()
    (which queries all four review queues), and returns the envelope.
    """
    from moneybin.services.account_links_service import AccountLinksService
    from moneybin.services.categorization import CategorizationService
    from moneybin.services.merchant_links_service import MerchantLinksService
    from moneybin.services.review_service import ReviewService

    with get_database(read_only=True) as db:
        status = ReviewService(
            match_service=MatchingService(db=db),
            categorize_service=CategorizationService(db=db),
            account_links_service=AccountLinksService(db=db),
            merchant_links_service=MerchantLinksService(db=db),
        ).status()

    return build_envelope(
        data=ReviewStatusPayload(
            matches_pending=status.matches_pending,
            categorize_pending=status.categorize_pending,
            account_links_pending=status.account_links_pending,
            merchant_links_pending=status.merchant_links_pending,
            total=status.total,
        ),
        actions=[
            "Use transactions_categorize_pending to fetch the categorize queue",
            "Use transactions_matches_pending to fetch the matches queue",
            "Use accounts_links_pending to fetch the account-links queue",
        ],
    )


@mcp_tool()
def review() -> ResponseEnvelope[ReviewStatusPayload]:
    """Return counts of pending reviews across all four queues.

    Orientation tool: call this to answer "what needs my attention?" in one call.
    Surfaces matches_pending, categorize_pending, account_links_pending, and
    merchant_links_pending so the agent can decide which queue to drain first.
    For categorize, fetch items via ``transactions_categorize_pending``.
    For matches, fetch the queue via ``transactions_matches_pending`` and
    decide each pair with ``transactions_matches_set``.
    For account links, fetch the queue via ``accounts_links_pending`` and
    decide each group with ``accounts_links_set``.
    """
    return _build_review_envelope()


@mcp_tool()
def transactions_review() -> ResponseEnvelope[ReviewStatusPayload]:
    """DEPRECATED: use `review` — removed after one minor release.

    Return counts of pending reviews across all four queues.
    Orientation tool: call this to decide which queue to drain first.
    Prefer the top-level ``review`` tool going forward.
    """
    return _build_review_envelope()


@mcp_tool(domain="matches", read_only=False)
def transactions_matches_set(
    match_id: str,
    status: Literal["accepted", "rejected"],
) -> ResponseEnvelope[MatchSetPayload]:
    """Accept or reject one pending transaction match by id.

    Mutates app.match_decisions (sets match_status). Only a *pending* decision
    can be set. Re-asserting a decision's current status is an idempotent no-op;
    any cross-status transition on an already-decided match errors with
    recovery_actions (e.g. rejecting an already-accepted match). Reverse an
    accepted match via `moneybin transactions matches undo` (no MCP undo tool
    yet). Find ids with transactions_matches_pending.

    Args:
        match_id: The match decision id (from transactions_matches_pending).
        status: 'accepted' folds the pair via dedup; 'rejected' keeps both and
            prevents re-proposal.
    """
    with get_database(read_only=False) as db:
        MatchingService(db).set_status(match_id, status=status, actor="mcp")
    return build_envelope(
        data=MatchSetPayload(match_id=match_id, match_status=status),
        actions=[
            "Use transactions_matches_pending to review remaining pending matches",
            "Run `moneybin transactions matches undo <match_id>` (CLI) to reverse "
            "an accepted match — there is no MCP undo tool yet",
        ],
    )


@mcp_tool(domain="matches")
def transactions_matches_pending(
    match_type: Literal["dedup", "transfer"] | None = None,
    limit: int = 50,
) -> ResponseEnvelope[MatchesPendingPayload]:
    """List pending transaction matches awaiting accept/reject.

    Returns pair decisions (match_id, type, confidence, the two source ids).
    app.match_decisions carries no descriptions/amounts; the confidence score
    and match type are the decision signal. Use transactions_matches_set to
    accept or reject one. ``summary.has_more`` indicates whether more pending
    matches exist beyond ``limit``.

    Args:
        match_type: Filter to 'dedup' or 'transfer'. Default None returns both.
        limit: Maximum rows (default 50).
    """
    with get_database(read_only=True) as db:
        svc = MatchingService(db)
        rows = svc.get_pending(match_type=match_type, limit=limit)
        total = svc.count_pending(match_type=match_type)
        # Count groups over the FULL pending queue (not just this page) so the
        # agent sees the true N-way cluster total even when has_more is true.
        # Pass the caller's filter through so the count matches the returned rows
        # (a transfer-scoped call reports 0 dedup groups, not the whole queue).
        n_dedup_groups = svc.count_pending_dedup_groups(match_type=match_type)

    return build_envelope(
        data=MatchesPendingPayload(
            n_dedup_groups=n_dedup_groups,
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
                    component_key=r["component_key"],
                )
                for r in rows
            ],
        ),
        total_count=total,
        actions=[
            "Use transactions_matches_set to accept or reject one match by match_id",
            "Group rows by component_key to review all edges of one N-way dedup "
            "cluster together",
            "For full pair context (both transactions side by side), use the CLI "
            "`moneybin transactions review --type matches` queue",
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
                    decided_at=r.get("decided_at"),
                )
                for r in rows
            ]
        ),
        actions=["Use transactions_matches_pending for the active queue"],
    )


@mcp_tool(domain="matches", read_only=False, idempotent=False)
def transactions_matches_run() -> ResponseEnvelope[MatchRunPayload]:
    """Run the matcher (dedup + transfer detection) over existing transactions.

    Operator-territory: a granular alternative to refresh_run. Writes new pending
    rows to app.match_decisions; review them with transactions_matches_pending and
    finalize each with transactions_matches_set. Does not auto-accept. Reverse an
    accepted match via `moneybin transactions matches undo` (no MCP undo tool yet).
    """
    with get_database(read_only=False) as db:
        result = MatchingService(db).run(actor="mcp")
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
        review,
        "review",
        "Return pending counts across all four review queues "
        "(matches, categorize, account-links, merchant-links). "
        "Call this to answer 'what needs my attention?' in one sweep. "
        "Drill into `transactions_matches_pending` for match proposals, "
        "`transactions_categorize_pending` for uncategorized transactions, "
        "and `accounts_links_pending` for account-link decisions.",
    )
    register(
        mcp,
        transactions_review,
        "transactions_review",
        "DEPRECATED: use `review` — removed after one minor release. "
        "Return pending counts for matches, categorize, account-links, and "
        "merchant-links queues. "
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
        "the confidence score is the decision signal. `summary.has_more` flags more "
        "beyond `limit`. Pair with transactions_matches_set to decide.",
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
