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

import base64
import binascii
import json
from dataclasses import replace
from datetime import date
from decimal import Decimal
from typing import Annotated, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import BeforeValidator, Field

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.write_contracts import AnnotationRequest
from moneybin.privacy.payloads.transactions import (
    MatchesHistoryPayload,
    MatchesPendingPayload,
    MatchHistoryRow,
    MatchPendingRow,
    MatchRunPayload,
    MatchSetPayload,
    ReviewStatusPayload,
    TransactionAnnotationBatchPayload,
    TransactionAnnotationOutcome,
    TransactionGetPayload,
    TransactionRow,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.account_service import AccountService
from moneybin.services.categorization import CategorizationService
from moneybin.services.entity_reference import (
    AmbiguousEntity,
    EntityCandidate,
    MissingEntity,
    resolve_entity_reference,
)
from moneybin.services.matching_service import MatchingService
from moneybin.services.mutation_context import current_operation_id
from moneybin.services.transaction_service import (
    OperationalTransactionResult,
    TransactionGetResult,
    TransactionService,
)


def _decimal_from_json_number(value: object) -> Decimal:
    """Convert only real JSON numbers to an exact finite Decimal."""
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("amount filters must be JSON numbers")
    else:
        parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError("amount filters must be finite")
    return parsed


_JSONDecimal = Annotated[
    Decimal,
    BeforeValidator(
        _decimal_from_json_number,
        json_schema_input_type=int | float,
    ),
]


def _transaction_payload(
    result: TransactionGetResult | OperationalTransactionResult,
    *,
    next_cursor: str | None,
) -> TransactionGetPayload:
    """Map either shared service result into the canonical transaction payload."""
    return TransactionGetPayload(
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
        next_cursor=next_cursor,
    )


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
    payload = _transaction_payload(result, next_cursor=result.next_cursor)
    return build_envelope(
        data=payload,
        next_cursor=result.next_cursor,
        actions=[
            "Use transactions_get with the next_cursor value to fetch the next page",
            "Use reports_spending for category breakdowns",
            "Use transactions_categorize_commit to categorize uncategorized transactions",
        ],
    )


def _resolve_transaction_reference(
    reference: str,
    candidates: list[EntityCandidate],
    *,
    noun: Literal["account", "merchant"],
) -> str:
    """Resolve one transaction filter without echoing it in errors."""
    resolution = resolve_entity_reference(reference, candidates)
    if isinstance(resolution, AmbiguousEntity):
        raise UserError(
            f"The {noun} reference matches multiple {noun}s.",
            code="ENTITY_REFERENCE_AMBIGUOUS",
            details={"candidate_ids": list(resolution.candidate_ids)},
        )
    if isinstance(resolution, MissingEntity):
        raise UserError(
            f"The {noun} reference did not match a {noun}.",
            code="ENTITY_REFERENCE_NOT_FOUND",
            details={"candidate_ids": []},
        )
    return resolution.entity_id


def _resolve_transaction_account(reference: str, service: AccountService) -> str:
    """Resolve exact account IDs across history and names among active accounts."""
    rows = service.list_accounts(
        include_archived=True,
        type_filter=None,
    ).rows
    for row in rows:
        if row.account_id == reference:
            return row.account_id

    candidates = [
        EntityCandidate(
            entity_id=row.account_id,
            display_name=row.display_name or row.account_id,
            aliases=tuple(
                value
                for value in (
                    row.institution_name,
                    row.account_type,
                    row.account_subtype,
                )
                if value is not None
            ),
        )
        for row in rows
        if not row.archived
    ]
    return _resolve_transaction_reference(reference, candidates, noun="account")


def _transaction_merchant_candidates(
    service: CategorizationService,
) -> list[EntityCandidate]:
    """Project canonical merchants and raw aliases into the shared resolver."""
    return [
        EntityCandidate(
            entity_id=row.merchant_id,
            display_name=row.canonical_name,
            aliases=(row.raw_pattern,) if row.raw_pattern is not None else (),
        )
        for row in service.list_merchants().merchants
    ]


def _transaction_cursor(offset: int, filters: dict[str, object]) -> str:
    """Encode a transaction cursor bound to canonical query state."""
    raw = json.dumps(
        {"filters": filters, "offset": offset, "tool": "transactions"},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(raw).decode()


def _transaction_offset(
    cursor: str | None,
    *,
    filters: dict[str, object],
) -> int:
    """Decode a transaction cursor and reject cross-filter reuse."""
    if cursor is None:
        return 0
    try:
        decoded = base64.b64decode(cursor.encode(), altchars=b"-_", validate=True)
        value = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UserError(
            "Invalid pagination cursor.",
            code="TRANSACTION_CURSOR_INVALID",
        ) from exc
    if not isinstance(value, dict):
        raise UserError(
            "Invalid pagination cursor.",
            code="TRANSACTION_CURSOR_INVALID",
        )
    payload = cast(dict[str, Any], value)
    offset = payload.get("offset")
    if (
        set(payload) != {"filters", "offset", "tool"}
        or payload.get("filters") != filters
        or payload.get("tool") != "transactions"
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
    ):
        raise UserError(
            "Invalid pagination cursor.",
            code="TRANSACTION_CURSOR_INVALID",
        )
    return offset


def _transaction_period(start: date | None, end: date | None) -> str | None:
    """Render the selected transaction date window."""
    if start is not None and end is not None:
        return f"{start.isoformat()} to {end.isoformat()}"
    if start is not None:
        return f"from {start.isoformat()}"
    if end is not None:
        return f"through {end.isoformat()}"
    return None


def _transaction_actions(
    *,
    account: str | None,
    start: date | None,
    end: date | None,
    merchant: str | None,
    category: str | None,
    min_amount: Decimal | None,
    max_amount: Decimal | None,
    text: str | None,
    limit: int,
    next_cursor: str | None,
) -> list[str]:
    """Return operational hints with a complete continuation call."""
    actions = [
        "Use reports(report_id='core:spending') for category breakdowns",
        "Use transactions_categorize_commit to categorize uncategorized transactions",
    ]
    if next_cursor is not None:
        arguments: list[str] = []
        if account is not None:
            arguments.append(f"account={account!r}")
        if start is not None:
            arguments.append(f"start={start.isoformat()!r}")
        if end is not None:
            arguments.append(f"end={end.isoformat()!r}")
        if merchant is not None:
            arguments.append(f"merchant={merchant!r}")
        if category is not None:
            arguments.append(f"category={category!r}")
        if min_amount is not None:
            arguments.append(f"min_amount={str(min_amount)}")
        if max_amount is not None:
            arguments.append(f"max_amount={str(max_amount)}")
        if text is not None:
            arguments.append(f"text={text!r}")
        arguments.extend((f"limit={limit}", f"cursor={next_cursor!r}"))
        actions.append(f"Continue with transactions({', '.join(arguments)})")
    return actions


@mcp_tool(read_only=True)
def transactions_coarse(
    account: str | None = None,
    start: date | None = None,
    end: date | None = None,
    merchant: str | None = None,
    category: str | None = None,
    min_amount: _JSONDecimal | None = None,
    max_amount: _JSONDecimal | None = None,
    text: str | None = None,
    limit: Annotated[int, Field(strict=True, ge=1)] = 100,
    cursor: str | None = None,
) -> ResponseEnvelope[TransactionGetPayload]:
    """Query operational transactions with resolved filters and exact pagination."""
    if start is not None and end is not None and start > end:
        raise UserError(
            "Transaction start must not be after end.",
            code="TRANSACTION_DATE_RANGE_INVALID",
        )
    if min_amount is not None and max_amount is not None and min_amount > max_amount:
        raise UserError(
            "Transaction min_amount must not exceed max_amount.",
            code="TRANSACTION_AMOUNT_RANGE_INVALID",
        )

    with get_database(read_only=True) as db:
        account_id = (
            _resolve_transaction_account(
                account,
                AccountService(db),
            )
            if account is not None
            else None
        )
        merchant_id = (
            _resolve_transaction_reference(
                merchant,
                _transaction_merchant_candidates(CategorizationService(db)),
                noun="merchant",
            )
            if merchant is not None
            else None
        )
        filters: dict[str, object] = {
            "account_id": account_id,
            "category": category,
            "end": end.isoformat() if end is not None else None,
            "max_amount": str(max_amount) if max_amount is not None else None,
            "merchant_id": merchant_id,
            "min_amount": str(min_amount) if min_amount is not None else None,
            "start": start.isoformat() if start is not None else None,
            "text": text.casefold() if text is not None else None,
        }
        offset = _transaction_offset(cursor, filters=filters)
        result = TransactionService(db).query_operational(
            account_id=account_id,
            date_from=start.isoformat() if start is not None else None,
            date_to=end.isoformat() if end is not None else None,
            merchant_id=merchant_id,
            category=category,
            amount_min=min_amount,
            amount_max=max_amount,
            text=text,
            limit=limit,
            offset=offset,
        )

    next_cursor = (
        _transaction_cursor(offset + limit, filters)
        if result.total_count > offset + limit
        else None
    )
    payload = _transaction_payload(result, next_cursor=next_cursor)
    envelope = build_envelope(
        data=payload,
        total_count=result.total_count,
        returned_count=len(result.transactions),
        next_cursor=next_cursor,
        period=_transaction_period(start, end),
        actions=_transaction_actions(
            account=account,
            start=start,
            end=end,
            merchant=merchant,
            category=category,
            min_amount=min_amount,
            max_amount=max_amount,
            text=text,
            limit=limit,
            next_cursor=next_cursor,
        ),
    )
    return replace(
        envelope,
        summary=replace(envelope.summary, has_more=next_cursor is not None),
    )


def register_transaction_coarse_reads(mcp: FastMCP) -> None:
    """Register the dormant Plan 6 replacement operational transaction read."""
    register(
        mcp,
        transactions_coarse,
        "transactions",
        "Query operational transactions by resolved account or merchant, date, "
        "category, amount, and text with exact cursor pagination. Amounts use "
        "the accounting convention: negative = expense, positive = income; "
        "transfers exempt. Currency is named by summary.display_currency.",
        privacy_actor="transactions",
    )
    # Plan 6 removes transactions_get from the live registry. Largest and
    # anomalous transaction analysis remains in the reports catalog.


@mcp_tool(read_only=False)
def transactions_annotate_coarse(
    requests: list[AnnotationRequest],
) -> ResponseEnvelope[TransactionAnnotationBatchPayload]:
    """Atomically declare complete note, tag, split, and tag-rename states."""
    operation_id = current_operation_id()
    with get_database(read_only=False) as db:
        result = TransactionService(db).apply_annotations(
            requests,
            actor="mcp",
            operation_id=operation_id,
        )
    return build_envelope(
        data=TransactionAnnotationBatchPayload(
            applied_count=len(result.outcomes),
            operation_id=result.operation_id,
            outcomes=[
                TransactionAnnotationOutcome(
                    kind=outcome.kind,
                    target_ids=list(outcome.target_ids),
                    changed=outcome.changed,
                    operation_id=result.operation_id,
                )
                for outcome in result.outcomes
            ],
        ),
        actions=[
            "Use system_audit(view='detail', operation_id=...) to inspect this batch",
            "Use system_audit_undo(operation_id=...) to reverse this batch",
        ],
    )


def register_transaction_coarse_writes(mcp: FastMCP) -> None:
    """Register the dormant Plan 6 atomic transaction annotation batch."""
    register(
        mcp,
        transactions_annotate_coarse,
        "transactions_annotate",
        "Atomically declare complete note, tag, and split states or rename one tag "
        "globally. Every request is preflighted before any write; failure leaves "
        "the whole batch unchanged. Results retain request order and share one "
        "operation_id for audit inspection or system_audit_undo recovery.",
        privacy_actor="transactions_annotate",
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
    from moneybin.services.security_links_service import SecurityLinksService

    with get_database(read_only=True) as db:
        status = ReviewService(
            match_service=MatchingService(db=db),
            categorize_service=CategorizationService(db=db),
            account_links_service=AccountLinksService(db=db),
            merchant_links_service=MerchantLinksService(db=db),
            security_links_service=SecurityLinksService(db=db),
        ).status()

    return build_envelope(
        data=ReviewStatusPayload(
            matches_pending=status.matches_pending,
            categorize_pending=status.categorize_pending,
            account_links_pending=status.account_links_pending,
            merchant_links_pending=status.merchant_links_pending,
            security_links_pending=status.security_links_pending,
            total=status.total,
        ),
        actions=[
            "Use transactions_categorize_pending to fetch the categorize queue",
            "Use transactions_matches_pending to fetch the matches queue",
            "Use accounts_links_pending to fetch the account-links queue",
            "Use merchants_links_pending to fetch the merchant-links queue",
            "Use investments_securities_links_pending to fetch the security-links queue",
        ],
    )


@mcp_tool()
def review() -> ResponseEnvelope[ReviewStatusPayload]:
    """Return counts of pending reviews across all five queues.

    Orientation tool: call this to answer "what needs my attention?" in one call.
    Surfaces matches_pending, categorize_pending, account_links_pending,
    merchant_links_pending, and security_links_pending so the agent can decide
    which queue to drain first.
    For categorize, fetch items via ``transactions_categorize_pending``.
    For matches, fetch the queue via ``transactions_matches_pending`` and
    decide each pair with ``transactions_matches_set``.
    For account links, fetch the queue via ``accounts_links_pending`` and
    decide each group with ``accounts_links_set``.
    For merchant links, fetch the queue via ``merchants_links_pending`` and
    decide each group with ``merchants_links_set``.
    For security links, fetch the queue via
    ``investments_securities_links_pending`` and decide each group with
    ``investments_securities_links_set``.
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
        "`accounts_links_pending` for account-link decisions, "
        "and `merchants_links_pending` for merchant-link decisions.",
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
