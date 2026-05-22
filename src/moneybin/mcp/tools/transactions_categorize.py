"""Transactions categorize namespace tools — rules, categorization, auto-rules."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Literal

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.adapters.categorize_adapters import (
    auto_accept_envelope,
    auto_review_envelope,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.categorize import (
    AutoAcceptPayload,
    AutoReviewPayload,
    AutoStatsPayload,
    CategorizeCommitPayload,
    CategorizeRulesPayload,
    CategorizeRunPayload,
    CategorizeStatsPayload,
    CategorizeStatsWithAutoPayload,
    CatPendingPayload,
    PendingTxnRow,
    RulesCreatePayload,
    RulesDeletePayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.account_service import AccountService
from moneybin.services.auto_rule_service import AutoRuleService
from moneybin.services.categorization import (
    CategorizationResult,
    CategorizationService,
    validate_items,
    validate_rule_items,
)

logger = logging.getLogger(__name__)


@mcp_tool(domain="categorize")
def transactions_categorize_rules() -> ResponseEnvelope[CategorizeRulesPayload]:
    """List all categorization rules.

    Returns rule ID, name, pattern, match type, category, priority,
    and active status. Rules are applied in priority order during import.
    """
    with get_database(read_only=True) as db:
        payload = CategorizationService(db).list_rules()
    return build_envelope(
        data=payload,
        actions=[
            "Use transactions_categorize_rules_create to add new rules",
            "Use transactions_categorize_rules_delete to soft-delete a rule",
        ],
    )


@mcp_tool(domain="categorize")
def transactions_categorize_stats(
    include_auto: bool = False,
) -> ResponseEnvelope[CategorizeStatsPayload | CategorizeStatsWithAutoPayload]:
    """Get categorization coverage statistics.

    Returns total transactions, categorized count, uncategorized count,
    percentage categorized, and breakdown by categorization source
    (user, ai, rule, plaid).

    Args:
        include_auto: When True, also return auto-rule health metrics
            (active auto-rules, pending proposals, transactions categorized
            by auto-rules). The response ``data`` becomes a
            ``CategorizeStatsWithAutoPayload`` (``{overall: {...}, auto: {...}}``)
            instead of the flat ``CategorizeStatsPayload`` shape. Default
            False returns the flat overall shape.
    """
    with get_database(read_only=True) as db:
        overall = CategorizationService(db).stats()
        if not include_auto:
            return build_envelope(
                data=overall.to_payload(),
                actions=[
                    "Use transactions_categorize_pending to see uncategorized transactions"
                ],
            )
        auto_data = AutoRuleService(db).stats()
    # include_auto=True: composite of overall coverage + auto-rule health, as a
    # typed payload so the annotation matches the runtime shape and the privacy
    # middleware derives the tier from real fields.
    return build_envelope(
        data=CategorizeStatsWithAutoPayload(
            overall=overall.to_payload(),
            auto=AutoStatsPayload(
                active_auto_rules=auto_data.active_auto_rules,
                pending_proposals=auto_data.pending_proposals,
                transactions_categorized=auto_data.transactions_categorized,
            ),
        ),
        actions=[
            "Use transactions_categorize_pending to see uncategorized transactions",
            "Use transactions_categorize_auto_review to review pending proposals",
        ],
    )


@mcp_tool(domain="categorize")
def transactions_categorize_pending(
    limit: int = 50,
    sort: Literal["date", "impact"] = "date",
    min_amount: Decimal = Decimal("0"),
    account: str | None = None,
) -> ResponseEnvelope[CatPendingPayload]:
    """Find transactions that have not been categorized yet.

    Returns uncategorized transactions from the curator-impact view (excludes
    transfer pairs and archived accounts). Use this to identify transactions
    that need manual or AI-assisted categorization.

    Amounts use the accounting convention: negative = expense, positive = income;
    transfers exempt. Amounts are in the currency named by
    ``summary.display_currency``.

    Args:
        limit: Maximum number of results (default 50, max 1000).
        sort: ``date`` (most recent first, default) or ``impact`` (ABS(amount)
            * age_days — highest-value/oldest transactions first).
        min_amount: Filter to ABS(amount) >= this value. Default 0 returns all.
        account: Filter to a specific account; accepts ``account_id`` or
            case-insensitive display_name. Ambiguous matches raise. Default
            None returns all accounts.
    """
    with get_database(read_only=True) as db:
        account_id: str | None = None
        if account is not None:
            account_id = AccountService(db).resolve_strict(account)
        records = CategorizationService(db).list_uncategorized_transactions(
            limit=min(limit, 1000),
            sort=sort,
            min_amount=min_amount,
            account_id=account_id,
        )
    if records is None:
        return build_envelope(
            data=CatPendingPayload(transactions=[]),
            actions=["Import data first using import_files"],
        )
    payload = CatPendingPayload(
        transactions=[
            PendingTxnRow(
                transaction_id=r["transaction_id"],
                transaction_date=str(r["txn_date"])
                if r.get("txn_date") is not None
                else None,
                amount=float(r["amount"]) if r.get("amount") is not None else None,
                description=r.get("description"),
                memo=None,
                account_id=r.get("account_id"),
                age_days=int(r["age_days"]) if r.get("age_days") is not None else None,
            )
            for r in records
        ]
    )
    return build_envelope(
        data=payload,
        actions=[
            "Use transactions_categorize_commit to commit categorizations for these transactions",
            "Use transactions_categorize_rules_create to set up automatic categorization",
        ],
    )


@mcp_tool(domain="categorize", read_only=False)
def transactions_categorize_commit(
    items: Sequence[Mapping[str, str | None]],
) -> ResponseEnvelope[CategorizeCommitPayload]:
    """Commit externally-decided categorizations for a batch of transactions.

    Each item should have ``transaction_id``, ``category``, and optionally
    ``subcategory`` and ``canonical_merchant_name``. Transactions that
    already have a category are overwritten (subject to source-precedence
    rules).

    Also auto-creates exemplar-only merchant mappings from each row's
    normalized match_text so future rows with the same match_text are
    categorized automatically via the merchant matcher. When
    ``canonical_merchant_name`` is provided, multiple rows with different
    match_text values are merged under one merchant identity by appending
    exemplars rather than spawning per-row merchants.

    Typical caller: an LLM that received redacted rows from
    transactions_categorize_assist, proposed categorizations, the user
    reviewed, and the LLM now persists the accepted decisions.

    Args:
        items: List of dicts with transaction_id, category, optional
            subcategory, and optional canonical_merchant_name.
    """
    if not items:
        empty = CategorizationResult(applied=0, skipped=0, errors=0, error_details=[])
        return build_envelope(
            data=empty.to_payload(),
            total_count=0,
            actions=[
                "Use transactions_categorize_rules to review auto-created rules",
                "Use transactions_categorize_pending to fetch the next batch",
            ],
        )

    validated, parse_errors = validate_items(items)
    with get_database() as db:
        result = CategorizationService(db).categorize_items(validated)
    result.merge_parse_errors(parse_errors)
    return build_envelope(
        data=result.to_payload(),
        total_count=len(items),
        actions=[
            "Use transactions_categorize_rules to review auto-created rules",
            "Use transactions_categorize_pending to fetch the next batch",
        ],
    )


@mcp_tool(domain="categorize", read_only=False)
def transactions_categorize_rules_create(
    rules: list[dict[str, str | float | int | None]], reapply: bool = False
) -> ResponseEnvelope[RulesCreatePayload]:
    """Create multiple categorization rules in one call.

    Each rule should have ``name``, ``merchant_pattern``, and ``category``.
    Optional fields: ``subcategory``, ``match_type`` (default 'contains'),
    ``min_amount``, ``max_amount``, ``account_id``, ``priority`` (default 100).

    Args:
        rules: List of rule dicts.
        reapply: If True, retroactively apply the new rules to all
            uncategorized transactions after the inserts commit. Default
            False; only future categorizations are affected.
    """
    validated, parse_errors = validate_rule_items(rules)
    with get_database() as db:
        result = CategorizationService(db).create_rules(validated, reapply=reapply)
    result.merge_parse_errors(parse_errors)
    return build_envelope(
        data=result.to_payload(),
        total_count=len(rules),
        actions=[
            "Use transactions_categorize_rules to review all rules",
        ],
    )


@mcp_tool(domain="categorize", read_only=False)
def transactions_categorize_rules_delete(
    rule_id: str, reapply: bool = False
) -> ResponseEnvelope[RulesDeletePayload]:
    """Soft-delete a categorization rule by setting it inactive.

    The rule remains in the database but will no longer be applied
    during auto-categorization.

    Args:
        rule_id: The rule ID to deactivate.
        reapply: If True, run categorize_pending after the deactivation so
            rows previously covered by lower-priority sources have a chance
            to be re-evaluated. Default False; existing categorizations are
            left untouched.
    """
    with get_database() as db:
        deactivated = CategorizationService(db).deactivate_rule(
            rule_id, reapply=reapply
        )
    if not deactivated:
        raise UserError(f"Rule {rule_id} not found", code="RULE_NOT_FOUND")
    return build_envelope(
        data=RulesDeletePayload(rule_id=rule_id, action="deactivated")
    )


@mcp_tool(domain="categorize")
def transactions_categorize_auto_review(
    limit: int | None = None,
) -> ResponseEnvelope[AutoReviewPayload]:
    """List pending auto-rule proposals.

    Returns proposed categorization rules awaiting review, including
    sample matching transactions and trigger counts.

    Args:
        limit: Maximum number of proposals to return. Defaults to the
            configured ``auto_rule_list_default_limit`` (100). The envelope
            ``summary.has_more`` flag indicates whether more proposals exist
            beyond the returned page.
    """
    with get_database(read_only=True) as db:
        result = AutoRuleService(db).review(limit=limit)
    return auto_review_envelope(result)


@mcp_tool(domain="categorize", read_only=False)
def transactions_categorize_auto_accept(
    accept: list[str] | None = None, reject: list[str] | None = None
) -> ResponseEnvelope[AutoAcceptPayload]:
    """Accept or reject auto-rule proposals by ID.

    Accepted proposals become active rules and immediately categorize
    matching transactions.

    Args:
        accept: Proposal IDs to accept and promote to active rules.
        reject: Proposal IDs to reject and dismiss.
    """
    with get_database() as db:
        result = AutoRuleService(db).accept(accept=accept or [], reject=reject or [])
    return auto_accept_envelope(result)


@mcp_tool(domain="categorize", read_only=False)
def transactions_categorize_run(
    methods: list[Literal["rules", "merchants"]] | None = None,
) -> ResponseEnvelope[CategorizeRunPayload]:
    """Run the categorization engine cascade over uncategorized transactions.

    Each method runs a deterministic engine: ``rules`` applies active
    user-authored pattern rules; ``merchants`` applies the stored merchant
    catalog. Engines run in the order given — an earlier engine's write
    blocks a later engine's write on the same row via source-precedence.
    The canonical order ``["rules", "merchants"]`` takes an optimized
    shared-scan path. Amounts use the accounting convention: negative =
    expense, positive = income; transfers exempt.

    Args:
        methods: Engines to run in the listed order. Defaults to
            ["rules", "merchants"].
    """
    with get_database() as db:
        data = CategorizationService(db).categorize_run(methods=methods)
    payload = CategorizeRunPayload(
        applied_by_method=data["applied_by_method"], total_applied=data["total_applied"]
    )
    return build_envelope(
        data=payload,
        actions=[
            "Use transactions_categorize_stats to check resulting coverage",
            "Use transactions_categorize_pending to see remaining uncategorized rows",
        ],
    )


def register_transactions_categorize_tools(mcp: FastMCP) -> None:
    """Register all transactions categorize namespace tools with the FastMCP server."""
    register(
        mcp,
        transactions_categorize_rules,
        "transactions_categorize_rules",
        "List all active categorization rules.",
    )
    register(
        mcp,
        transactions_categorize_stats,
        "transactions_categorize_stats",
        "Get categorization coverage statistics: total, categorized, uncategorized, "
        "percent, and breakdown by source. Pass include_auto=True to also include "
        "auto-rule health metrics (active rules, pending proposals, transactions "
        "categorized by auto-rules); the response data becomes "
        "{overall: {...}, auto: {...}} instead of the flat shape.",
    )
    register(
        mcp,
        transactions_categorize_pending,
        "transactions_categorize_pending",
        "Find transactions that have not been categorized yet. "
        "Excludes transfer pairs and archived accounts. "
        "sort='impact' ranks by ABS(amount)*age_days (largest-value/oldest first); "
        "sort='date' (default) orders by most recent first. "
        "Filter by min_amount (absolute value) and account (account_id or display_name). "
        "Amounts use the accounting convention: negative = expense, positive = income; transfers exempt. "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        transactions_categorize_commit,
        "transactions_categorize_commit",
        "Commit externally-decided categorizations for a batch of transactions. "
        "Auto-creates merchant mappings for future auto-categorization. "
        "Writes app.transaction_categories and app.user_merchants; revert by calling again with a different category.",
    )
    register(
        mcp,
        transactions_categorize_rules_create,
        "transactions_categorize_rules_create",
        "Create multiple categorization rules for automatic "
        "transaction categorization. Idempotent: rules are deduped against "
        "active rules by matcher+output (merchant_pattern, match_type, "
        "min/max_amount, account_id, category, subcategory); name and "
        "priority are metadata. Retries return the existing rule_id. "
        "Writes app.categorization_rules; revert with transactions_categorize_rules_delete (soft-delete sets active=False).",
    )
    register(
        mcp,
        transactions_categorize_rules_delete,
        "transactions_categorize_rules_delete",
        "Soft-delete a categorization rule (set inactive). "
        "Updates app.categorization_rules.active=False; the rule row is preserved and can be reactivated by re-creating with the same fields (no built-in reactivate tool).",
    )
    register(
        mcp,
        transactions_categorize_auto_review,
        "transactions_categorize_auto_review",
        "List pending auto-rule proposals with sample transactions and trigger counts.",
    )
    register(
        mcp,
        transactions_categorize_auto_accept,
        "transactions_categorize_auto_accept",
        "Batch accept/reject auto-rule proposals. Accepted "
        "proposals become active rules and immediately categorize "
        "matching transactions. "
        "Writes app.categorization_rules and app.transaction_categories; revert accepted rules with transactions_categorize_rules_delete (rejected proposals cannot be un-rejected).",
    )
    register(
        mcp,
        transactions_categorize_run,
        "transactions_categorize_run",
        "Run the categorization engine cascade (rules and/or merchants) over uncategorized transactions. "
        "Amounts use the accounting convention: negative = expense, positive = income; transfers exempt. "
        "Writes app.transaction_categories via the named engine(s); revert by calling transactions_categorize_commit with a different category, or by soft-deleting the source rule via transactions_categorize_rules_delete(reapply=True).",
    )
