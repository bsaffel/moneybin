"""Transactions categorize namespace tools — rules, categorization, auto-rules."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.adapters.categorize_adapters import (
    auto_accept_envelope,
    auto_review_envelope,
    auto_stats_envelope,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.auto_rule_service import AutoRuleService
from moneybin.services.categorization import (
    CategorizationResult,
    CategorizationService,
    validate_items,
    validate_rule_items,
)

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="low", domain="categorize")
def transactions_categorize_rules_list() -> ResponseEnvelope:
    """List all categorization rules.

    Returns rule ID, name, pattern, match type, category, priority,
    and active status. Rules are applied in priority order during import.
    """
    with get_database(read_only=True) as db:
        data = CategorizationService(db).list_rules()
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=[
            "Use transactions_categorize_rules_create to add new rules",
            "Use transactions_categorize_rules_delete to soft-delete a rule",
        ],
    )


@mcp_tool(sensitivity="low", domain="categorize")
def transactions_categorize_stats() -> ResponseEnvelope:
    """Get categorization coverage statistics.

    Returns total transactions, categorized count, uncategorized count,
    percentage categorized, and breakdown by categorization source
    (user, ai, rule, plaid).
    """
    with get_database(read_only=True) as db:
        result = CategorizationService(db).stats()
    return result.to_envelope()


@mcp_tool(sensitivity="medium", domain="categorize")
def transactions_categorize_pending_list(
    limit: int = 50,
) -> ResponseEnvelope:
    """Find transactions that have not been categorized yet.

    Returns transaction details for uncategorized transactions,
    ordered by date descending. Use this to identify transactions
    that need manual or AI-assisted categorization.

    Args:
        limit: Maximum number of results (default 50, max 1000).
    """
    with get_database(read_only=True) as db:
        records = CategorizationService(db).list_uncategorized_transactions(
            limit=min(limit, 1000)
        )
    if records is None:
        return build_envelope(
            data=[],
            sensitivity="medium",
            actions=["Import data first using import_files"],
        )
    return build_envelope(
        data=records,
        sensitivity="medium",
        actions=[
            "Use transactions_categorize_apply to assign categories to these transactions",
            "Use transactions_categorize_rules_create to set up automatic categorization",
        ],
    )


@mcp_tool(sensitivity="medium", domain="categorize", read_only=False)
def transactions_categorize_apply(
    items: Sequence[Mapping[str, str | None]],
) -> ResponseEnvelope:
    """Assign categories to multiple transactions in one call.

    Each item should have ``transaction_id``, ``category``, and
    optionally ``subcategory`` and ``canonical_merchant_name``.
    Transactions that already have a category are overwritten (subject
    to source-precedence rules).

    Also auto-creates exemplar-only merchant mappings from the row's
    normalized match_text (description + memo) so future rows with the
    same match_text are categorized automatically via the oneOf
    set-membership matcher. When ``canonical_merchant_name`` is
    provided, multiple rows with different match_text values are
    merged under one merchant identity by appending exemplars rather
    than spawning per-row merchants.

    Args:
        items: List of dicts with transaction_id, category, optional
            subcategory, and optional canonical_merchant_name (the
            LLM-proposed display name used to merge exemplars).
    """
    if not items:
        return CategorizationResult(
            applied=0, skipped=0, errors=0, error_details=[]
        ).to_envelope(0)

    validated, parse_errors = validate_items(items)
    with get_database() as db:
        result = CategorizationService(db).categorize_items(validated)
    result.merge_parse_errors(parse_errors)
    return result.to_envelope(len(items))


@mcp_tool(sensitivity="low", domain="categorize", read_only=False)
def transactions_categorize_rules_create(
    rules: list[dict[str, str | float | int | None]],
    reapply: bool = False,
) -> ResponseEnvelope:
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
    return result.to_envelope(len(rules))


@mcp_tool(sensitivity="low", domain="categorize", read_only=False)
def transactions_categorize_rules_delete(
    rule_id: str, reapply: bool = False
) -> ResponseEnvelope:
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
        data={"rule_id": rule_id, "action": "deactivated"},
        sensitivity="low",
    )


@mcp_tool(sensitivity="medium", domain="categorize")
def transactions_categorize_auto_review(limit: int | None = None) -> ResponseEnvelope:
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


@mcp_tool(sensitivity="medium", domain="categorize", read_only=False)
def transactions_categorize_auto_accept(
    accept: list[str] | None = None,
    reject: list[str] | None = None,
) -> ResponseEnvelope:
    """Accept or reject auto-rule proposals by ID.

    Accepted proposals become active rules and immediately categorize
    matching transactions.

    Args:
        accept: Proposal IDs to accept and promote to active rules.
        reject: Proposal IDs to reject and dismiss.
    """
    with get_database() as db:
        result = AutoRuleService(db).accept(
            accept=accept or [],
            reject=reject or [],
        )
    return auto_accept_envelope(result)


@mcp_tool(sensitivity="low", domain="categorize")
def transactions_categorize_auto_stats() -> ResponseEnvelope:
    """Auto-rule health metrics.

    Returns counts of active auto-rules, pending proposals, and
    transactions categorized by auto-rules.
    """
    with get_database(read_only=True) as db:
        data = AutoRuleService(db).stats()
    return auto_stats_envelope(data)


def register_transactions_categorize_tools(mcp: FastMCP) -> None:
    """Register all transactions categorize namespace tools with the FastMCP server."""
    register(
        mcp,
        transactions_categorize_rules_list,
        "transactions_categorize_rules_list",
        "List all active categorization rules.",
    )
    register(
        mcp,
        transactions_categorize_stats,
        "transactions_categorize_stats",
        "Get categorization coverage statistics: total, "
        "categorized, uncategorized, percent, and breakdown by source.",
    )
    register(
        mcp,
        transactions_categorize_pending_list,
        "transactions_categorize_pending_list",
        "Find transactions that have not been categorized yet. "
        "Amounts use the accounting convention: negative = expense, positive = income; transfers exempt. "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        transactions_categorize_apply,
        "transactions_categorize_apply",
        "Assign categories to multiple transactions in one call. "
        "Auto-creates merchant mappings for future auto-categorization. "
        "Writes app.transaction_categories and app.user_merchants; revert by calling again with a different category, or by clearing via a follow-up apply.",
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
        transactions_categorize_auto_stats,
        "transactions_categorize_auto_stats",
        "Auto-rule health: active count, pending proposals, transactions categorized.",
    )
