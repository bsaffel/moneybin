"""Transactions categorize namespace tools — rules, bulk categorization, auto-rules."""

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
from moneybin.services.categorization_service import (
    BulkCategorizationResult,
    CategorizationService,
    validate_bulk_items,
    validate_rule_items,
)

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="low", domain="categorize")
def transactions_categorize_rules_list() -> ResponseEnvelope:
    """List all categorization rules.

    Returns rule ID, name, pattern, match type, category, priority,
    and active status. Rules are applied in priority order during import.
    """
    data = CategorizationService(get_database()).list_rules()
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=[
            "Use transactions_categorize_rules_create to add new rules",
            "Use transactions_categorize_rule_delete to soft-delete a rule",
        ],
    )


@mcp_tool(sensitivity="low", domain="categorize")
def transactions_categorize_stats() -> ResponseEnvelope:
    """Get categorization coverage statistics.

    Returns total transactions, categorized count, uncategorized count,
    percentage categorized, and breakdown by categorization source
    (user, ai, rule, plaid).
    """
    result = CategorizationService(get_database()).stats()
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
    records = CategorizationService(get_database()).list_uncategorized_transactions(
        limit=min(limit, 1000)
    )
    if records is None:
        return build_envelope(
            data=[],
            sensitivity="medium",
            actions=["Import data first using import_file"],
        )
    return build_envelope(
        data=records,
        sensitivity="medium",
        actions=[
            "Use transactions_categorize_apply to assign categories to these transactions",
            "Use transactions_categorize_rules_create to set up automatic categorization",
        ],
    )


@mcp_tool(sensitivity="medium", domain="categorize")
def transactions_categorize_apply(
    items: Sequence[Mapping[str, str | None]],
) -> ResponseEnvelope:
    """Assign categories to multiple transactions in one call.

    Renamed from transactions_categorize_bulk_apply — the _bulk suffix was
    redundant per mcp-server.md batch-first principle (all collection ops accept
    lists by default).

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
        return BulkCategorizationResult(
            applied=0, skipped=0, errors=0, error_details=[]
        ).to_envelope(0)

    validated, parse_errors = validate_bulk_items(items)
    result = CategorizationService(get_database()).bulk_categorize(validated)
    result.merge_parse_errors(parse_errors)
    return result.to_envelope(len(items))


@mcp_tool(sensitivity="low", domain="categorize")
def transactions_categorize_rules_create(
    rules: list[dict[str, str | float | int | None]],
) -> ResponseEnvelope:
    """Create multiple categorization rules in one call.

    Each rule should have ``name``, ``merchant_pattern``, and ``category``.
    Optional fields: ``subcategory``, ``match_type`` (default 'contains'),
    ``min_amount``, ``max_amount``, ``account_id``, ``priority`` (default 100).

    Args:
        rules: List of rule dicts.
    """
    validated, parse_errors = validate_rule_items(rules)
    result = CategorizationService(get_database()).create_rules(validated)
    result.merge_parse_errors(parse_errors)
    return result.to_envelope(len(rules))


@mcp_tool(sensitivity="low", domain="categorize")
def transactions_categorize_rule_delete(rule_id: str) -> ResponseEnvelope:
    """Soft-delete a categorization rule by setting it inactive.

    The rule remains in the database but will no longer be applied
    during auto-categorization.

    Args:
        rule_id: The rule ID to deactivate.
    """
    if not CategorizationService(get_database()).deactivate_rule(rule_id):
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
    result = AutoRuleService(get_database()).review(limit=limit)
    return auto_review_envelope(result)


@mcp_tool(sensitivity="medium", domain="categorize")
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
    result = AutoRuleService(get_database()).accept(
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
    data = AutoRuleService(get_database()).stats()
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
        "Find transactions that have not been categorized yet.",
    )
    register(
        mcp,
        transactions_categorize_apply,
        "transactions_categorize_apply",
        "Assign categories to multiple transactions in one call. "
        "Auto-creates merchant mappings for future auto-categorization.",
    )
    register(
        mcp,
        transactions_categorize_rules_create,
        "transactions_categorize_rules_create",
        "Create multiple categorization rules for automatic "
        "transaction categorization.",
    )
    register(
        mcp,
        transactions_categorize_rule_delete,
        "transactions_categorize_rule_delete",
        "Soft-delete a categorization rule (set inactive).",
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
        "matching transactions.",
    )
    register(
        mcp,
        transactions_categorize_auto_stats,
        "transactions_categorize_auto_stats",
        "Auto-rule health: active count, pending proposals, transactions categorized.",
    )
