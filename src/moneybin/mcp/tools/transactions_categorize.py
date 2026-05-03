"""Transactions categorize namespace tools — rules, bulk categorization, auto-rules."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence

import duckdb
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.adapters.categorize_adapters import (
    auto_confirm_envelope,
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
from moneybin.tables import (
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)


@mcp_tool(sensitivity="low", domain="categorize")
def transactions_categorize_rules_list() -> ResponseEnvelope:
    """List all categorization rules.

    Returns rule ID, name, pattern, match type, category, priority,
    and active status. Rules are applied in priority order during import.
    """
    db = get_database()
    try:
        rows = db.execute(
            f"""
            SELECT rule_id, name, merchant_pattern, match_type,
                   min_amount, max_amount, account_id,
                   category, subcategory, priority, is_active
            FROM {CATEGORIZATION_RULES.full_name}
            ORDER BY priority ASC, created_at ASC
            """  # noqa: S608  # TableRef constant, no user input
        ).fetchall()
    except duckdb.CatalogException:
        rows = []

    data = [
        {
            "rule_id": r[0],
            "name": r[1],
            "merchant_pattern": r[2],
            "match_type": r[3],
            "min_amount": r[4],
            "max_amount": r[5],
            "account_id": r[6],
            "category": r[7],
            "subcategory": r[8],
            "priority": r[9],
            "is_active": r[10],
        }
        for r in rows
    ]
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
    db = get_database()
    clamped_limit = min(limit, 1000)

    try:
        result = db.execute(
            f"""
            SELECT t.transaction_id, t.transaction_date, t.amount,
                   t.description, t.memo, t.account_id
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            WHERE c.transaction_id IS NULL
            ORDER BY t.transaction_date DESC
            LIMIT ?
            """,  # noqa: S608  # TableRef constants, no user input
            [clamped_limit],
        )
        columns = [desc[0] for desc in result.description]
        fetched = result.fetchall()
    except duckdb.CatalogException:
        return build_envelope(
            data=[],
            sensitivity="medium",
            actions=["Import data first using import_file"],
        )

    records = [dict(zip(columns, row, strict=False)) for row in fetched]
    return build_envelope(
        data=records,
        sensitivity="medium",
        actions=[
            "Use transactions_categorize_bulk_apply to assign categories to these transactions",
            "Use transactions_categorize_rules_create to set up automatic categorization",
        ],
    )


@mcp_tool(sensitivity="medium", domain="categorize")
def transactions_categorize_bulk_apply(
    items: Sequence[Mapping[str, str | None]],
) -> ResponseEnvelope:
    """Assign categories to multiple transactions in one call.

    Each item should have ``transaction_id``, ``category``, and
    optionally ``subcategory``. Transactions that already have a
    category are overwritten.

    Also auto-creates merchant mappings from transaction descriptions
    so future similar transactions are categorized automatically.

    Args:
        items: List of dicts with transaction_id, category, subcategory.
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
    if not rules:
        return build_envelope(
            data={"created": 0, "skipped": 0, "error_details": []},
            sensitivity="low",
        )

    validated, parse_errors = validate_rule_items(rules)
    result = CategorizationService(get_database()).create_rules(validated)
    result.merge_parse_errors(parse_errors)

    return build_envelope(
        data={
            "created": result.created,
            "skipped": result.skipped,
            "error_details": result.error_details,
        },
        sensitivity="low",
        total_count=len(rules),
        actions=[
            "Use transactions_categorize_rules_list to review all rules",
        ],
    )


@mcp_tool(sensitivity="low", domain="categorize")
def transactions_categorize_rule_delete(rule_id: str) -> ResponseEnvelope:
    """Soft-delete a categorization rule by setting it inactive.

    The rule remains in the database but will no longer be applied
    during auto-categorization.

    Args:
        rule_id: The rule ID to deactivate.
    """
    db = get_database()
    row = db.execute(
        f"""
        UPDATE {CATEGORIZATION_RULES.full_name}
        SET is_active = false, updated_at = CURRENT_TIMESTAMP
        WHERE rule_id = ?
        RETURNING rule_id
        """,
        [rule_id],
    ).fetchone()
    if not row:
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
def transactions_categorize_auto_confirm(
    approve: list[str] | None = None,
    reject: list[str] | None = None,
) -> ResponseEnvelope:
    """Approve or reject auto-rule proposals by ID.

    Approved proposals become active rules and immediately categorize
    matching transactions.

    Args:
        approve: Proposal IDs to approve and promote to active rules.
        reject: Proposal IDs to reject and dismiss.
    """
    result = AutoRuleService(get_database()).confirm(
        approve=approve or [],
        reject=reject or [],
    )
    return auto_confirm_envelope(result)


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
        transactions_categorize_bulk_apply,
        "transactions_categorize_bulk_apply",
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
        transactions_categorize_auto_confirm,
        "transactions_categorize_auto_confirm",
        "Batch approve/reject auto-rule proposals. Approved "
        "proposals become active rules and immediately categorize "
        "matching transactions.",
    )
    register(
        mcp,
        transactions_categorize_auto_stats,
        "transactions_categorize_auto_stats",
        "Auto-rule health: active count, pending proposals, transactions categorized.",
    )
