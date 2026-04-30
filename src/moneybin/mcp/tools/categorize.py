# src/moneybin/mcp/tools/categorize.py
"""Categorize namespace tools — rules, merchants, bulk categorization.

Tools:
    - categorize.categories — List categories (low)
    - categorize.rules — List categorization rules (low)
    - categorize.merchants — List merchant mappings (low)
    - categorize.stats — Categorization coverage stats (low)
    - categorize.uncategorized — Find uncategorized transactions (medium)
    - categorize.bulk — Bulk-assign categories (medium)
    - categorize.create_rules — Create categorization rules (low)
    - categorize.delete_rule — Soft-delete a rule (low)
    - categorize.create_merchants — Create merchant mappings (low)
    - categorize.create_category — Create a custom category (low)
    - categorize.toggle_category — Enable/disable a category (low)
    - categorize.seed — Seed default categories from taxonomy (low)
    - categorize.auto_review — List pending auto-rule proposals (medium)
    - categorize.auto_confirm — Approve/reject auto-rule proposals (medium)
    - categorize.auto_stats — Auto-rule health metrics (low)
"""

from __future__ import annotations

import logging
import typing
import uuid
from collections.abc import Mapping, Sequence

import duckdb

from moneybin.database import get_database
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.namespaces import NamespaceRegistry, ToolDefinition
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.auto_rule_service import AutoRuleService
from moneybin.services.categorization_service import (
    BulkCategorizationResult,
    CategorizationService,
    MatchType,
    SeedResult,
)
from moneybin.tables import (
    CATEGORIES,
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)

_VALID_MATCH_TYPES: frozenset[MatchType] = frozenset(typing.get_args(MatchType))


def _validate_match_type(match_type: str) -> MatchType:
    """Validate and narrow a match_type string at the MCP boundary.

    Args:
        match_type: Raw string from MCP tool input.

    Returns:
        Validated MatchType literal.

    Raises:
        ValueError: If match_type is not one of the valid values.
    """
    if match_type not in _VALID_MATCH_TYPES:
        raise ValueError(
            f"Invalid match_type: '{match_type}'. "
            f"Must be one of: {', '.join(sorted(_VALID_MATCH_TYPES))}"
        )
    return match_type  # type: ignore[return-value]  # validated above


# ---------------------------------------------------------------------------
# Read tools
# ---------------------------------------------------------------------------


@mcp_tool(sensitivity="low")
def categorize_categories(
    include_inactive: bool = False,
) -> ResponseEnvelope:
    """List all categories in the taxonomy.

    Returns category ID, name, subcategory, description, and active
    status. By default only active categories are returned.

    Args:
        include_inactive: Include disabled categories (default False).
    """
    db = get_database()
    if include_inactive:
        try:
            rows = db.execute(
                f"""
                SELECT category_id, category, subcategory, description,
                       is_default, is_active, plaid_detailed
                FROM {CATEGORIES.full_name}
                ORDER BY category, subcategory
                """
            ).fetchall()
        except duckdb.CatalogException:
            rows = []

        data = [
            {
                "category_id": r[0],
                "category": r[1],
                "subcategory": r[2],
                "description": r[3],
                "is_default": r[4],
                "is_active": r[5],
                "plaid_detailed": r[6],
            }
            for r in rows
        ]
    else:
        data = CategorizationService(db).get_active_categories()

    return build_envelope(
        data=data,
        sensitivity="low",
        actions=[
            "Use categorize.seed to populate default categories",
            "Use categorize.create_category to add a custom category",
        ],
    )


@mcp_tool(sensitivity="low")
def categorize_rules() -> ResponseEnvelope:
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
            """
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
            "Use categorize.create_rules to add new rules",
            "Use categorize.delete_rule to soft-delete a rule",
        ],
    )


@mcp_tool(sensitivity="low")
def categorize_merchants() -> ResponseEnvelope:
    """List all merchant name mappings.

    Returns merchant ID, raw pattern, match type, canonical name,
    and associated category. Merchant mappings normalize transaction
    descriptions and provide default categories.
    """
    db = get_database()
    try:
        rows = db.execute(
            f"""
            SELECT merchant_id, raw_pattern, match_type,
                   canonical_name, category, subcategory
            FROM {MERCHANTS.full_name}
            ORDER BY canonical_name
            """
        ).fetchall()
    except duckdb.CatalogException:
        rows = []

    data = [
        {
            "merchant_id": r[0],
            "raw_pattern": r[1],
            "match_type": r[2],
            "canonical_name": r[3],
            "category": r[4],
            "subcategory": r[5],
        }
        for r in rows
    ]
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=[
            "Use categorize.create_merchants to add new merchant mappings",
        ],
    )


@mcp_tool(sensitivity="low")
def categorize_stats() -> ResponseEnvelope:
    """Get categorization coverage statistics.

    Returns total transactions, categorized count, uncategorized count,
    percentage categorized, and breakdown by categorization source
    (user, ai, rule, plaid).
    """
    result = CategorizationService(get_database()).stats()
    return result.to_envelope()


@mcp_tool(sensitivity="medium")
def categorize_uncategorized(
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
            """,
        )
        columns = [desc[0] for desc in result.description]
        fetched = result.fetchmany(clamped_limit)
    except duckdb.CatalogException:
        return build_envelope(
            data=[],
            sensitivity="medium",
            actions=["Import data first using import.file"],
        )

    records = [dict(zip(columns, row, strict=False)) for row in fetched]
    return build_envelope(
        data=records,
        sensitivity="medium",
        actions=[
            "Use categorize.bulk to assign categories to these transactions",
            "Use categorize.create_rules to set up automatic categorization",
        ],
    )


# ---------------------------------------------------------------------------
# Write tools
# ---------------------------------------------------------------------------


@mcp_tool(sensitivity="medium")
def categorize_bulk(
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

    result = CategorizationService(get_database()).bulk_categorize(items)
    return result.to_envelope(len(items))


@mcp_tool(sensitivity="low")
def categorize_create_rules(
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

    db = get_database()
    created = 0
    skipped = 0
    error_details: list[dict[str, str]] = []

    for item in rules:
        name = str(item.get("name", "")).strip()
        pattern = str(item.get("merchant_pattern", "")).strip()
        category = str(item.get("category", "")).strip()
        if not name or not pattern or not category:
            skipped += 1
            error_details.append({
                "name": name or "(missing)",
                "reason": "Missing name, merchant_pattern, or category",
            })
            continue

        subcategory = str(item.get("subcategory", "")).strip() or None
        raw_match_type = str(item.get("match_type", "contains")).strip()
        try:
            match_type = _validate_match_type(raw_match_type)
        except ValueError:
            skipped += 1
            error_details.append({
                "name": name,
                "reason": f"Invalid match_type: {raw_match_type}",
            })
            continue

        min_amount = item.get("min_amount")
        max_amount = item.get("max_amount")
        account_id = item.get("account_id")
        priority = int(item.get("priority", 100) or 100)

        rule_id = uuid.uuid4().hex[:12]
        try:
            db.execute(
                f"""
                INSERT INTO {CATEGORIZATION_RULES.full_name}
                (rule_id, name, merchant_pattern, match_type,
                 min_amount, max_amount, account_id,
                 category, subcategory, priority, is_active,
                 created_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true,
                        'ai', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [
                    rule_id,
                    name,
                    pattern,
                    match_type,
                    min_amount,
                    max_amount,
                    account_id,
                    category,
                    subcategory,
                    priority,
                ],
            )
            created += 1
        except Exception:  # noqa: BLE001 — DuckDB raises untyped errors on constraint violations
            skipped += 1
            logger.exception(f"create_rules failed for {name!r}")
            error_details.append({
                "name": name,
                "reason": "Failed to create rule — check logs for details.",
            })

    return build_envelope(
        data={
            "created": created,
            "skipped": skipped,
            "error_details": error_details,
        },
        sensitivity="low",
        total_count=len(rules),
        actions=[
            "Use categorize.rules to review all rules",
        ],
    )


@mcp_tool(sensitivity="low")
def categorize_delete_rule(rule_id: str) -> ResponseEnvelope:
    """Soft-delete a categorization rule by setting it inactive.

    The rule remains in the database but will no longer be applied
    during auto-categorization.

    Args:
        rule_id: The rule ID to deactivate.
    """
    db = get_database()
    try:
        row = db.execute(
            f"""
            UPDATE {CATEGORIZATION_RULES.full_name}
            SET is_active = false, updated_at = CURRENT_TIMESTAMP
            WHERE rule_id = ?
            RETURNING rule_id
            """,
            [rule_id],
        ).fetchone()
        if row:
            return build_envelope(
                data={"rule_id": rule_id, "action": "deactivated"},
                sensitivity="low",
            )
        return build_envelope(
            data={"error": f"Rule {rule_id} not found"},
            sensitivity="low",
        )
    except Exception:  # noqa: BLE001 — DuckDB raises untyped errors
        logger.exception(f"delete_rule failed for {rule_id}")
        return build_envelope(
            data={"error": "Failed to delete rule — check logs for details."},
            sensitivity="low",
        )


@mcp_tool(sensitivity="low")
def categorize_create_merchants(
    merchants: list[dict[str, str | None]],
) -> ResponseEnvelope:
    """Create multiple merchant name mappings in one call.

    Each merchant dict should have ``raw_pattern`` and ``canonical_name``.
    Optional fields: ``match_type`` (default 'contains'), ``category``,
    ``subcategory``.

    Args:
        merchants: List of merchant mapping dicts.
    """
    if not merchants:
        return build_envelope(
            data={"created": 0, "skipped": 0, "error_details": []},
            sensitivity="low",
        )

    db = get_database()
    created = 0
    skipped = 0
    error_details: list[dict[str, str]] = []

    for item in merchants:
        raw_pattern = str(item.get("raw_pattern", "")).strip()
        canonical_name = str(item.get("canonical_name", "")).strip()
        if not raw_pattern or not canonical_name:
            skipped += 1
            error_details.append({
                "canonical_name": canonical_name or "(missing)",
                "reason": "Missing raw_pattern or canonical_name",
            })
            continue

        raw_match_type = str(item.get("match_type", "contains")).strip()
        try:
            match_type = _validate_match_type(raw_match_type)
        except ValueError:
            skipped += 1
            error_details.append({
                "canonical_name": canonical_name,
                "reason": f"Invalid match_type: {raw_match_type}",
            })
            continue

        category = str(item.get("category", "")).strip() or None
        subcategory = str(item.get("subcategory", "")).strip() or None

        try:
            CategorizationService(db).create_merchant(
                raw_pattern,
                canonical_name,
                match_type=match_type,
                category=category,
                subcategory=subcategory,
                created_by="ai",
            )
            created += 1
        except Exception:  # noqa: BLE001 — DuckDB raises untyped errors on constraint violations
            skipped += 1
            logger.exception(f"create_merchants failed for {canonical_name!r}")
            error_details.append({
                "canonical_name": canonical_name,
                "reason": "Failed to create merchant — check logs for details.",
            })

    return build_envelope(
        data={
            "created": created,
            "skipped": skipped,
            "error_details": error_details,
        },
        sensitivity="low",
        total_count=len(merchants),
        actions=[
            "Use categorize.merchants to review all merchant mappings",
        ],
    )


@mcp_tool(sensitivity="low")
def categorize_create_category(
    category: str,
    subcategory: str | None = None,
    description: str | None = None,
) -> ResponseEnvelope:
    """Create a custom category or subcategory.

    Categories created this way are marked as non-default and active.
    They can be toggled on/off with ``categorize.toggle_category``.

    Args:
        category: Primary category name (e.g. 'Childcare').
        subcategory: Optional subcategory (e.g. 'Daycare').
        description: Optional description of this category.
    """
    cat_id = uuid.uuid4().hex[:12]
    db = get_database()

    try:
        db.execute(
            f"""
            INSERT INTO {CATEGORIES.full_name}
            (category_id, category, subcategory, description,
             is_default, is_active, created_at)
            VALUES (?, ?, ?, ?, false, true, CURRENT_TIMESTAMP)
            """,
            [cat_id, category, subcategory, description],
        )
        sub = f" / {subcategory}" if subcategory else ""
        return build_envelope(
            data={
                "category_id": cat_id,
                "category": category,
                "subcategory": subcategory,
                "action": "created",
                "display": f"{category}{sub}",
            },
            sensitivity="low",
        )
    except duckdb.ConstraintException:
        sub = f" / {subcategory}" if subcategory else ""
        return build_envelope(
            data={"error": f"Category already exists: {category}{sub}"},
            sensitivity="low",
        )
    except Exception:  # noqa: BLE001 — DuckDB raises untyped errors
        logger.exception("create_category failed")
        return build_envelope(
            data={"error": "Failed to create category — check logs for details."},
            sensitivity="low",
        )


@mcp_tool(sensitivity="low")
def categorize_toggle_category(
    category_id: str,
    is_active: bool,
) -> ResponseEnvelope:
    """Enable or disable a category.

    Disabled categories are hidden from the taxonomy but existing
    categorizations using them are preserved.

    Args:
        category_id: The category ID to toggle (e.g. 'FND-COF').
        is_active: True to enable, False to disable.
    """
    db = get_database()
    try:
        row = db.execute(
            f"""
            UPDATE {CATEGORIES.full_name}
            SET is_active = ?
            WHERE category_id = ?
            RETURNING category_id
            """,
            [is_active, category_id],
        ).fetchone()
        if row:
            action = "enabled" if is_active else "disabled"
            return build_envelope(
                data={"category_id": category_id, "action": action},
                sensitivity="low",
            )
        return build_envelope(
            data={"error": f"Category {category_id} not found"},
            sensitivity="low",
        )
    except Exception:  # noqa: BLE001 — DuckDB raises untyped errors
        logger.exception("toggle_category failed")
        return build_envelope(
            data={"error": "Failed to toggle category — check logs for details."},
            sensitivity="low",
        )


@mcp_tool(sensitivity="low")
def categorize_seed() -> ResponseEnvelope:
    """Initialize default categories from the Plaid PFCv2 taxonomy.

    Copies ~100 default categories from the SQLMesh seed table into
    app.categories. Safe to call multiple times -- existing categories
    are not overwritten.
    """
    try:
        count = CategorizationService(get_database()).seed()
        return SeedResult(seeded_count=count).to_envelope()
    except Exception:  # noqa: BLE001 — DuckDB raises untyped errors
        logger.exception("seed_categories failed")
        return build_envelope(
            data={"error": "Failed to seed categories — check logs for details."},
            sensitivity="low",
        )


@mcp_tool(sensitivity="medium")
def categorize_auto_review(limit: int | None = None) -> ResponseEnvelope:
    """List pending auto-rule proposals.

    Returns proposed categorization rules awaiting review, including
    sample matching transactions and trigger counts.

    Args:
        limit: Maximum number of proposals to return. Defaults to the
            configured ``auto_rule_list_default_limit`` (100). The envelope
            ``summary.has_more`` flag indicates whether more proposals exist
            beyond the returned page.
    """
    try:
        result = AutoRuleService(get_database()).review(limit=limit)
    except Exception:  # noqa: BLE001 — DuckDB raises untyped errors
        logger.exception("categorize.auto_review failed")
        return build_envelope(
            data={"error": "Failed to load proposals — check logs for details."},
            sensitivity="medium",
        )
    return result.to_envelope()


@mcp_tool(sensitivity="medium")
def categorize_auto_confirm(
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
    try:
        result = AutoRuleService(get_database()).confirm(
            approve=approve or [],
            reject=reject or [],
        )
    except Exception:  # noqa: BLE001 — DuckDB raises untyped errors
        logger.exception("categorize.auto_confirm failed")
        return build_envelope(
            data={"error": "Failed to confirm proposals — check logs for details."},
            sensitivity="medium",
        )
    return result.to_envelope()


@mcp_tool(sensitivity="low")
def categorize_auto_stats() -> ResponseEnvelope:
    """Auto-rule health metrics.

    Returns counts of active auto-rules, pending proposals, and
    transactions categorized by auto-rules.
    """
    try:
        data = AutoRuleService(get_database()).stats()
    except Exception:  # noqa: BLE001 — DuckDB raises untyped errors
        logger.exception("categorize.auto_stats failed")
        return build_envelope(
            data={"error": "Failed to load auto-rule stats — check logs for details."},
            sensitivity="low",
        )
    return data.to_envelope()


def register_categorize_tools(
    registry: NamespaceRegistry,
) -> list[ToolDefinition]:
    """Register all categorize namespace tools with the registry."""
    tools = [
        ToolDefinition(
            name="categorize.categories",
            description="List all categories in the taxonomy.",
            fn=categorize_categories,
        ),
        ToolDefinition(
            name="categorize.rules",
            description="List all active categorization rules.",
            fn=categorize_rules,
        ),
        ToolDefinition(
            name="categorize.merchants",
            description="List all merchant name mappings.",
            fn=categorize_merchants,
        ),
        ToolDefinition(
            name="categorize.stats",
            description=(
                "Get categorization coverage statistics: total, "
                "categorized, uncategorized, percent, and breakdown by source."
            ),
            fn=categorize_stats,
        ),
        ToolDefinition(
            name="categorize.uncategorized",
            description=("Find transactions that have not been categorized yet."),
            fn=categorize_uncategorized,
        ),
        ToolDefinition(
            name="categorize.bulk",
            description=(
                "Assign categories to multiple transactions in one call. "
                "Auto-creates merchant mappings for future auto-categorization."
            ),
            fn=categorize_bulk,
        ),
        ToolDefinition(
            name="categorize.create_rules",
            description=(
                "Create multiple categorization rules for automatic "
                "transaction categorization."
            ),
            fn=categorize_create_rules,
        ),
        ToolDefinition(
            name="categorize.delete_rule",
            description="Soft-delete a categorization rule (set inactive).",
            fn=categorize_delete_rule,
        ),
        ToolDefinition(
            name="categorize.create_merchants",
            description=(
                "Create multiple merchant name mappings for description "
                "normalization and auto-categorization."
            ),
            fn=categorize_create_merchants,
        ),
        ToolDefinition(
            name="categorize.create_category",
            description="Create a custom category or subcategory.",
            fn=categorize_create_category,
        ),
        ToolDefinition(
            name="categorize.toggle_category",
            description="Enable or disable a category in the taxonomy.",
            fn=categorize_toggle_category,
        ),
        ToolDefinition(
            name="categorize.seed",
            description=(
                "Initialize default categories from the Plaid PFCv2 taxonomy."
            ),
            fn=categorize_seed,
        ),
        ToolDefinition(
            name="categorize.auto_review",
            description=(
                "List pending auto-rule proposals with sample transactions "
                "and trigger counts."
            ),
            fn=categorize_auto_review,
        ),
        ToolDefinition(
            name="categorize.auto_confirm",
            description=(
                "Batch approve/reject auto-rule proposals. Approved "
                "proposals become active rules and immediately categorize "
                "matching transactions."
            ),
            fn=categorize_auto_confirm,
        ),
        ToolDefinition(
            name="categorize.auto_stats",
            description=(
                "Auto-rule health: active count, pending proposals, "
                "transactions categorized."
            ),
            fn=categorize_auto_stats,
        ),
    ]
    for tool in tools:
        registry.register(tool)
    return tools
