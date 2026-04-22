"""MCP write tool implementations for MoneyBin.

These tools allow the AI to modify data: importing files, categorizing
transactions, and managing budgets. All writes go through the service
layer with privacy validation.
"""

import json
import logging
import uuid
from pathlib import Path

import duckdb

from moneybin.database import get_database
from moneybin.services.categorization_service import (
    MatchType,
    create_merchant,
    match_merchant,
    normalize_description,
)
from moneybin.services.categorization_service import (
    seed_categories as seed_categories_svc,
)
from moneybin.services.import_service import import_file as do_import
from moneybin.tables import (
    BUDGETS,
    CATEGORIES,
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    TRANSACTION_CATEGORIES,
)

from .server import mcp, table_exists

logger = logging.getLogger(__name__)

_VALID_MATCH_TYPES: set[MatchType] = {"exact", "contains", "regex"}


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
# Import
# ---------------------------------------------------------------------------


@mcp.tool()
def import_file(
    file_path: str,
    account_id: str | None = None,
    institution: str | None = None,
) -> str:
    """Import a financial data file into MoneyBin.

    Supported formats (detected automatically by extension):
      - .ofx / .qfx — OFX/Quicken bank statements
      - .pdf        — W-2 tax forms
      - .csv        — bank transaction exports (see CSV workflow below)

    CSV workflow:
      CSVs do not contain account identifiers, so ``account_id`` must be
      supplied by the caller (e.g. 'chase-checking-1234'). The institution
      profile is auto-detected from the file's header row; use
      ``csv_list_profiles`` to see what is installed.

      If this tool returns a "Could not auto-detect CSV format" error:
        1. Call ``csv_preview_file`` to inspect the headers and sample rows
        2. Determine the column mapping:
             - date_column and date_format (e.g. '%m/%d/%Y', '%Y-%m-%d')
             - description_column (payee / merchant name)
             - sign_convention: 'negative_is_expense' (single col, most common),
               'negative_is_income' (single col, inverted), or
               'split_debit_credit' (separate debit/credit columns)
             - amount_column (single-col) OR debit_column + credit_column
             - Optional: post_date_column, memo_column, category_column,
               type_column, balance_column, reference_column, check_number_column
        3. Propose a profile name (e.g. 'wellsfargo_checking') and confirm
           with the user; set header_signature to the minimal set of columns
           that uniquely fingerprint this institution's format
        4. Call ``csv_save_profile`` with the confirmed mapping
        5. Retry this tool with the original file_path and account_id

    Args:
        file_path: Absolute path to the file to import.
        account_id: Account identifier (required for CSV files).
        institution: Institution name (OFX) or CSV profile name (optional,
            auto-detects for CSV).
    """
    logger.info(f"Tool called: import_file({file_path})")

    # Expand ~ and resolve to canonical path (collapses '..' and follows
    # symlinks), then verify the result stays within the user's home directory.
    resolved = Path(file_path).expanduser().resolve()
    if not resolved.is_relative_to(Path.home()):
        return (
            "Error: file_path must be within the user's home directory. "
            "Path traversal and symlinks that escape the home directory are not allowed."
        )

    try:
        result = do_import(
            get_database(),
            str(resolved),
            account_id=account_id,
            institution=institution,
        )
        return result.summary()
    except FileNotFoundError as e:
        return f"Error: {e}"
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception(f"Import failed: {file_path}")
        return f"Import failed: {e}"


# ---------------------------------------------------------------------------
# Transaction categorization
# ---------------------------------------------------------------------------


@mcp.tool()
def categorize_transaction(
    transaction_id: str,
    category: str,
    subcategory: str | None = None,
    categorized_by: str = "user",
) -> str:
    """Assign a category to a transaction.

    Also auto-creates a merchant mapping if one doesn't exist for the
    transaction's description, so future similar transactions are
    categorized automatically.

    Args:
        transaction_id: The transaction ID to categorize.
        category: Category name (e.g. 'Food & Drink', 'Shopping').
        subcategory: Optional subcategory (e.g. 'Groceries', 'Restaurants').
        categorized_by: Who is categorizing: 'user' (default), 'ai', 'rule', 'plaid'.
    """
    logger.info(f"Tool called: categorize_transaction({transaction_id}, {category})")

    try:
        db = get_database()
        # Resolve merchant_id before inserting the category record
        merchant_id = None
        try:
            txn = db.execute(
                f"""
                SELECT description FROM {FCT_TRANSACTIONS.full_name}
                WHERE transaction_id = ?
                """,
                [transaction_id],
            ).fetchone()

            if txn and txn[0]:
                description = txn[0]
                existing = match_merchant(db, description)
                if existing:
                    merchant_id = existing["merchant_id"]
                else:
                    normalized = normalize_description(description)
                    if normalized:
                        merchant_id = create_merchant(
                            db,
                            normalized,
                            normalized,
                            match_type="contains",
                            category=category,
                            subcategory=subcategory,
                            created_by=categorized_by,
                        )
        except Exception:
            logger.debug(
                "Could not resolve merchant mapping",
                exc_info=True,
            )

        db.execute(
            f"""
            INSERT OR REPLACE INTO {TRANSACTION_CATEGORIES.full_name}
            (transaction_id, category, subcategory,
             categorized_at, categorized_by, merchant_id)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            """,
            [transaction_id, category, subcategory, categorized_by, merchant_id],
        )

        sub = f" / {subcategory}" if subcategory else ""
        return f"Transaction {transaction_id} categorized as: {category}{sub}"
    except Exception as e:
        logger.exception("Categorization failed")
        return f"Error categorizing transaction: {e}"


@mcp.tool()
def get_uncategorized_transactions(limit: int = 50) -> str:
    """Find transactions that have not been categorized yet.

    Args:
        limit: Maximum number of results (default 50).
    """
    logger.info("Tool called: get_uncategorized_transactions")

    if not table_exists(FCT_TRANSACTIONS):
        return "No transactions found. Import data first."

    db = get_database()
    limit = min(limit, 1000)

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
            """,
            [limit],
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row, strict=False)) for row in rows]
        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("Query failed")
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Category taxonomy management
# ---------------------------------------------------------------------------


@mcp.tool()
def seed_categories() -> str:
    """Initialize default categories from the Plaid PFCv2 taxonomy.

    Copies ~100 default categories from the SQLMesh seed table into
    user.categories. Safe to call multiple times — existing categories
    are not overwritten.

    Requires SQLMesh transforms to have been run at least once so the
    seeds.categories table exists.
    """
    logger.info("Tool called: seed_categories")

    try:
        db = get_database()
        count = seed_categories_svc(db)
        return f"Seeded {count} new categories."
    except Exception as e:
        logger.exception("Failed to seed categories")
        return f"Error seeding categories: {e}"


@mcp.tool()
def toggle_category(category_id: str, is_active: bool) -> str:
    """Enable or disable a category.

    Disabled categories are hidden from the taxonomy but existing
    categorizations using them are preserved.

    Args:
        category_id: The category ID to toggle (e.g. 'FND-COF').
        is_active: True to enable, False to disable.
    """
    logger.info(f"Tool called: toggle_category({category_id}, {is_active})")

    try:
        db = get_database()
        row = db.execute(
            f"""
            UPDATE {CATEGORIES.full_name}
            SET is_active = ?
            WHERE category_id = ?
            RETURNING category_id
            """,
            [is_active, category_id],
        ).fetchone()
        if row is not None:
            action = "enabled" if is_active else "disabled"
            return f"Category {category_id} {action}."
        return f"Category {category_id} not found."
    except Exception as e:
        logger.exception("Toggle category failed")
        return f"Error: {e}"


@mcp.tool()
def create_category(
    category: str,
    subcategory: str | None = None,
    description: str | None = None,
) -> str:
    """Create a custom category or subcategory.

    Args:
        category: Primary category name (e.g. 'Childcare').
        subcategory: Optional subcategory (e.g. 'Daycare', 'Babysitter').
        description: Optional description of this category.
    """
    logger.info(f"Tool called: create_category({category}, {subcategory})")

    import uuid as _uuid

    cat_id = str(_uuid.uuid4())[:8].upper()

    try:
        db = get_database()
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
        return f"Created category: {category}{sub} (ID: {cat_id})"
    except duckdb.ConstraintException:
        sub = f" / {subcategory}" if subcategory else ""
        return f"Category already exists: {category}{sub}"
    except Exception as e:
        logger.exception("Create category failed")
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# Merchant management
# ---------------------------------------------------------------------------


@mcp.tool()
def create_merchant_mapping(
    raw_pattern: str,
    canonical_name: str,
    match_type: str = "contains",
    category: str | None = None,
    subcategory: str | None = None,
) -> str:
    """Create a merchant name mapping and optional category default.

    Merchant mappings normalize messy transaction descriptions (e.g.
    'SQ *STARBUCKS #1234 SEATTLE WA' -> 'Starbucks') and cache the
    merchant-to-category association for future auto-categorization.

    Args:
        raw_pattern: Pattern to match in descriptions.
        canonical_name: Clean merchant name for display.
        match_type: How to match: 'exact', 'contains' (default), or 'regex'.
        category: Optional default category for this merchant.
        subcategory: Optional default subcategory.
    """
    logger.info(f"Tool called: create_merchant_mapping({canonical_name})")

    try:
        validated_match_type = _validate_match_type(match_type)
    except ValueError as e:
        return f"Error: {e}"

    try:
        db = get_database()
        merchant_id = create_merchant(
            db,
            raw_pattern,
            canonical_name,
            match_type=validated_match_type,
            category=category,
            subcategory=subcategory,
            created_by="user",
        )
        cat_info = f" -> {category}" if category else ""
        return f"Created merchant: {canonical_name} (pattern: '{raw_pattern}'{cat_info}, ID: {merchant_id})"
    except Exception as e:
        logger.exception("Create merchant failed")
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# Categorization rules
# ---------------------------------------------------------------------------


@mcp.tool()
def create_categorization_rule(
    name: str,
    merchant_pattern: str,
    category: str,
    subcategory: str | None = None,
    match_type: str = "contains",
    min_amount: float | None = None,
    max_amount: float | None = None,
    account_id: str | None = None,
    priority: int = 100,
) -> str:
    """Create a rule for automatic transaction categorization.

    Rules match transactions by description pattern and optional conditions
    (amount range, account). They are applied in priority order during import.

    Args:
        name: Human-readable rule name (e.g. 'Starbucks -> Coffee').
        merchant_pattern: Pattern to match in transaction descriptions.
        category: Category to assign (e.g. 'Food & Drink').
        subcategory: Subcategory to assign (e.g. 'Coffee Shops').
        match_type: How to match: 'contains' (default), 'exact', or 'regex'.
        min_amount: Optional minimum amount filter (use negative for expenses).
        max_amount: Optional maximum amount filter (use negative for expenses).
        account_id: Optional account ID filter.
        priority: Rule priority (lower = higher priority, default 100).
    """
    logger.info(f"Tool called: create_categorization_rule({name})")

    try:
        validated_match_type = _validate_match_type(match_type)
    except ValueError as e:
        return f"Error: {e}"

    import uuid as _uuid

    rule_id = str(_uuid.uuid4())[:8]

    try:
        db = get_database()
        db.execute(
            f"""
            INSERT INTO {CATEGORIZATION_RULES.full_name}
            (rule_id, name, merchant_pattern, match_type,
             min_amount, max_amount, account_id,
             category, subcategory, priority, is_active,
             created_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true,
                    'user', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            [
                rule_id,
                name,
                merchant_pattern,
                validated_match_type,
                min_amount,
                max_amount,
                account_id,
                category,
                subcategory,
                priority,
            ],
        )
        return f"Created rule '{name}' (ID: {rule_id}, priority: {priority})"
    except Exception as e:
        logger.exception("Create rule failed")
        return f"Error: {e}"


@mcp.tool()
def delete_categorization_rule(rule_id: str) -> str:
    """Delete a categorization rule.

    Args:
        rule_id: The rule ID to delete.
    """
    logger.info(f"Tool called: delete_categorization_rule({rule_id})")

    try:
        db = get_database()
        db.execute(
            f"""
            DELETE FROM {CATEGORIZATION_RULES.full_name}
            WHERE rule_id = ?
            """,
            [rule_id],
        )
        return f"Deleted rule {rule_id}."
    except Exception as e:
        logger.exception("Delete rule failed")
        return f"Error: {e}"


# ---------------------------------------------------------------------------
# Bulk categorization
# ---------------------------------------------------------------------------


@mcp.tool()
def bulk_categorize(
    categorizations: list[dict[str, str]],
    create_merchant_mappings: bool = True,
) -> str:
    """Apply categorizations to multiple transactions in a single call.

    Use this after calling get_uncategorized_transactions: review the list,
    decide categories for each, then submit them all here in one call.
    High-confidence assignments also create merchant mappings so future
    imports are categorized automatically by the rule engine.

    Args:
        categorizations: List of dicts, each with:
            - transaction_id: the transaction to categorize
            - category: category name (e.g. 'Food & Drink')
            - subcategory: optional subcategory (e.g. 'Coffee Shops')
            - merchant_name: optional clean merchant name for mapping
        create_merchant_mappings: If True (default), auto-create merchant
            mappings from transaction descriptions so future similar
            transactions are categorized automatically.
    """
    logger.info(f"Tool called: bulk_categorize({len(categorizations)} items)")

    if not categorizations:
        return "No categorizations provided."

    categorized_count = 0
    merchant_count = 0
    errors: list[str] = []

    try:
        db = get_database()
        for item in categorizations:
            txn_id = item.get("transaction_id", "").strip()
            category = item.get("category", "").strip()
            if not txn_id or not category:
                errors.append(
                    f"Skipped item missing transaction_id or category: {item}"
                )
                continue

            subcategory = item.get("subcategory", "").strip() or None
            merchant_name = item.get("merchant_name", "").strip() or None

            # Resolve merchant_id before inserting
            merchant_id = None
            try:
                txn = db.execute(
                    f"""
                    SELECT description FROM {FCT_TRANSACTIONS.full_name}
                    WHERE transaction_id = ?
                    """,
                    [txn_id],
                ).fetchone()
                if txn and txn[0]:
                    existing = match_merchant(db, txn[0])
                    if existing:
                        merchant_id = existing["merchant_id"]
                    elif create_merchant_mappings and merchant_name:
                        normalized = normalize_description(txn[0])
                        if normalized:
                            merchant_id = create_merchant(
                                db,
                                normalized,
                                merchant_name,
                                match_type="contains",
                                category=category,
                                subcategory=subcategory,
                                created_by="ai",
                            )
                            merchant_count += 1
            except Exception:
                logger.debug(
                    "Could not resolve merchant mapping for %s",
                    txn_id,
                    exc_info=True,
                )

            db.execute(
                f"""
                INSERT OR REPLACE INTO {TRANSACTION_CATEGORIES.full_name}
                (transaction_id, category, subcategory,
                 categorized_at, categorized_by, merchant_id)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'ai', ?)
                """,
                [txn_id, category, subcategory, merchant_id],
            )
            categorized_count += 1

    except Exception as e:
        logger.exception("bulk_categorize failed")
        return f"Error: {e}"

    summary = f"Categorized {categorized_count} transactions"
    if merchant_count:
        summary += f", created {merchant_count} merchant mappings"
    if errors:
        summary += "\nWarnings:\n" + "\n".join(errors)
    return summary + "."


# ---------------------------------------------------------------------------
# Bulk rule & merchant creation
# ---------------------------------------------------------------------------


@mcp.tool()
def bulk_create_categorization_rules(
    rules: list[dict[str, str | float | int | None]],
) -> str:
    """Create multiple categorization rules in a single call.

    Use this after reviewing uncategorized transactions to set up rules
    that will automatically categorize future imports. Each rule matches
    transactions by description pattern and assigns a category.

    Args:
        rules: List of dicts, each with:
            - name: Human-readable rule name (e.g. 'Starbucks -> Coffee')
            - merchant_pattern: Pattern to match in descriptions
            - category: Category to assign
            - subcategory: optional subcategory
            - match_type: 'contains' (default), 'exact', or 'regex'
            - min_amount: optional minimum amount filter
            - max_amount: optional maximum amount filter
            - account_id: optional account ID filter
            - priority: rule priority (default 100, lower = higher priority)
    """
    logger.info(f"Tool called: bulk_create_categorization_rules({len(rules)} items)")

    if not rules:
        return "No rules provided."

    created_count = 0
    errors: list[str] = []

    try:
        db = get_database()
        for item in rules:
            name = str(item.get("name", "")).strip()
            pattern = str(item.get("merchant_pattern", "")).strip()
            category = str(item.get("category", "")).strip()
            if not name or not pattern or not category:
                errors.append(
                    f"Skipped rule missing name, merchant_pattern, or category: {item}"
                )
                continue

            subcategory = str(item.get("subcategory", "")).strip() or None
            raw_match_type = str(item.get("match_type", "contains")).strip()
            try:
                match_type = _validate_match_type(raw_match_type)
            except ValueError:
                errors.append(
                    f"Skipped rule with invalid match_type '{raw_match_type}': {item}"
                )
                continue
            min_amount = item.get("min_amount")
            max_amount = item.get("max_amount")
            account_id = item.get("account_id")
            priority = int(item.get("priority", 100) or 100)

            rule_id = str(uuid.uuid4())[:8]
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
            created_count += 1

    except Exception as e:
        logger.exception("bulk_create_categorization_rules failed")
        return f"Error: {e}"

    summary = f"Created {created_count} categorization rules"
    if errors:
        summary += "\nWarnings:\n" + "\n".join(errors)
    return summary + "."


@mcp.tool()
def bulk_create_merchant_mappings(
    mappings: list[dict[str, str | None]],
) -> str:
    """Create multiple merchant name mappings in a single call.

    Merchant mappings normalize messy transaction descriptions and cache
    the merchant-to-category association for automatic categorization of
    future imports. Use this after reviewing transactions to set up
    merchant recognition in bulk.

    Args:
        mappings: List of dicts, each with:
            - raw_pattern: Pattern to match in descriptions
            - canonical_name: Clean merchant name for display
            - match_type: 'contains' (default), 'exact', or 'regex'
            - category: optional default category
            - subcategory: optional default subcategory
    """
    logger.info(f"Tool called: bulk_create_merchant_mappings({len(mappings)} items)")

    if not mappings:
        return "No mappings provided."

    created_count = 0
    errors: list[str] = []

    try:
        db = get_database()
        for item in mappings:
            raw_pattern = str(item.get("raw_pattern", "")).strip()
            canonical_name = str(item.get("canonical_name", "")).strip()
            if not raw_pattern or not canonical_name:
                errors.append(
                    f"Skipped mapping missing raw_pattern or canonical_name: {item}"
                )
                continue

            raw_match_type = str(item.get("match_type", "contains")).strip()
            try:
                match_type = _validate_match_type(raw_match_type)
            except ValueError:
                errors.append(
                    f"Skipped mapping with invalid match_type '{raw_match_type}': {item}"
                )
                continue
            category = str(item.get("category", "")).strip() or None
            subcategory = str(item.get("subcategory", "")).strip() or None

            try:
                create_merchant(
                    db,
                    raw_pattern,
                    canonical_name,
                    match_type=match_type,
                    category=category,
                    subcategory=subcategory,
                    created_by="ai",
                )
                created_count += 1
            except Exception as e:
                errors.append(f"Failed to create mapping '{canonical_name}': {e}")

    except Exception as e:
        logger.exception("bulk_create_merchant_mappings failed")
        return f"Error: {e}"

    summary = f"Created {created_count} merchant mappings"
    if errors:
        summary += "\nWarnings:\n" + "\n".join(errors)
    return summary + "."


# ---------------------------------------------------------------------------
# Budget management
# ---------------------------------------------------------------------------


@mcp.tool()
def set_budget(
    category: str,
    monthly_amount: float,
    start_month: str | None = None,
) -> str:
    """Create or update a monthly budget for a category.

    Args:
        category: Budget category (should match transaction categories).
        monthly_amount: Monthly budget amount in dollars.
        start_month: Starting month (YYYY-MM). Defaults to current month.
    """
    logger.info(f"Tool called: set_budget({category})")

    db = get_database()

    if start_month is None:
        start_month = db.execute("SELECT STRFTIME(CURRENT_DATE, '%Y-%m')").fetchone()[0]  # type: ignore[index] — fetchone() returns a row here, not None

    try:
        # Check if budget already exists for this category
        existing = db.execute(
            """
            SELECT budget_id FROM app.budgets
            WHERE category = ? AND (end_month IS NULL OR end_month >= ?)
            """,
            [category, start_month],  # type: ignore[reportUnknownArgumentType] — DuckDB accepts list of mixed types
        ).fetchone()

        if existing:
            db.execute(
                """
                UPDATE app.budgets
                SET monthly_amount = ?, updated_at = CURRENT_TIMESTAMP
                WHERE budget_id = ?
                """,
                [monthly_amount, existing[0]],  # type: ignore[reportUnknownArgumentType] — DuckDB accepts list of mixed types
            )
            return f"Updated budget for '{category}': ${monthly_amount:.2f}/month"
        else:
            budget_id = str(uuid.uuid4())[:8]
            db.execute(
                """
                INSERT INTO app.budgets
                (budget_id, category, monthly_amount, start_month, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [budget_id, category, monthly_amount, start_month],  # type: ignore[reportUnknownArgumentType] — DuckDB accepts list of mixed types
            )
            return f"Created budget for '{category}': ${monthly_amount:.2f}/month starting {start_month}"
    except Exception as e:
        logger.exception("Budget operation failed")
        return f"Error setting budget: {e}"


@mcp.tool()
def get_budget_status(month: str | None = None) -> str:
    """Get budget vs actual spending comparison for a month.

    Args:
        month: Month to check (YYYY-MM). Defaults to current month.
    """
    logger.info("Tool called: get_budget_status")
    db = get_database()

    if not table_exists(BUDGETS):
        return "No budgets set yet. Use set_budget to create one."

    if month is None:
        month = db.execute("SELECT STRFTIME(CURRENT_DATE, '%Y-%m')").fetchone()[0]  # type: ignore[index] — fetchone() returns a row here, not None

    try:
        result = db.execute(
            f"""
            WITH spending AS (
                SELECT
                    c.category,
                    SUM(ABS(t.amount)) AS total_spent
                FROM {FCT_TRANSACTIONS.full_name} t
                JOIN app.transaction_categories c
                    ON t.transaction_id = c.transaction_id
                WHERE t.transaction_year_month = ?
                    AND t.amount < 0
                GROUP BY c.category
            )
            SELECT
                b.category,
                b.monthly_amount AS budget,
                COALESCE(s.total_spent, 0) AS spent,
                b.monthly_amount - COALESCE(s.total_spent, 0) AS remaining,
                CASE
                    WHEN COALESCE(s.total_spent, 0) > b.monthly_amount THEN 'OVER'
                    WHEN COALESCE(s.total_spent, 0) > b.monthly_amount * 0.9 THEN 'WARNING'
                    ELSE 'OK'
                END AS status
            FROM app.budgets b
            LEFT JOIN spending s ON b.category = s.category
            WHERE b.start_month <= ?
                AND (b.end_month IS NULL OR b.end_month >= ?)
            ORDER BY b.category
            """,
            [month, month, month],  # type: ignore[reportUnknownArgumentType] — DuckDB accepts list of str params
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row, strict=False)) for row in rows]

        if not records:
            return f"No active budgets found for {month}."

        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("Budget status query failed")
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Analytical tools
# ---------------------------------------------------------------------------


@mcp.tool()
def get_monthly_summary(months: int = 6) -> str:
    """Get income vs expenses summary by month.

    Args:
        months: Number of recent months to include (default 6).
    """
    logger.info("Tool called: get_monthly_summary")

    if not table_exists(FCT_TRANSACTIONS):
        return "No transactions found. Import data first."

    db = get_database()

    try:
        result = db.execute(
            f"""
            SELECT
                transaction_year_month,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS income,
                SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS expenses,
                SUM(amount) AS net,
                COUNT(*) AS transaction_count
            FROM {FCT_TRANSACTIONS.full_name}
            GROUP BY transaction_year_month
            ORDER BY transaction_year_month DESC
            LIMIT ?
            """,
            [months],
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row, strict=False)) for row in rows]
        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("Monthly summary query failed")
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_spending_by_category(month: str | None = None) -> str:
    """Get spending breakdown by category for a month.

    Requires transactions to be categorized first using categorize_transaction.

    Args:
        month: Month to analyze (YYYY-MM). Defaults to current month.
    """
    logger.info("Tool called: get_spending_by_category")

    if not table_exists(FCT_TRANSACTIONS):
        return "No transactions found. Import data first."

    if not table_exists(TRANSACTION_CATEGORIES):
        return "No categorized transactions. Use categorize_transaction first."

    db = get_database()

    if month is None:
        month = db.execute("SELECT STRFTIME(CURRENT_DATE, '%Y-%m')").fetchone()[0]  # type: ignore[index] — fetchone() returns a row here, not None

    try:
        result = db.execute(
            f"""
            SELECT
                c.category,
                c.subcategory,
                SUM(ABS(t.amount)) AS total_spent,
                COUNT(*) AS transaction_count
            FROM {FCT_TRANSACTIONS.full_name} t
            JOIN app.transaction_categories c
                ON t.transaction_id = c.transaction_id
            WHERE t.transaction_year_month = ?
                AND t.amount < 0
            GROUP BY c.category, c.subcategory
            ORDER BY total_spent DESC
            """,
            [month],  # type: ignore[reportUnknownArgumentType] — DuckDB accepts list of str params
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row, strict=False)) for row in rows]

        if not records:
            return f"No categorized spending found for {month}."

        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("Spending by category query failed")
        return json.dumps({"error": str(e)})


@mcp.tool()
def find_recurring_transactions(min_occurrences: int = 3) -> str:
    """Identify recurring transactions like subscriptions and regular charges.

    Looks for transactions with the same payee and similar amounts that
    appear multiple times.

    Args:
        min_occurrences: Minimum number of times a transaction must appear (default 3).
    """
    logger.info("Tool called: find_recurring_transactions")

    if not table_exists(FCT_TRANSACTIONS):
        return "No transactions found. Import data first."

    db = get_database()

    try:
        result = db.execute(
            f"""
            WITH payee_groups AS (
                SELECT
                    description,
                    ROUND(ABS(amount), 0) AS rounded_amount,
                    COUNT(*) AS occurrence_count,
                    MIN(transaction_date) AS first_seen,
                    MAX(transaction_date) AS last_seen,
                    ROUND(AVG(amount), 2) AS avg_amount
                FROM {FCT_TRANSACTIONS.full_name}
                WHERE amount < 0
                    AND description IS NOT NULL
                    AND description != ''
                GROUP BY description, ROUND(ABS(amount), 0)
                HAVING COUNT(*) >= ?
            )
            SELECT
                description,
                avg_amount,
                occurrence_count,
                first_seen,
                last_seen
            FROM payee_groups
            ORDER BY occurrence_count DESC, avg_amount DESC
            LIMIT 50
            """,
            [min_occurrences],
        )
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        records = [dict(zip(columns, row, strict=False)) for row in rows]

        if not records:
            return (
                f"No recurring transactions found with {min_occurrences}+ occurrences."
            )

        return json.dumps(records, indent=2, default=str)
    except Exception as e:
        logger.exception("Recurring transactions query failed")
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# CSV profile management
# ---------------------------------------------------------------------------


@mcp.tool()
def csv_preview_file(file_path: str) -> str:
    """Preview a CSV file's headers and first few rows.

    Use this to understand the structure of an unknown CSV before
    creating a column mapping profile. Returns headers and 3 sample rows.

    Args:
        file_path: Absolute path to the CSV file.
    """
    logger.info(f"Tool called: csv_preview_file({file_path})")

    import csv as csv_mod
    from pathlib import Path

    path = Path(file_path)
    if not path.exists():
        return f"Error: File not found: {file_path}"

    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv_mod.reader(f)
            headers = next(reader)
            sample_rows: list[list[str]] = []
            for i, row in enumerate(reader):
                if i >= 3:
                    break
                sample_rows.append(row)

        clean_headers = [h.strip() for h in headers]
        preview: dict[str, object] = {
            "file": path.name,
            "headers": clean_headers,
            "column_count": len(headers),
            "sample_rows": [
                dict(zip(clean_headers, row, strict=False)) for row in sample_rows
            ],
        }
        return json.dumps(preview, indent=2)
    except Exception as e:
        return f"Error reading CSV: {e}"


@mcp.tool()
def csv_list_profiles() -> str:
    """List all available CSV institution profiles.

    Shows built-in and user-created profiles with their institution names
    and header signatures for auto-detection.
    """
    logger.info("Tool called: csv_list_profiles")

    from moneybin.config import get_raw_data_path
    from moneybin.extractors.csv_profiles import load_profiles

    user_profiles_dir = get_raw_data_path().parent / "csv_profiles"
    profiles = load_profiles(user_profiles_dir)

    if not profiles:
        return json.dumps({
            "profiles": [],
            "message": "No profiles found. Use csv_save_profile to create one.",
        })

    profile_list: list[dict[str, object]] = []
    for name, profile in sorted(profiles.items()):
        profile_list.append({
            "name": name,
            "institution_name": profile.institution_name,
            "header_signature": profile.header_signature,
            "sign_convention": profile.sign_convention.value,
            "date_format": profile.date_format,
        })

    return json.dumps({"profiles": profile_list}, indent=2)


@mcp.tool()
def csv_save_profile(
    name: str,
    institution_name: str,
    header_signature: list[str],
    date_column: str,
    date_format: str,
    description_column: str,
    sign_convention: str,
    amount_column: str | None = None,
    debit_column: str | None = None,
    credit_column: str | None = None,
    post_date_column: str | None = None,
    memo_column: str | None = None,
    category_column: str | None = None,
    subcategory_column: str | None = None,
    type_column: str | None = None,
    status_column: str | None = None,
    check_number_column: str | None = None,
    reference_column: str | None = None,
    balance_column: str | None = None,
    member_name_column: str | None = None,
    skip_rows: int = 0,
    encoding: str = "utf-8",
) -> str:
    """Create or update a CSV institution profile for importing transactions.

    A profile maps an institution's CSV column names to MoneyBin's canonical schema.
    Once saved, the profile is auto-detected when importing CSVs with
    matching headers.

    For sign_convention, use one of:
      - "negative_is_expense": Single amount column, negative = expense (most common)
      - "negative_is_income": Single amount column, negative = income (rare)
      - "split_debit_credit": Separate debit and credit columns

    Args:
        name: Machine identifier (e.g. 'chase_credit', 'wellsfargo_checking').
        institution_name: Human-readable name (e.g. 'Chase', 'Wells Fargo').
        header_signature: Column names that uniquely identify this format.
        date_column: Column containing the transaction date.
        date_format: Date format string (e.g. '%m/%d/%Y').
        description_column: Column containing the transaction description.
        sign_convention: How amounts are represented (see above).
        amount_column: Single amount column (required unless split).
        debit_column: Debit column (required for split_debit_credit).
        credit_column: Credit column (required for split_debit_credit).
        post_date_column: Optional posting date column.
        memo_column: Optional memo/notes column.
        category_column: Optional category column.
        subcategory_column: Optional subcategory column.
        type_column: Optional transaction type column.
        status_column: Optional status column.
        check_number_column: Optional check number column.
        reference_column: Optional reference number column.
        balance_column: Optional running balance column.
        member_name_column: Optional member/account holder name column.
        skip_rows: Rows to skip before header (default 0).
        encoding: File encoding (default 'utf-8').
    """
    logger.info(f"Tool called: csv_save_profile({name})")

    from moneybin.config import get_raw_data_path
    from moneybin.extractors.csv_profiles import CSVProfile, save_profile

    user_profiles_dir = get_raw_data_path().parent / "csv_profiles"

    try:
        # Build kwargs, excluding None optional columns
        kwargs: dict[str, object] = {
            "name": name,
            "institution_name": institution_name,
            "header_signature": header_signature,
            "date_column": date_column,
            "date_format": date_format,
            "description_column": description_column,
            "sign_convention": sign_convention,
            "skip_rows": skip_rows,
            "encoding": encoding,
        }
        optional_fields = {
            "amount_column": amount_column,
            "debit_column": debit_column,
            "credit_column": credit_column,
            "post_date_column": post_date_column,
            "memo_column": memo_column,
            "category_column": category_column,
            "subcategory_column": subcategory_column,
            "type_column": type_column,
            "status_column": status_column,
            "check_number_column": check_number_column,
            "reference_column": reference_column,
            "balance_column": balance_column,
            "member_name_column": member_name_column,
        }
        for key, value in optional_fields.items():
            if value is not None:
                kwargs[key] = value

        profile = CSVProfile(**kwargs)  # type: ignore[arg-type] — kwargs built dynamically from validated MCP tool inputs
        output_path = save_profile(profile, user_profiles_dir)
        return f"Saved CSV profile '{name}' for {institution_name} to {output_path}"
    except Exception as e:
        return f"Error saving profile: {e}"
