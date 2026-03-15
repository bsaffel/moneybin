"""MCP write tool implementations for MoneyBin.

These tools allow the AI to modify data: importing files, categorizing
transactions, and managing budgets. All writes go through the service
layer with privacy validation.
"""

import json
import logging
import uuid

import duckdb

from moneybin.tables import (
    BUDGETS,
    CATEGORIES,
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    TRANSACTION_CATEGORIES,
)

from .server import get_db, get_db_path, get_write_db, mcp, table_exists

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------


@mcp.tool()
def import_file(file_path: str) -> str:
    """Import a financial data file into MoneyBin.

    Supports OFX/QFX bank statements and W-2 PDF forms. The file is
    automatically detected by extension, extracted, loaded into raw tables,
    and core tables are rebuilt.

    Args:
        file_path: Absolute path to the file to import.
    """
    logger.info("Tool called: import_file(%s)", file_path)

    from moneybin.services.import_service import import_file as do_import

    try:
        db_path = get_db_path()
        # Close read-only conn so import_service + SQLMesh can write;
        # get_write_db reopens read conn when the context exits.
        with get_write_db():
            result = do_import(db_path, file_path)
        return result.summary()
    except FileNotFoundError as e:
        return f"Error: {e}"
    except ValueError as e:
        return f"Error: {e}"
    except Exception as e:
        logger.exception("Import failed: %s", file_path)
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
    logger.info("Tool called: categorize_transaction(%s, %s)", transaction_id, category)

    from moneybin.services.categorization_service import (
        create_merchant,
        match_merchant,
        normalize_description,
    )

    try:
        with get_write_db() as db:
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

    db = get_db()
    limit = min(limit, 1000)

    try:
        result = db.execute(
            f"""
            SELECT t.transaction_id, t.transaction_date, t.amount,
                   t.description, t.memo, t.account_id
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN app.transaction_categories c
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

    from moneybin.services.categorization_service import (
        seed_categories as _seed,
    )

    try:
        with get_write_db() as db:
            count = _seed(db)
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
    logger.info("Tool called: toggle_category(%s, %s)", category_id, is_active)

    try:
        with get_write_db() as db:
            result = db.execute(
                f"""
                UPDATE {CATEGORIES.full_name}
                SET is_active = ?
                WHERE category_id = ?
                """,
                [is_active, category_id],
            )
            if result.fetchone is not None:
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
    logger.info("Tool called: create_category(%s, %s)", category, subcategory)

    import uuid as _uuid

    cat_id = str(_uuid.uuid4())[:8].upper()

    try:
        with get_write_db() as db:
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
    logger.info("Tool called: create_merchant_mapping(%s)", canonical_name)

    from moneybin.services.categorization_service import (
        create_merchant as _create,
    )

    try:
        with get_write_db() as db:
            merchant_id = _create(
                db,
                raw_pattern,
                canonical_name,
                match_type=match_type,
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
    logger.info("Tool called: create_categorization_rule(%s)", name)

    import uuid as _uuid

    rule_id = str(_uuid.uuid4())[:8]

    try:
        with get_write_db() as db:
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
                    match_type,
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
    logger.info("Tool called: delete_categorization_rule(%s)", rule_id)

    try:
        with get_write_db() as db:
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
    logger.info("Tool called: bulk_categorize(%d items)", len(categorizations))

    from moneybin.services.categorization_service import (
        create_merchant,
        match_merchant,
        normalize_description,
    )

    if not categorizations:
        return "No categorizations provided."

    categorized_count = 0
    merchant_count = 0
    errors: list[str] = []

    try:
        with get_write_db() as db:
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
    logger.info("Tool called: bulk_create_categorization_rules(%d items)", len(rules))

    if not rules:
        return "No rules provided."

    created_count = 0
    errors: list[str] = []

    try:
        with get_write_db() as db:
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
                match_type = str(item.get("match_type", "contains")).strip()
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
    logger.info("Tool called: bulk_create_merchant_mappings(%d items)", len(mappings))

    from moneybin.services.categorization_service import (
        create_merchant,
    )

    if not mappings:
        return "No mappings provided."

    created_count = 0
    errors: list[str] = []

    try:
        with get_write_db() as db:
            for item in mappings:
                raw_pattern = str(item.get("raw_pattern", "")).strip()
                canonical_name = str(item.get("canonical_name", "")).strip()
                if not raw_pattern or not canonical_name:
                    errors.append(
                        f"Skipped mapping missing raw_pattern or canonical_name: {item}"
                    )
                    continue

                match_type = str(item.get("match_type", "contains")).strip()
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
    logger.info("Tool called: set_budget(%s, %.2f)", category, monthly_amount)

    if start_month is None:
        read_db = get_db()
        start_month = read_db.execute(
            "SELECT STRFTIME(CURRENT_DATE, '%Y-%m')"
        ).fetchone()[0]  # type: ignore[index]

    try:
        with get_write_db() as db:
            # Check if budget already exists for this category
            existing = db.execute(
                """
                SELECT budget_id FROM app.budgets
                WHERE category = ? AND (end_month IS NULL OR end_month >= ?)
                """,
                [category, start_month],  # type: ignore[reportUnknownArgumentType]
            ).fetchone()

            if existing:
                db.execute(
                    """
                    UPDATE app.budgets
                    SET monthly_amount = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE budget_id = ?
                    """,
                    [monthly_amount, existing[0]],  # type: ignore[reportUnknownArgumentType]
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
                    [budget_id, category, monthly_amount, start_month],  # type: ignore[reportUnknownArgumentType]
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
    db = get_db()

    if not table_exists(BUDGETS):
        return "No budgets set yet. Use set_budget to create one."

    if month is None:
        month = db.execute("SELECT STRFTIME(CURRENT_DATE, '%Y-%m')").fetchone()[0]  # type: ignore[index]

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
            [month, month, month],  # type: ignore[reportUnknownArgumentType]
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

    db = get_db()

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

    db = get_db()

    if month is None:
        month = db.execute("SELECT STRFTIME(CURRENT_DATE, '%Y-%m')").fetchone()[0]  # type: ignore[index]

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
            [month],  # type: ignore[reportUnknownArgumentType]
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

    db = get_db()

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
