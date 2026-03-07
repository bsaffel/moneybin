"""MCP write tool implementations for MoneyBin.

These tools allow the AI to modify data: importing files, categorizing
transactions, and managing budgets. All writes go through the service
layer with privacy validation.
"""

import json
import logging
import uuid

from .server import (
    BUDGETS,
    FCT_TRANSACTIONS,
    TRANSACTION_CATEGORIES,
    get_db,
    get_write_db,
    mcp,
    table_exists,
)

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
        with get_write_db() as db:
            result = do_import(db, file_path)
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
) -> str:
    """Assign a category to a transaction.

    Args:
        transaction_id: The transaction ID to categorize.
        category: Category name (e.g. 'Food', 'Housing', 'Transportation').
        subcategory: Optional subcategory (e.g. 'Groceries', 'Restaurants').
    """
    logger.info("Tool called: categorize_transaction(%s, %s)", transaction_id, category)

    try:
        with get_write_db() as db:
            db.execute(
                """
                INSERT OR REPLACE INTO "user".transaction_categories
                (transaction_id, category, subcategory, categorized_at, categorized_by)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'ai')
                """,
                [transaction_id, category, subcategory],
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
            LEFT JOIN "user".transaction_categories c
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
                SELECT budget_id FROM "user".budgets
                WHERE category = ? AND (end_month IS NULL OR end_month >= ?)
                """,
                [category, start_month],  # type: ignore[reportUnknownArgumentType]
            ).fetchone()

            if existing:
                db.execute(
                    """
                    UPDATE "user".budgets
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
                    INSERT INTO "user".budgets
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
                JOIN "user".transaction_categories c
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
            FROM "user".budgets b
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
            JOIN "user".transaction_categories c
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
