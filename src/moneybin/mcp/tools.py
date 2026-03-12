"""MCP tool implementations for MoneyBin.

Tools are callable functions that AI assistants can invoke to query and
analyze financial data. Each tool is registered with the FastMCP server
via the @mcp.tool() decorator.
"""

import json
import logging

from moneybin.tables import (
    CATEGORIES,
    CATEGORIZATION_RULES,
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
    MERCHANTS,
    OFX_BALANCES,
    OFX_INSTITUTIONS,
    W2_FORMS,
)

from .privacy import (
    MAX_ROWS,
    check_table_allowed,
    truncate_result,
    validate_read_only_query,
)
from .server import get_db, mcp, table_exists

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _query_to_json(sql: str, params: list[object] | None = None) -> str:
    """Execute a query and return results as a JSON string.

    Args:
        sql: The SQL query to execute.
        params: Optional query parameters.

    Returns:
        JSON string of query results.
    """
    db = get_db()
    try:
        if params:
            result = db.execute(sql, params)
        else:
            result = db.execute(sql)

        columns = [desc[0] for desc in result.description]
        rows = result.fetchmany(MAX_ROWS)

        records = [dict(zip(columns, row, strict=False)) for row in rows]
        return truncate_result(json.dumps(records, indent=2, default=str))
    except Exception as e:
        logger.exception("Query failed: %s", sql)
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Live Tools — backed by real data in DuckDB
# ---------------------------------------------------------------------------


@mcp.tool()
def list_tables() -> str:
    """List all tables and views in the DuckDB database.

    Returns table name, schema, type (table/view), and estimated row count.
    Useful for discovering what data is available.
    """
    logger.info("Tool called: list_tables")
    sql = """
        SELECT
            table_schema,
            table_name,
            table_type,
            (SELECT COUNT(*) FROM information_schema.columns c
             WHERE c.table_schema = t.table_schema
               AND c.table_name = t.table_name) AS column_count
        FROM information_schema.tables t
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """
    return _query_to_json(sql)


@mcp.tool()
def describe_table(table_name: str, schema_name: str = "raw") -> str:
    """Show columns, data types, and row count for a specific table.

    Args:
        table_name: Name of the table to describe.
        schema_name: Schema containing the table (default: 'raw').
    """
    logger.info("Tool called: describe_table(%s.%s)", schema_name, table_name)

    error = check_table_allowed(f"{schema_name}.{table_name}")
    if error:
        return error

    sql = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
    """
    columns_json = _query_to_json(sql, [schema_name, table_name])

    # Get row count
    db = get_db()
    try:
        count_result = db.execute(
            f"""
            SELECT COUNT(*) AS row_count
            FROM "{schema_name}"."{table_name}"
            """
        ).fetchone()
        row_count = count_result[0] if count_result else 0
    except Exception:
        row_count = "unknown"

    return (
        f"Table: {schema_name}.{table_name}\n"
        f"Row count: {row_count}\n\n"
        f"Columns:\n{columns_json}"
    )


@mcp.tool()
def list_accounts() -> str:
    """List all known financial accounts with type and institution.

    Returns deduplicated accounts from the core dim_accounts table,
    which consolidates all sources (OFX, Plaid, etc.).
    """
    logger.info("Tool called: list_accounts")

    if not table_exists(DIM_ACCOUNTS):
        return (
            "No accounts found. Import data first with the import_file tool "
            "or 'moneybin extract ofx' — core tables are rebuilt automatically."
        )

    return _query_to_json(f"""
        SELECT account_id, account_type, institution_name,
            routing_number, source_system
        FROM {DIM_ACCOUNTS.full_name}
        ORDER BY institution_name, account_type
    """)


@mcp.tool()
def query_transactions(
    start_date: str | None = None,
    end_date: str | None = None,
    min_amount: float | None = None,
    max_amount: float | None = None,
    payee_pattern: str | None = None,
    account_id: str | None = None,
    limit: int = 100,
) -> str:
    """Search transactions with optional filters.

    Queries the canonical fct_transactions table which consolidates all
    sources (OFX, Plaid, etc.) into a single standardized format.

    Args:
        start_date: Filter transactions on or after this date (YYYY-MM-DD).
        end_date: Filter transactions on or before this date (YYYY-MM-DD).
        min_amount: Minimum transaction amount.
        max_amount: Maximum transaction amount.
        payee_pattern: SQL LIKE pattern to match description (e.g. '%AMAZON%').
        account_id: Filter to a specific account.
        limit: Maximum number of results (default 100, max 1000).
    """
    logger.info("Tool called: query_transactions")
    limit = min(limit, MAX_ROWS)

    if not table_exists(FCT_TRANSACTIONS):
        return (
            "No transactions found. Import data first with the import_file tool "
            "or 'moneybin extract ofx' — core tables are rebuilt automatically."
        )

    conditions: list[str] = []
    params: list[object] = []

    if start_date:
        conditions.append("transaction_date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("transaction_date <= ?")
        params.append(end_date)
    if min_amount is not None:
        conditions.append("amount >= ?")
        params.append(min_amount)
    if max_amount is not None:
        conditions.append("amount <= ?")
        params.append(max_amount)
    if payee_pattern:
        conditions.append("(description ILIKE ? OR memo ILIKE ?)")
        params.extend([payee_pattern, payee_pattern])
    if account_id:
        conditions.append("account_id = ?")
        params.append(account_id)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    params.append(limit)

    sql = f"""
        SELECT transaction_id, account_id, transaction_type,
            transaction_date, amount, description, memo,
            source_system
        FROM {FCT_TRANSACTIONS.full_name} {where}
        ORDER BY transaction_date DESC LIMIT ?
    """
    return _query_to_json(sql, params)


@mcp.tool()
def get_account_balances(account_id: str | None = None) -> str:
    """Get the most recent balance for each account.

    Args:
        account_id: Optional account ID to filter to a single account.
    """
    logger.info("Tool called: get_account_balances")

    if not table_exists(OFX_BALANCES):
        return (
            "No balance data found. "
            "Import OFX/QFX files with 'moneybin extract ofx' first."
        )

    conditions: list[str] = []
    params: list[object] = []

    if account_id:
        conditions.append("account_id = ?")
        params.append(account_id)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        WITH latest AS (
            SELECT
                account_id,
                ledger_balance,
                available_balance,
                ledger_balance_date,
                statement_end_date,
                ROW_NUMBER() OVER (
                    PARTITION BY account_id
                    ORDER BY ledger_balance_date DESC
                ) AS rn
            FROM {OFX_BALANCES.full_name}
            {where}
        )
        SELECT account_id, ledger_balance, available_balance,
               ledger_balance_date, statement_end_date
        FROM latest WHERE rn = 1
        ORDER BY account_id
    """
    return _query_to_json(sql, params if params else None)


@mcp.tool()
def list_institutions() -> str:
    """List all connected financial institutions from OFX data."""
    logger.info("Tool called: list_institutions")

    if not table_exists(OFX_INSTITUTIONS):
        return (
            "No institution data found. "
            "Import OFX/QFX files with 'moneybin extract ofx' first."
        )

    return _query_to_json(f"""
        SELECT organization, fid
        FROM {OFX_INSTITUTIONS.full_name}
        ORDER BY organization
    """)


@mcp.tool()
def get_w2_summary(tax_year: int | None = None) -> str:
    """Summarize W-2 tax form data including wages, taxes, and employer info.

    Args:
        tax_year: Filter to a specific tax year. If omitted, returns all years.
    """
    logger.info("Tool called: get_w2_summary")

    if not table_exists(W2_FORMS):
        return "No W-2 data found. Extract W-2 forms with 'moneybin extract w2' first."

    conditions: list[str] = []
    params: list[object] = []

    if tax_year is not None:
        conditions.append("tax_year = ?")
        params.append(tax_year)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT
            tax_year,
            employer_name,
            wages,
            federal_income_tax,
            social_security_wages,
            social_security_tax,
            medicare_wages,
            medicare_tax,
            state_local_info
        FROM {W2_FORMS.full_name}
        {where}
        ORDER BY tax_year DESC, employer_name
    """
    return _query_to_json(sql, params if params else None)


@mcp.tool()
def list_categories() -> str:
    """List all active transaction categories.

    Returns the category taxonomy including category ID, name, subcategory,
    and description. Use seed_categories first if no categories exist.
    """
    logger.info("Tool called: list_categories")

    if not table_exists(CATEGORIES):
        return "No categories found. Use seed_categories to initialize defaults."

    return _query_to_json(f"""
        SELECT category_id, category, subcategory, description,
               is_default, plaid_detailed
        FROM {CATEGORIES.full_name}
        WHERE is_active = true
        ORDER BY category, subcategory
    """)


@mcp.tool()
def list_categorization_rules() -> str:
    """List all active categorization rules.

    Shows rules that auto-categorize transactions based on merchant patterns,
    amount ranges, and account filters.
    """
    logger.info("Tool called: list_categorization_rules")

    if not table_exists(CATEGORIZATION_RULES):
        return "No categorization rules found."

    return _query_to_json(f"""
        SELECT rule_id, name, merchant_pattern, match_type,
               min_amount, max_amount, account_id,
               category, subcategory, priority, created_by
        FROM {CATEGORIZATION_RULES.full_name}
        WHERE is_active = true
        ORDER BY priority ASC, name
    """)


@mcp.tool()
def list_merchants() -> str:
    """List all known merchant mappings.

    Shows how raw transaction descriptions are mapped to canonical merchant
    names and default categories.
    """
    logger.info("Tool called: list_merchants")

    if not table_exists(MERCHANTS):
        return "No merchant mappings found."

    return _query_to_json(f"""
        SELECT merchant_id, raw_pattern, match_type, canonical_name,
               category, subcategory, created_by
        FROM {MERCHANTS.full_name}
        ORDER BY canonical_name
    """)


@mcp.tool()
def get_categorization_stats() -> str:
    """Get summary statistics about transaction categorization coverage.

    Shows total transactions, how many are categorized vs uncategorized,
    and a breakdown by categorization source (user, rule, ai, plaid).
    """
    logger.info("Tool called: get_categorization_stats")

    import json

    from moneybin.services.categorization_service import (
        get_categorization_stats as _get_stats,
    )

    db = get_db()
    stats = _get_stats(db)
    return json.dumps(stats, indent=2, default=str)


@mcp.tool()
def run_read_query(sql: str) -> str:
    """Execute an arbitrary read-only SQL query against the DuckDB database.

    This is a power-user tool for custom analysis. Only SELECT, WITH,
    DESCRIBE, SHOW, PRAGMA, and EXPLAIN statements are allowed.
    Write operations (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, etc.)
    are rejected.

    Args:
        sql: The SQL query to execute. Must be read-only.
    """
    logger.info("Tool called: run_read_query")

    error = validate_read_only_query(sql)
    if error:
        return f"Query rejected: {error}"

    return _query_to_json(sql)
