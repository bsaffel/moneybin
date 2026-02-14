"""MCP tool implementations for MoneyBin.

Tools are callable functions that AI assistants can invoke to query and
analyze financial data. Each tool is registered with the FastMCP server
via the @mcp.tool() decorator.

Live tools query real DuckDB data. Stub tools return a helpful
not-implemented message explaining what's needed to enable them.
"""

import json
import logging

from .privacy import (
    MAX_ROWS,
    check_table_allowed,
    not_implemented,
    truncate_result,
    validate_read_only_query,
)
from .server import (
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
    OFX_BALANCES,
    OFX_INSTITUTIONS,
    W2_FORMS,
    get_db,
    mcp,
    table_exists,
)

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
            "No accounts found. Run 'dbt run' to build the core data models "
            "after importing data with 'moneybin extract ofx'."
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
            "No transactions found. Run 'dbt run' to build the core data models "
            "after importing data with 'moneybin extract ofx'."
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


# ---------------------------------------------------------------------------
# Not-Yet-Implemented Tools — return helpful messages
# ---------------------------------------------------------------------------


@mcp.tool()
def get_spending_by_category(
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    """Get spending breakdown by category for a given period.

    Args:
        start_date: Start of period (YYYY-MM-DD).
        end_date: End of period (YYYY-MM-DD).
    """
    _ = start_date, end_date  # acknowledge params
    return not_implemented(
        "Spending by category",
        "1. Run 'moneybin transform run' to build the dbt categorization models\n"
        "2. Ensure transaction data has been imported via OFX or Plaid",
    )


@mcp.tool()
def get_monthly_summary(year: int | None = None) -> str:
    """Get income vs expenses summary by month.

    Args:
        year: Filter to a specific year. Defaults to current year.
    """
    _ = year
    return not_implemented(
        "Monthly income/expense summary",
        "1. Import transactions via 'moneybin extract ofx' or 'moneybin sync plaid'\n"
        "2. Run 'moneybin transform run' to build the unified fact table",
    )


@mcp.tool()
def get_investment_holdings() -> str:
    """Get current investment positions and values."""
    return not_implemented(
        "Investment holdings data",
        "1. Connect investment accounts via Plaid sync\n"
        "2. Run 'moneybin sync plaid' with an institution that supports investments",
    )


@mcp.tool()
def get_investment_performance(
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    """Get portfolio returns over a time period.

    Args:
        start_date: Start of period (YYYY-MM-DD).
        end_date: End of period (YYYY-MM-DD).
    """
    _ = start_date, end_date
    return not_implemented(
        "Investment performance tracking",
        "1. Connect investment accounts via Plaid sync\n"
        "2. Ensure securities and holdings data is being extracted\n"
        "3. Run 'moneybin transform run' to build performance models",
    )


@mcp.tool()
def get_liabilities_summary() -> str:
    """Get summary of outstanding debts, interest rates, and payments."""
    return not_implemented(
        "Liabilities data (credit cards, loans, mortgages)",
        "1. Connect liability accounts via Plaid sync\n"
        "2. Run 'moneybin sync plaid' with institutions that hold your debts",
    )


@mcp.tool()
def get_net_worth() -> str:
    """Calculate total net worth (assets minus liabilities)."""
    return not_implemented(
        "Net worth calculation",
        "1. Import all account data (banking, investments, liabilities)\n"
        "2. Run 'moneybin transform run' to build aggregation models\n"
        "3. Requires: accounts + investment holdings + liabilities data",
    )


@mcp.tool()
def get_balance_history(
    account_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    """Get balance trends over time for one or all accounts.

    Args:
        account_id: Optional account to filter.
        start_date: Start of period (YYYY-MM-DD).
        end_date: End of period (YYYY-MM-DD).
    """
    _ = account_id, start_date, end_date
    return not_implemented(
        "Balance history time series",
        "1. This feature requires recurring balance snapshots\n"
        "2. Import multiple OFX/QFX statements over time to build history\n"
        "3. Run 'moneybin transform run' to build the balance history model",
    )


@mcp.tool()
def find_recurring_transactions(min_occurrences: int = 3) -> str:
    """Identify recurring transactions like subscriptions and regular charges.

    Args:
        min_occurrences: Minimum number of times a transaction must appear.
    """
    _ = min_occurrences
    return not_implemented(
        "Recurring transaction detection",
        "1. Import sufficient transaction history (3+ months recommended)\n"
        "2. Run 'moneybin transform run' to build pattern detection models",
    )


@mcp.tool()
def get_budget_status(month: str | None = None) -> str:
    """Get budget vs actual spending comparison.

    Args:
        month: Month to check (YYYY-MM). Defaults to current month.
    """
    _ = month
    return not_implemented(
        "Budget tracking",
        "1. Budget feature has not been implemented yet\n"
        "2. Requires: transaction categorization + budget definition",
    )


@mcp.tool()
def get_tax_summary(tax_year: int | None = None) -> str:
    """Get comprehensive tax summary across all data sources for a year.

    Args:
        tax_year: The tax year to summarize. Defaults to most recent.
    """
    _ = tax_year
    return not_implemented(
        "Comprehensive tax summary across all sources",
        "1. Import W-2 forms via 'moneybin extract w2'\n"
        "2. Import 1099 forms (not yet supported)\n"
        "3. Import transaction data for deduction tracking\n"
        "4. Run 'moneybin transform run' to build tax summary models",
    )
