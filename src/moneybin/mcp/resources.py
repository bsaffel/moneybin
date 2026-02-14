"""MCP resource definitions for MoneyBin.

Resources provide read-only data endpoints that AI assistants can access
directly. They are registered with the FastMCP server via decorators.

Documentation: https://modelcontextprotocol.github.io/python-sdk/servers/resources/
"""

import json
import logging
from datetime import date, timedelta

from .privacy import MAX_ROWS, not_implemented, truncate_result
from .server import (
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
    OFX_BALANCES,
    W2_FORMS,
    get_db,
    mcp,
    table_exists,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema resources
# ---------------------------------------------------------------------------


@mcp.resource("moneybin://schema/tables")
def schema_tables() -> str:
    """List of all tables in the database with schema, type, and column count."""
    logger.info("Resource read: schema/tables")
    db = get_db()

    result = db.execute("""
        SELECT
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """)

    columns = [desc[0] for desc in result.description]
    rows = result.fetchmany(MAX_ROWS)
    records = [dict(zip(columns, row, strict=False)) for row in rows]
    return json.dumps(records, indent=2, default=str)


@mcp.resource("moneybin://schema/{table_name}")
def schema_table_detail(table_name: str) -> str:
    """Column definitions for a specific table.

    The table_name can be schema-qualified (e.g. 'raw.ofx_transactions')
    or just the table name (searches all schemas).
    """
    logger.info("Resource read: schema/%s", table_name)
    db = get_db()

    # Handle schema-qualified names
    if "." in table_name:
        schema, tbl = table_name.split(".", 1)
        where = "WHERE table_schema = ? AND table_name = ?"
        params: list[str] = [schema, tbl]
    else:
        where = "WHERE table_name = ?"
        params = [table_name]

    result = db.execute(
        f"""
        SELECT
            table_schema,
            table_name,
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        {where}
        ORDER BY table_schema, ordinal_position
        """,
        params,
    )

    columns = [desc[0] for desc in result.description]
    rows = result.fetchmany(MAX_ROWS)
    records = [dict(zip(columns, row, strict=False)) for row in rows]

    if not records:
        return json.dumps({"error": f"Table '{table_name}' not found"})

    return json.dumps(records, indent=2, default=str)


# ---------------------------------------------------------------------------
# Account resources
# ---------------------------------------------------------------------------


@mcp.resource("moneybin://accounts/summary")
def accounts_summary() -> str:
    """Account listing with latest balances from OFX data."""
    logger.info("Resource read: accounts/summary")
    db = get_db()

    if not table_exists(DIM_ACCOUNTS):
        return json.dumps({"message": "No account data loaded yet."})

    # Join accounts with latest balances
    has_balances = table_exists(OFX_BALANCES)

    if has_balances:
        result = db.execute(f"""
            WITH latest_balances AS (
                SELECT
                    account_id,
                    ledger_balance,
                    available_balance,
                    ledger_balance_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY account_id
                        ORDER BY ledger_balance_date DESC
                    ) AS rn
                FROM {OFX_BALANCES.full_name}
            )
            SELECT
                a.account_id,
                a.account_type,
                a.institution_name,
                a.source_system,
                b.ledger_balance,
                b.available_balance,
                b.ledger_balance_date
            FROM {DIM_ACCOUNTS.full_name} a
            LEFT JOIN latest_balances b
                ON a.account_id = b.account_id AND b.rn = 1
            GROUP BY a.account_id, a.account_type, a.institution_name,
                     a.source_system,
                     b.ledger_balance, b.available_balance, b.ledger_balance_date
            ORDER BY a.institution_name, a.account_type
        """)
    else:
        result = db.execute(f"""
            SELECT DISTINCT
                account_id,
                account_type,
                institution_name,
                source_system
            FROM {DIM_ACCOUNTS.full_name}
            ORDER BY institution_name, account_type
        """)

    columns = [desc[0] for desc in result.description]
    rows = result.fetchmany(MAX_ROWS)
    records = [dict(zip(columns, row, strict=False)) for row in rows]
    return json.dumps(records, indent=2, default=str)


# ---------------------------------------------------------------------------
# Transaction resources
# ---------------------------------------------------------------------------


@mcp.resource("moneybin://transactions/recent")
def recent_transactions() -> str:
    """Last 30 days of transactions across all sources."""
    logger.info("Resource read: transactions/recent")
    db = get_db()

    cutoff = (date.today() - timedelta(days=30)).isoformat()

    if not table_exists(FCT_TRANSACTIONS):
        return json.dumps({
            "message": "No recent transactions found in the last 30 days."
        })

    result = db.execute(
        f"""
        SELECT transaction_id, account_id, transaction_date, amount,
            description, merchant_name, memo, transaction_type,
            source_system
        FROM {FCT_TRANSACTIONS.full_name}
        WHERE transaction_date >= CAST(? AS DATE)
        ORDER BY transaction_date DESC
        LIMIT ?
        """,
        [cutoff, MAX_ROWS],
    )
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    records = [dict(zip(columns, row, strict=False)) for row in rows]

    if not records:
        return json.dumps({
            "message": "No recent transactions found in the last 30 days."
        })

    return truncate_result(json.dumps(records, indent=2, default=str))


# ---------------------------------------------------------------------------
# W2 resources
# ---------------------------------------------------------------------------


@mcp.resource("moneybin://w2/{tax_year}")
def w2_by_year(tax_year: str) -> str:
    """W-2 tax form data for a specific year.

    Args:
        tax_year: The tax year (e.g. '2024').
    """
    logger.info("Resource read: w2/%s", tax_year)
    db = get_db()

    if not table_exists(W2_FORMS):
        return json.dumps({"message": "No W-2 data loaded yet."})

    result = db.execute(
        f"""
        SELECT tax_year, employer_name, wages, federal_income_tax,
            social_security_wages, social_security_tax, medicare_wages,
            medicare_tax, state_local_info
        FROM {W2_FORMS.full_name}
        WHERE tax_year = ?
        ORDER BY employer_name
        """,
        [int(tax_year)],
    )

    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    records = [dict(zip(columns, row, strict=False)) for row in rows]

    if not records:
        return json.dumps({"message": f"No W-2 data found for tax year {tax_year}."})

    return json.dumps(records, indent=2, default=str)


# ---------------------------------------------------------------------------
# Not-yet-implemented resources
# ---------------------------------------------------------------------------


@mcp.resource("moneybin://investments/holdings")
def investments_holdings() -> str:
    """Current investment holdings (not yet implemented)."""
    return not_implemented(
        "Investment holdings data",
        "1. Connect investment accounts via Plaid sync\n"
        "2. Run 'moneybin sync plaid' with an investment-supporting institution",
    )


@mcp.resource("moneybin://spending/categories")
def spending_categories() -> str:
    """Spending breakdown by category (not yet implemented)."""
    return not_implemented(
        "Spending by category",
        "1. Run 'moneybin transform run' to build categorization models\n"
        "2. Ensure transaction data has been imported via OFX or Plaid",
    )
