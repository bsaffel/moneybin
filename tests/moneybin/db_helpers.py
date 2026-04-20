"""Shared database helpers for tests.

Core tables (dim_accounts, fct_transactions) are managed by SQLMesh in
production, so no DDL exists in ``src/moneybin/sql/schema/``.  Tests that
need to INSERT test data directly require concrete tables, so we define
minimal CREATE TABLE statements here.
"""

import duckdb

from moneybin.database import Database

# ---------------------------------------------------------------------------
# Core table DDL — keep in sync with the SQLMesh model output columns.
# ---------------------------------------------------------------------------

CORE_DIM_ACCOUNTS_DDL = """\
CREATE TABLE IF NOT EXISTS core.dim_accounts (
    account_id VARCHAR PRIMARY KEY,
    routing_number VARCHAR,
    account_type VARCHAR,
    institution_name VARCHAR,
    institution_fid VARCHAR,
    source_system VARCHAR,
    source_file VARCHAR,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CORE_FCT_TRANSACTIONS_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_transactions (
    transaction_id VARCHAR,
    account_id VARCHAR,
    transaction_date DATE,
    authorized_date DATE,
    amount DECIMAL(18, 2),
    amount_absolute DECIMAL(18, 2),
    transaction_direction VARCHAR,
    description VARCHAR,
    merchant_name VARCHAR,
    memo VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    categorized_by VARCHAR,
    payment_channel VARCHAR,
    transaction_type VARCHAR,
    check_number VARCHAR,
    is_pending BOOLEAN,
    pending_transaction_id VARCHAR,
    location_address VARCHAR,
    location_city VARCHAR,
    location_region VARCHAR,
    location_postal_code VARCHAR,
    location_country VARCHAR,
    location_latitude DOUBLE,
    location_longitude DOUBLE,
    currency_code VARCHAR,
    source_system VARCHAR,
    source_extracted_at TIMESTAMP,
    loaded_at TIMESTAMP,
    transaction_year INTEGER,
    transaction_month INTEGER,
    transaction_day INTEGER,
    transaction_day_of_week INTEGER,
    transaction_year_month VARCHAR,
    transaction_year_quarter VARCHAR
);
"""


def create_core_tables(db: Database) -> None:
    """Create core tables for testing.

    Core tables are managed by SQLMesh in production.  Tests that INSERT
    fixture data need concrete tables, so this helper creates them.

    Args:
        db: A Database instance for executing DDL.
    """
    db.execute(CORE_DIM_ACCOUNTS_DDL)
    db.execute(CORE_FCT_TRANSACTIONS_DDL)


def create_core_tables_raw(conn: duckdb.DuckDBPyConnection) -> None:
    """Create core tables for testing (raw connection version).

    Legacy version that accepts a raw DuckDB connection. Use create_core_tables()
    with a Database instance for new code.

    Args:
        conn: An active read-write DuckDB connection.
    """
    conn.execute(CORE_DIM_ACCOUNTS_DDL)
    conn.execute(CORE_FCT_TRANSACTIONS_DDL)
