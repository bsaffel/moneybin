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
    source_type VARCHAR,
    source_file VARCHAR,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    display_name VARCHAR,
    official_name VARCHAR,
    last_four VARCHAR,
    account_subtype VARCHAR,
    holder_category VARCHAR,
    iso_currency_code VARCHAR DEFAULT 'USD',
    credit_limit DECIMAL(18, 2),
    archived BOOLEAN DEFAULT FALSE,
    include_in_net_worth BOOLEAN DEFAULT TRUE
);
"""

CORE_BRIDGE_TRANSFERS_DDL = """\
CREATE TABLE IF NOT EXISTS core.bridge_transfers (
    transfer_id VARCHAR PRIMARY KEY,
    debit_transaction_id VARCHAR,
    credit_transaction_id VARCHAR,
    date_offset_days INTEGER,
    amount DECIMAL(18, 2)
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
    source_type VARCHAR,
    source_count INTEGER,
    match_confidence DECIMAL(5, 4),
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
    db.execute(CORE_BRIDGE_TRANSFERS_DDL)


def create_core_tables_raw(conn: duckdb.DuckDBPyConnection) -> None:
    """Create core tables for testing (raw connection version).

    Legacy version that accepts a raw DuckDB connection. Use create_core_tables()
    with a Database instance for new code.

    Args:
        conn: An active read-write DuckDB connection.
    """
    conn.execute(CORE_DIM_ACCOUNTS_DDL)
    conn.execute(CORE_FCT_TRANSACTIONS_DDL)
    conn.execute(CORE_BRIDGE_TRANSFERS_DDL)


# Table and column comments for core tables — mirror the SQLMesh model
# headers and inline column comments. Applied separately because the
# minimal CREATE TABLE DDL above does not embed them.
CORE_TABLE_COMMENTS: dict[str, str] = {
    "core.fct_transactions": (
        "Canonical transactions fact view; reads from the deduplicated "
        "merged layer with categorization and merchant joins; "
        "negative amount = expense, positive = income"
    ),
    "core.dim_accounts": (
        "Canonical accounts dimension; one row per account across sources"
    ),
    "core.bridge_transfers": (
        "Confirmed transfer pairs linking two fct_transactions rows; "
        "derived from app.match_decisions where match_type = 'transfer'"
    ),
}

CORE_COLUMN_COMMENTS: dict[str, dict[str, str]] = {
    "core.fct_transactions": {
        "transaction_id": (
            "Gold key: deterministic SHA-256 hash, unique per real-world transaction"
        ),
        "amount": "Transaction amount; negative = expense, positive = income",
        "transaction_direction": ("Derived from amount sign: expense, income, or zero"),
        "category": (
            "Spending category; from app.transaction_categories when "
            "categorized, else source value"
        ),
    },
    "core.dim_accounts": {
        "account_id": "Stable per-source account identifier",
        "institution_name": "Display name of the issuing institution",
    },
}


def apply_core_table_comments(database: Database) -> None:
    """Apply COMMENT ON TABLE/COLUMN for core test tables.

    Production comments are applied by SQLMesh's `register_comments`;
    tests need to mirror that for the schema catalog tests to see prose.
    """
    for table, comment in CORE_TABLE_COMMENTS.items():
        escaped = comment.replace("'", "''")
        database.execute(  # noqa: S608  # static module constants, not user input
            f"COMMENT ON TABLE {table} IS '{escaped}'"
        )
    for table, cols in CORE_COLUMN_COMMENTS.items():
        for col, comment in cols.items():
            escaped = comment.replace("'", "''")
            database.execute(  # noqa: S608  # static module constants, not user input
                f"COMMENT ON COLUMN {table}.{col} IS '{escaped}'"
            )
