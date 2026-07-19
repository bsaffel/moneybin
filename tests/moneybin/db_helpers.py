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
    currency_code VARCHAR DEFAULT 'USD',
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
    original_description VARCHAR,
    merchant_name VARCHAR,
    merchant_id VARCHAR,
    memo VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    categorized_by VARCHAR,
    payment_channel VARCHAR,
    transaction_type VARCHAR,
    check_number VARCHAR,
    is_pending BOOLEAN,
    pending_transaction_id VARCHAR,
    is_transfer BOOLEAN,
    transfer_pair_id VARCHAR,
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
    transaction_year_quarter VARCHAR,
    notes STRUCT(note_id VARCHAR, "text" VARCHAR, author VARCHAR, created_at TIMESTAMP)[],
    note_count INTEGER,
    tags VARCHAR[],
    tag_count INTEGER,
    splits STRUCT(split_id VARCHAR, amount DECIMAL(18, 2), category VARCHAR, subcategory VARCHAR, note VARCHAR)[],
    split_count INTEGER,
    has_splits BOOLEAN,
    updated_at TIMESTAMP
);
"""

# core.fct_transaction_lines view — split-expanded grain. Mirrors the SQLMesh
# model definition. Created in tests after fct_transactions is created.
CORE_FCT_TRANSACTION_LINES_DDL = """\
CREATE OR REPLACE VIEW core.fct_transaction_lines AS
SELECT
    t.transaction_id,
    COALESCE(s.split_id, 'whole') AS line_id,
    COALESCE(s.amount, t.amount) AS line_amount,
    COALESCE(s.category, t.category) AS line_category,
    COALESCE(s.subcategory, t.subcategory) AS line_subcategory,
    s.note AS line_note,
    CASE WHEN s.split_id IS NULL THEN 'whole' ELSE 'split' END AS line_kind,
    t.account_id,
    t.transaction_date,
    t.merchant_name,
    t.description,
    t.is_pending,
    t.transfer_pair_id,
    t.is_transfer,
    t.source_type,
    t.source_count,
    t.transaction_year,
    t.transaction_month,
    t.transaction_year_month,
    t.transaction_year_quarter
FROM core.fct_transactions AS t
LEFT JOIN UNNEST(t.splits) AS u (s) ON TRUE
WHERE NOT COALESCE(t.has_splits, FALSE) OR s.split_id IS NOT NULL;
"""


CORE_FCT_BALANCES_DDL = """\
CREATE VIEW IF NOT EXISTS core.fct_balances AS
SELECT
    'placeholder'::VARCHAR AS account_id,
    CURRENT_DATE AS balance_date,
    0.00::DECIMAL(18, 2) AS balance,
    'ofx'::VARCHAR AS source_type,
    'placeholder'::VARCHAR AS source_ref,
    CURRENT_TIMESTAMP AS updated_at,
    'USD'::VARCHAR AS currency_code
WHERE FALSE;
"""

CORE_FCT_BALANCES_DAILY_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_balances_daily (
    account_id VARCHAR,
    balance_date DATE,
    balance DECIMAL(18, 2),
    is_observed BOOLEAN,
    observation_source VARCHAR,
    reconciliation_delta DECIMAL(18, 2),
    currency_code VARCHAR
);
"""

REPORTS_NET_WORTH_DDL = """\
CREATE VIEW IF NOT EXISTS reports.net_worth AS
SELECT
    CURRENT_DATE AS balance_date,
    0.00::DECIMAL(18, 2) AS net_worth,
    0 AS account_count,
    0.00::DECIMAL(18, 2) AS total_assets,
    0.00::DECIMAL(18, 2) AS total_liabilities
WHERE FALSE;
"""

# core.dim_categories / core.dim_merchants are SQLMesh-managed views in
# production. Tests stub their shape so schema-catalog and classification
# checks resolve the names. Column shapes mirror the SQLMesh models.
CORE_DIM_CATEGORIES_STUB_DDL = """\
CREATE OR REPLACE VIEW core.dim_categories AS
SELECT CAST(NULL AS VARCHAR) AS category_id,
       CAST(NULL AS VARCHAR) AS category,
       CAST(NULL AS VARCHAR) AS subcategory,
       CAST(NULL AS VARCHAR) AS description,
       CAST(NULL AS VARCHAR) AS class,
       CAST(NULL AS BOOLEAN) AS is_default,
       CAST(NULL AS BOOLEAN) AS is_active,
       CAST(NULL AS TIMESTAMP) AS created_at,
       CAST(NULL AS TIMESTAMP) AS updated_at
WHERE FALSE;
"""

CORE_DIM_MERCHANTS_STUB_DDL = """\
CREATE OR REPLACE VIEW core.dim_merchants AS
SELECT CAST(NULL AS VARCHAR) AS merchant_id,
       CAST(NULL AS VARCHAR) AS raw_pattern,
       CAST(NULL AS VARCHAR) AS match_type,
       CAST(NULL AS VARCHAR) AS canonical_name,
       CAST(NULL AS VARCHAR) AS category_id,
       CAST(NULL AS VARCHAR) AS category,
       CAST(NULL AS VARCHAR) AS subcategory,
       CAST(NULL AS VARCHAR) AS created_by,
       CAST(NULL AS VARCHAR[]) AS exemplars,
       CAST(NULL AS TIMESTAMP) AS created_at,
       CAST(NULL AS TIMESTAMP) AS updated_at
WHERE FALSE;
"""


# core.bridge_category_source_map — SQLMesh-managed view in production.
# Tests stub its shape so schema-catalog and classification checks resolve
# the name. Column shape mirrors the SQLMesh model / seeds.refresh_views.
CORE_BRIDGE_CATEGORY_SOURCE_MAP_STUB_DDL = """\
CREATE OR REPLACE VIEW core.bridge_category_source_map AS
SELECT CAST(NULL AS VARCHAR) AS source_type,
       CAST(NULL AS VARCHAR) AS source_category_code,
       CAST(NULL AS VARCHAR) AS code_level,
       CAST(NULL AS VARCHAR) AS category_id,
       CAST(NULL AS VARCHAR) AS source_taxonomy_version,
       CAST(NULL AS BOOLEAN) AS is_default
WHERE FALSE;
"""

# core.dim_securities — SQLMesh-managed view in production. Unlike the
# dim_categories/dim_merchants stubs (which are empty `WHERE FALSE` shape-only
# views because production builds them with joins/seeds), dim_securities.sql is
# a *pure passthrough* of app.securities — so the faithful stub is that same
# passthrough. This lets reads (`InvestmentService.list_securities`, the CLI's
# `securities list`) reflect real `securities add`/`set` writes without a
# per-test inline view override. app.securities always exists here: every
# caller of create_core_dim_stub_views opens a real Database (init_schemas).
CORE_DIM_SECURITIES_STUB_DDL = """\
CREATE OR REPLACE VIEW core.dim_securities AS
SELECT security_id, name, security_type, ticker, exchange, cusip, isin, figi,
       coingecko_id, is_cash_equivalent, currency_code
FROM app.securities;
"""

# core.fct_investment_transactions — SQLMesh FULL-kind table in production.
# Column shape mirrors fct_investment_transactions.sql's final SELECT.
CORE_FCT_INVESTMENT_TRANSACTIONS_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_investment_transactions (
    investment_transaction_id VARCHAR,
    account_id VARCHAR,
    security_id VARCHAR,
    trade_date DATE,
    settlement_date DATE,
    original_acquisition_date DATE,
    type VARCHAR,
    subtype VARCHAR,
    event_group_id VARCHAR,
    quantity DECIMAL(28, 10),
    price DECIMAL(28, 10),
    amount DECIMAL(18, 2),
    fees DECIMAL(18, 2),
    currency_code VARCHAR,
    provider_type VARCHAR,
    provider_subtype VARCHAR,
    source_type VARCHAR,
    source_origin VARCHAR,
    description VARCHAR,
    updated_at TIMESTAMP
);
"""

# core.fct_investment_lots — SQLMesh Python FULL-kind table in production.
# Column shape mirrors fct_investment_lots.py's `columns={...}` block.
CORE_FCT_INVESTMENT_LOTS_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_investment_lots (
    lot_id VARCHAR,
    account_id VARCHAR,
    security_id VARCHAR,
    acquisition_date DATE,
    acquisition_type VARCHAR,
    original_quantity DECIMAL(28, 10),
    remaining_quantity DECIMAL(28, 10),
    cost_basis_total DECIMAL(18, 2),
    cost_basis_remaining DECIMAL(18, 2),
    cost_basis_method VARCHAR,
    currency_code VARCHAR,
    is_open BOOLEAN,
    source_transaction_id VARCHAR,
    basis_incomplete BOOLEAN,
    updated_at TIMESTAMP
);
"""

# core.fct_realized_gains — SQLMesh Python FULL-kind table in production.
# Column shape mirrors fct_realized_gains.py's `columns={...}` block.
CORE_FCT_REALIZED_GAINS_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_realized_gains (
    realized_gain_id VARCHAR,
    account_id VARCHAR,
    security_id VARCHAR,
    disposal_txn_id VARCHAR,
    lot_id VARCHAR,
    quantity DECIMAL(28, 10),
    acquisition_date DATE,
    disposal_date DATE,
    proceeds DECIMAL(18, 2),
    cost_basis DECIMAL(18, 2),
    gain_loss DECIMAL(18, 2),
    term VARCHAR,
    cost_basis_method VARCHAR,
    basis_incomplete BOOLEAN,
    currency_code VARCHAR,
    updated_at TIMESTAMP
);
"""

# core.dim_holdings — SQLMesh-managed view in production (aggregates open
# lots per account/security). Column shape mirrors dim_holdings.sql's
# final SELECT; stubbed standalone (not derived from fct_investment_lots)
# to match the dim_categories/dim_merchants stub convention.
CORE_DIM_HOLDINGS_STUB_DDL = """\
CREATE OR REPLACE VIEW core.dim_holdings AS
SELECT CAST(NULL AS VARCHAR) AS account_id,
       CAST(NULL AS VARCHAR) AS security_id,
       CAST(NULL AS DECIMAL(28, 10)) AS quantity,
       CAST(NULL AS DECIMAL(18, 2)) AS cost_basis,
       CAST(NULL AS DECIMAL(28, 10)) AS average_cost,
       CAST(NULL AS VARCHAR) AS currency_code,
       CAST(NULL AS DECIMAL(28, 10)) AS provider_reported_quantity,
       CAST(NULL AS DECIMAL(18, 2)) AS provider_reported_cost_basis,
       CAST(NULL AS DECIMAL(18, 2)) AS provider_reported_value,
       CAST(NULL AS TIMESTAMP) AS provider_reported_as_of,
       CAST(NULL AS TIMESTAMP) AS updated_at
WHERE FALSE;
"""
# NB: quantity/cost_basis/average_cost types above mirror dim_holdings.sql's
# explicit casts (DECIMAL(28,10)/(18,2)/(28,10)) — average_cost is DECIMAL,
# NOT DOUBLE (DuckDB's decimal `/` promotes to DOUBLE unless the whole
# division is cast back). database.md: no FLOAT for financial quantities.
# The provider_reported_* columns are the broker's non-authoritative claim
# (LEFT JOINed from the newest holdings snapshot in production) — same types as
# the ledger-derived columns they mirror.

# core.fct_security_prices — SQLMesh SQL FULL-kind table in production.
# Column shape mirrors fct_security_prices.sql's final SELECT.
CORE_FCT_SECURITY_PRICES_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_security_prices (
    security_id VARCHAR,
    price_date DATE,
    quote_currency VARCHAR,
    close DECIMAL(28, 10),
    source VARCHAR,
    price_basis VARCHAR,
    updated_at TIMESTAMP
);
"""

# core.uncategorized_queue — SQLMesh-managed view in production (curator-impact
# queue, moved from reports.* per reports-foundation.md R5). Column shape
# mirrors uncategorized_queue.sql's final SELECT.
CORE_UNCATEGORIZED_QUEUE_STUB_DDL = """\
CREATE OR REPLACE VIEW core.uncategorized_queue AS
SELECT CAST(NULL AS VARCHAR) AS transaction_id,
       CAST(NULL AS VARCHAR) AS account_id,
       CAST(NULL AS VARCHAR) AS account_name,
       CAST(NULL AS DATE) AS txn_date,
       CAST(NULL AS DECIMAL(18, 2)) AS amount,
       CAST(NULL AS VARCHAR) AS description,
       CAST(NULL AS VARCHAR) AS merchant_id,
       CAST(NULL AS VARCHAR) AS merchant_normalized,
       CAST(NULL AS INTEGER) AS age_days,
       CAST(NULL AS DECIMAL(18, 2)) AS priority_score,
       CAST(NULL AS VARCHAR) AS source_type,
       CAST(NULL AS VARCHAR) AS source_id
WHERE FALSE;
"""


def create_core_dim_stub_views(db: Database) -> None:
    """Materialize core.* SQLMesh-managed view/table stubs for testing.

    Production builds these via SQLMesh; tests stub them so anything
    inspecting the catalog (schema-catalog tests, classification
    completeness tests) sees the expected column shape.
    """
    db.execute(CORE_DIM_CATEGORIES_STUB_DDL)
    db.execute(CORE_DIM_MERCHANTS_STUB_DDL)
    db.execute(CORE_BRIDGE_CATEGORY_SOURCE_MAP_STUB_DDL)
    db.execute(CORE_DIM_SECURITIES_STUB_DDL)
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    db.execute(CORE_FCT_INVESTMENT_LOTS_DDL)
    db.execute(CORE_FCT_REALIZED_GAINS_DDL)
    db.execute(CORE_DIM_HOLDINGS_STUB_DDL)
    db.execute(CORE_FCT_SECURITY_PRICES_DDL)
    db.execute(CORE_UNCATEGORIZED_QUEUE_STUB_DDL)


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
    db.execute(CORE_FCT_TRANSACTION_LINES_DDL)
    db.execute(CORE_FCT_BALANCES_DDL)
    db.execute(CORE_FCT_BALANCES_DAILY_DDL)
    db.execute(REPORTS_NET_WORTH_DDL)


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
    conn.execute(CORE_FCT_TRANSACTION_LINES_DDL)
    conn.execute(CORE_FCT_BALANCES_DDL)
    conn.execute(CORE_FCT_BALANCES_DAILY_DDL)
    conn.execute(REPORTS_NET_WORTH_DDL)


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


def seed_categories_view(db: Database) -> None:
    """Seed seeds.categories with a single default row + refresh dim views.

    Used by tests that exercise category-toggle behavior on default-category rows.
    The seeded row is
    ``('FND', 'Food & Drink', NULL, 'Food and beverages', 'expense')``.
    """
    from moneybin.seeds import refresh_views

    db.execute("CREATE SCHEMA IF NOT EXISTS seeds")
    db.execute("""
        CREATE TABLE IF NOT EXISTS seeds.categories (
            category_id VARCHAR,
            category VARCHAR,
            subcategory VARCHAR,
            description VARCHAR,
            class VARCHAR
        )
    """)
    db.execute("""
        INSERT INTO seeds.categories VALUES
        ('FND', 'Food & Drink', NULL, 'Food and beverages', 'expense')
    """)
    refresh_views(db)


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
