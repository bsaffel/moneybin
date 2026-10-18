"""Shared database helpers for tests.

Core tables (dim_accounts, fct_transactions) are managed by SQLMesh in
production, so no DDL exists in ``src/moneybin/sql/schema/``.  Tests that
need to INSERT test data directly require concrete tables, so we define
minimal CREATE TABLE statements here.
"""

from collections.abc import Callable, Collection
from datetime import datetime

import duckdb
import pytest
import sqlglot
from sqlglot import exp

from moneybin.database import SQLMESH_ROOT, Database
from moneybin.tables import TableRef

_MODEL_FRESHNESS_DDL = (
    "meta.model_freshness (model_name VARCHAR, last_changed_at TIMESTAMPTZ, "
    "last_applied_at TIMESTAMPTZ, last_executed_at TIMESTAMPTZ, model_kind VARCHAR)"
)


def record_sqlmesh_apply(db: Database, when: datetime) -> None:
    """Stamp every model as executed at ``when``, read as UTC.

    ``TransformService.freshness()`` decides ``pending`` by comparing raw
    landing times against the *oldest* model execution, so any fixture that
    means "the warehouse has been refreshed" must write it — seeding
    ``core.dim_accounts`` no longer says that. Stands in for the
    ``meta.model_freshness`` view SQLMesh materializes over its own
    ``_snapshots`` / ``_intervals`` state; the view's derivation is covered by
    the scenario and integration suites, so this keeps only the columns
    ``freshness()`` reads plus enough shape to stay honest about the contract.

    Fixture instants use explicit known-UTC offsets so both freshness and
    raw landing values describe the same instants on any machine.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS meta")
    db.execute(f"CREATE OR REPLACE TABLE {_MODEL_FRESHNESS_DDL}")
    db.execute(
        "INSERT INTO meta.model_freshness VALUES ('core.dim_accounts', ?, ?, ?, 'FULL')",
        [when, when, when],
    )


def record_model_execution(
    db: Database, model_name: str, when: datetime, *, model_kind: str = "FULL"
) -> None:
    """Stamp one model's last successful backfill, leaving its siblings alone.

    ``record_sqlmesh_apply`` answers "the whole warehouse was refreshed"; this
    answers "this one model was rebuilt at this instant", which is what a
    per-report staleness check reads. ``when`` is an aware fixture instant,
    as ``meta.model_freshness`` stores it.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS meta")
    db.execute(f"CREATE TABLE IF NOT EXISTS {_MODEL_FRESHNESS_DDL}")
    db.execute("DELETE FROM meta.model_freshness WHERE model_name = ?", [model_name])
    db.execute(
        "INSERT INTO meta.model_freshness VALUES (?, ?, ?, ?, ?)",
        [model_name, when, when, when, model_kind],
    )


# ---------------------------------------------------------------------------
# Core table DDL — keep in sync with the SQLMesh model output columns.
# ---------------------------------------------------------------------------

CORE_DIM_ACCOUNTS_DDL = """\
CREATE TABLE IF NOT EXISTS core.dim_accounts (
    account_id VARCHAR PRIMARY KEY,
    display_name VARCHAR,
    official_name VARCHAR,
    account_type VARCHAR,
    account_type_override VARCHAR,
    source_account_type VARCHAR,
    account_subtype VARCHAR,
    holder_category VARCHAR,
    institution_slug VARCHAR,
    institution_fid VARCHAR,
    routing_number VARCHAR,
    institution_name VARCHAR,
    last_four VARCHAR,
    source_type VARCHAR,
    source_file VARCHAR,
    investment_source_type VARCHAR,
    currency_code VARCHAR,
    credit_limit DECIMAL(18, 2),
    display_name_is_user_set BOOLEAN,
    archived BOOLEAN DEFAULT FALSE,
    include_in_net_worth BOOLEAN DEFAULT TRUE,
    archived_at DATE,
    extracted_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    investment_source_type_changed_at TIMESTAMPTZ,
    archived_changed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
    -- The common case; derive_is_categorizable() recomputes it from the model.
    is_categorizable BOOLEAN DEFAULT TRUE,
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
    source_extracted_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ,
    transaction_year INTEGER,
    transaction_month INTEGER,
    transaction_day INTEGER,
    transaction_day_of_week INTEGER,
    transaction_year_month VARCHAR,
    transaction_year_quarter VARCHAR,
    notes STRUCT(note_id VARCHAR, "text" VARCHAR, author VARCHAR, created_at TIMESTAMPTZ)[],
    note_count INTEGER,
    tags VARCHAR[],
    tag_count INTEGER,
    splits STRUCT(split_id VARCHAR, amount DECIMAL(18, 2), category VARCHAR, subcategory VARCHAR, note VARCHAR)[],
    split_count INTEGER,
    has_splits BOOLEAN,
    updated_at TIMESTAMPTZ
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
    t.currency_code,
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
    in_net_worth BOOLEAN,
    observation_source VARCHAR,
    reconciliation_delta DECIMAL(18, 2),
    currency_code VARCHAR
);
"""

REPORTS_NET_WORTH_DDL = """\
CREATE VIEW IF NOT EXISTS reports.net_worth AS
SELECT
    CAST(NULL AS VARCHAR) AS home_currency_code,
    CURRENT_DATE AS balance_date,
    0 AS account_count,
    0 AS carried_forward_count,
    0 AS currency_count,
    0 AS unpriced_currency_count,
    0 AS unanchored_account_count,
    0.00::DECIMAL(18, 2) AS total_assets,
    0.00::DECIMAL(18, 2) AS total_liabilities,
    0.00::DECIMAL(18, 2) AS net_worth
WHERE FALSE;
"""

# core:net_worth_currencies reads this rung, not reports.net_worth (the
# day-grain total) — stubbed here so any fixture built on
# create_core_tables() / create_core_tables_raw() can exercise
# core:net_worth_currencies without a full SQLMesh build.
REPORTS_NET_WORTH_CURRENCIES_DDL = """\
CREATE VIEW IF NOT EXISTS reports.net_worth_currencies AS
SELECT
    CAST(NULL AS VARCHAR) AS currency_code,
    CAST(NULL AS VARCHAR) AS home_currency_code,
    CAST(NULL AS VARCHAR) AS rate_source,
    CURRENT_DATE AS balance_date,
    CAST(NULL AS DATE) AS rate_published_date,
    0 AS account_count,
    0 AS carried_forward_count,
    0 AS unanchored_account_count,
    0.00::DECIMAL(18, 2) AS total_assets,
    0.00::DECIMAL(18, 2) AS total_liabilities,
    0.00::DECIMAL(18, 2) AS net_worth,
    0.00::DECIMAL(18, 2) AS total_assets_home,
    0.00::DECIMAL(18, 2) AS total_liabilities_home,
    0.00::DECIMAL(18, 2) AS net_worth_home
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
       CAST(NULL AS TIMESTAMPTZ) AS created_at,
       CAST(NULL AS TIMESTAMPTZ) AS updated_at
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
       CAST(NULL AS TIMESTAMPTZ) AS created_at,
       CAST(NULL AS TIMESTAMPTZ) AS updated_at
WHERE FALSE;
"""


# core.bridge_category_source_map — SQLMesh-managed view in production.
# Tests stub its shape so schema-catalog and classification checks resolve
# the name. Column shape mirrors the SQLMesh model / seeds.refresh_views.
CORE_BRIDGE_MERCHANT_ENTITIES_STUB_DDL = """\
CREATE OR REPLACE VIEW core.bridge_merchant_entities AS
SELECT CAST(NULL AS VARCHAR) AS transaction_id,
       CAST(NULL AS VARCHAR) AS merchant_entity_id,
       CAST(NULL AS VARCHAR) AS merchant_entity_source_type,
       CAST(NULL AS VARCHAR) AS source_merchant_name
WHERE FALSE;
"""

CORE_BRIDGE_CATEGORY_SOURCE_MAP_STUB_DDL = """\
CREATE OR REPLACE VIEW core.bridge_category_source_map AS
SELECT CAST(NULL AS VARCHAR) AS source_type,
       CAST(NULL AS VARCHAR) AS source_origin,
       CAST(NULL AS VARCHAR) AS source_category_code,
       CAST(NULL AS VARCHAR) AS source_subcategory_code,
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
    updated_at TIMESTAMPTZ
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
    updated_at TIMESTAMPTZ
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
    updated_at TIMESTAMPTZ
);
"""

# M1K.3 Currency-accounting Core tables — SQLMesh Python FULL-kind tables in
# production. Column shapes mirror the fixed public schemas in multi-currency.md.
CORE_BRIDGE_CURRENCY_CONVERSIONS_DDL = """\
CREATE TABLE IF NOT EXISTS core.bridge_currency_conversions (
    conversion_id VARCHAR,
    transfer_pair_id VARCHAR,
    from_transaction_id VARCHAR,
    to_transaction_id VARCHAR,
    from_account_id VARCHAR,
    to_account_id VARCHAR,
    from_source_transaction_id VARCHAR,
    to_source_transaction_id VARCHAR,
    source_shape VARCHAR,
    from_currency VARCHAR,
    to_currency VARCHAR,
    home_currency VARCHAR,
    valuation_source_type VARCHAR,
    from_source_type VARCHAR,
    from_source_origin VARCHAR,
    to_source_type VARCHAR,
    to_source_origin VARCHAR,
    coverage_status VARCHAR,
    coverage_reason VARCHAR,
    from_amount DECIMAL(18, 2),
    to_amount DECIMAL(18, 2),
    executed_rate DECIMAL(18, 8),
    home_value DECIMAL(18, 2),
    valuation_rate DECIMAL(18, 8),
    from_date DATE,
    to_date DATE,
    valuation_rate_date DATE,
    updated_at TIMESTAMPTZ
);
"""

CORE_FCT_CURRENCY_LOTS_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_currency_lots (
    currency_lot_id VARCHAR,
    account_id VARCHAR,
    source_conversion_id VARCHAR,
    source_investment_transaction_id VARCHAR,
    source_transfer_id VARCHAR,
    currency_code VARCHAR,
    acquisition_type VARCHAR,
    cost_basis_method VARCHAR,
    home_currency VARCHAR,
    coverage_status VARCHAR,
    coverage_reason VARCHAR,
    original_quantity DECIMAL(18, 2),
    remaining_quantity DECIMAL(18, 2),
    cost_basis_total DECIMAL(18, 2),
    cost_basis_remaining DECIMAL(18, 2),
    basis_incomplete BOOLEAN,
    acquisition_date DATE,
    updated_at TIMESTAMPTZ
);
"""

CORE_FCT_REALIZED_FX_GAINS_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_realized_fx_gains (
    realized_fx_gain_id VARCHAR,
    account_id VARCHAR,
    conversion_id VARCHAR,
    currency_lot_id VARCHAR,
    currency_code VARCHAR,
    home_currency VARCHAR,
    cost_basis_method VARCHAR,
    valuation_source_type VARCHAR,
    coverage_status VARCHAR,
    coverage_reason VARCHAR,
    disposed_amount DECIMAL(18, 2),
    proceeds DECIMAL(18, 2),
    cost_basis DECIMAL(18, 2),
    gain_loss DECIMAL(18, 2),
    fee_amount DECIMAL(18, 2),
    valuation_rate DECIMAL(18, 8),
    acquisition_date DATE,
    disposal_date DATE,
    valuation_rate_date DATE,
    updated_at TIMESTAMPTZ
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
       CAST(NULL AS DECIMAL(18, 2)) AS market_value,
       CAST(NULL AS DECIMAL(18, 2)) AS unrealized_gain,
       CAST(NULL AS DATE) AS price_date,
       CAST(NULL AS VARCHAR) AS price_source,
       CAST(NULL AS INTEGER) AS days_since_observed,
       CAST(NULL AS VARCHAR) AS valuation_status,
       CAST(NULL AS DECIMAL(28, 10)) AS provider_reported_quantity,
       CAST(NULL AS DECIMAL(18, 2)) AS provider_reported_cost_basis,
       CAST(NULL AS DECIMAL(18, 2)) AS provider_reported_value,
       CAST(NULL AS TIMESTAMPTZ) AS provider_reported_as_of,
       CAST(NULL AS TIMESTAMPTZ) AS updated_at
WHERE FALSE;
"""
# NB: quantity/cost_basis/average_cost types above mirror dim_holdings.sql's
# explicit casts (DECIMAL(28,10)/(18,2)/(28,10)) — average_cost is DECIMAL,
# NOT DOUBLE (DuckDB's decimal `/` promotes to DOUBLE unless the whole
# division is cast back). database.md: no FLOAT for financial quantities.
# market_value/unrealized_gain/price_date/price_source/days_since_observed/
# valuation_status (Pillar C) mirror dim_holdings.sql's latest_price join —
# NULL (never zero) when no usable price applies.
# The provider_reported_* columns are the broker's non-authoritative claim
# (LEFT JOINed from the newest holdings snapshot in production) — same types as
# the ledger-derived columns they mirror.

# core.dim_holdings_broker_reported — SQLMesh-managed view in production
# (the broker's claim, reduced to one row per account). Column shape mirrors
# dim_holdings_broker_reported.sql's final SELECT.
CORE_DIM_HOLDINGS_BROKER_REPORTED_STUB_DDL = """\
CREATE OR REPLACE VIEW core.dim_holdings_broker_reported AS
SELECT CAST(NULL AS VARCHAR) AS account_id,
       CAST(NULL AS BOOLEAN) AS has_position,
       CAST(NULL AS DATE) AS as_of
WHERE FALSE;
"""

# core.fct_security_prices — SQLMesh SQL FULL-kind table in production.
# Column shape mirrors fct_security_prices.sql's final SELECT.
CORE_FCT_SECURITY_PRICES_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_security_prices (
    security_id VARCHAR,
    price_date DATE,
    quote_currency VARCHAR,
    close DECIMAL(28, 10),
    source_type VARCHAR,
    price_basis VARCHAR,
    updated_at TIMESTAMPTZ
);
"""


# The rate spine — three SQLMesh models (one VIEW, one FULL table, one VIEW) in
# production. Column shapes mirror each model's final SELECT, in its projected
# order. Stubbed here so the classification-completeness guard actually sees
# them: it compares CLASSIFICATION against this synthetic catalog rather than a
# SQLMesh-built one, so a table absent here is a table the guard cannot miss.
CORE_FCT_EXCHANGE_RATES_STUB_DDL = """\
CREATE OR REPLACE VIEW core.fct_exchange_rates AS
SELECT CAST(NULL AS VARCHAR) AS from_currency,
       CAST(NULL AS VARCHAR) AS to_currency,
       CAST(NULL AS VARCHAR) AS rate_source,
       CAST(NULL AS VARCHAR) AS rate_vendor,
       CAST(NULL AS DECIMAL(18, 8)) AS rate,
       CAST(NULL AS DATE) AS rate_date,
       CAST(NULL AS TIMESTAMPTZ) AS updated_at
WHERE FALSE;
"""

CORE_FCT_EXCHANGE_RATES_DAILY_DDL = """\
CREATE TABLE IF NOT EXISTS core.fct_exchange_rates_daily (
    from_currency VARCHAR,
    to_currency VARCHAR,
    rate_source VARCHAR,
    rate_vendor VARCHAR,
    rate DECIMAL(18, 8),
    days_since_published INTEGER,
    effective_date DATE,
    published_date DATE
);
"""

CORE_FCT_EXCHANGE_RATES_EFFECTIVE_STUB_DDL = """\
CREATE OR REPLACE VIEW core.fct_exchange_rates_effective AS
SELECT CAST(NULL AS VARCHAR) AS from_currency,
       CAST(NULL AS VARCHAR) AS to_currency,
       CAST(NULL AS VARCHAR) AS rate_source,
       CAST(NULL AS VARCHAR) AS rate_vendor,
       CAST(NULL AS DECIMAL(18, 8)) AS rate,
       CAST(NULL AS INTEGER) AS days_since_published,
       CAST(NULL AS DATE) AS effective_date,
       CAST(NULL AS DATE) AS published_date
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
    db.execute(CORE_BRIDGE_MERCHANT_ENTITIES_STUB_DDL)
    db.execute(CORE_DIM_SECURITIES_STUB_DDL)
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    db.execute(CORE_FCT_INVESTMENT_LOTS_DDL)
    db.execute(CORE_FCT_REALIZED_GAINS_DDL)
    db.execute(CORE_BRIDGE_CURRENCY_CONVERSIONS_DDL)
    db.execute(CORE_FCT_CURRENCY_LOTS_DDL)
    db.execute(CORE_FCT_REALIZED_FX_GAINS_DDL)
    db.execute(CORE_DIM_HOLDINGS_STUB_DDL)
    db.execute(CORE_DIM_HOLDINGS_BROKER_REPORTED_STUB_DDL)
    db.execute(CORE_FCT_SECURITY_PRICES_DDL)
    db.execute(CORE_FCT_EXCHANGE_RATES_DAILY_DDL)
    db.execute(CORE_FCT_EXCHANGE_RATES_STUB_DDL)
    db.execute(CORE_FCT_EXCHANGE_RATES_EFFECTIVE_STUB_DDL)


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
    db.execute(REPORTS_NET_WORTH_CURRENCIES_DDL)


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
    conn.execute(REPORTS_NET_WORTH_CURRENCIES_DDL)


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
        database.execute(  # static module constants, not user input
            f"COMMENT ON TABLE {table} IS '{escaped}'"
        )
    for table, cols in CORE_COLUMN_COMMENTS.items():
        for col, comment in cols.items():
            escaped = comment.replace("'", "''")
            database.execute(  # static module constants, not user input
                f"COMMENT ON COLUMN {table}.{col} IS '{escaped}'"
            )


# Resolve via the package's own SQLMESH_ROOT rather than walking up to the repo
# root: the SQLMesh project lives inside the installed package, so a path built
# from a test file's parents breaks whenever the project moves.
_FCT_TRANSACTIONS_MODEL_FILE = SQLMESH_ROOT / "models" / "core" / "fct_transactions.sql"


def _is_categorizable_expression() -> str:
    """The model's is_categorizable, inlined over aliases f (fact) and a (dim_accounts)."""
    raw = _FCT_TRANSACTIONS_MODEL_FILE.read_text()
    end = raw.index(");", raw.index("MODEL (")) + 2
    select = sqlglot.parse_one(raw[end:], read="duckdb")
    enriched = next(cte for cte in select.find_all(exp.CTE) if cte.alias == "enriched")
    account_half = next(
        e.this
        for e in enriched.this.expressions
        if e.alias == "account_resolved_unarchived"
    )
    column = next(e.this for e in select.expressions if e.alias == "is_categorizable")

    def inline(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Column) and node.name == "account_resolved_unarchived":
            return exp.paren(account_half.copy())
        if isinstance(node, exp.Column) and not node.table:
            return exp.column(node.name, table="f")
        return node

    return column.transform(inline).sql("duckdb")


def derive_is_categorizable(db: Database) -> None:
    """Recompute the fixture's is_categorizable with the shipped model's expression.

    Production builds core.fct_transactions as a SQLMesh view; the unit-test
    stand-in is a table whose column defaults TRUE. A test exercising an
    exclusion calls this after seeding, so it binds to the model's definition
    rather than a hand-typed copy of it.
    """
    db.execute(
        f"""
        UPDATE core.fct_transactions AS t
        SET is_categorizable = d.is_categorizable
        FROM (
            SELECT f.rowid AS rid, {_is_categorizable_expression()} AS is_categorizable
            FROM core.fct_transactions AS f
            LEFT JOIN core.dim_accounts AS a ON f.account_id = a.account_id
        ) AS d
        WHERE t.rowid = d.rid
        """  # noqa: S608  # model expression read from the repo file, not user input
    )


def simulate_pre_refresh_fct_transactions(db: Database) -> None:
    """Drop is_categorizable: the fact a database holds after upgrade, before refresh."""
    db.execute("ALTER TABLE core.fct_transactions DROP COLUMN is_categorizable")


def insert_three_pending_transactions(db: Database) -> None:
    """T1–T3, uncategorized, on the MCP template's ACC001 and ACC002.

    Impact order is T2, T1, T3 at any CURRENT_DATE: ABS(amount) * age is
    500*(d+5) > 25*(d+14) > 5*d, d being days since 2026-04-15.
    """
    db.execute(
        """
        INSERT INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, amount, currency_code,
             description, merchant_name, source_type, is_transfer)
        VALUES
            ('T1', 'ACC001', DATE '2026-04-01', -25.00, 'USD', 'COFFEE SHOP',
             'Coffee Shop', 'ofx', false),
            ('T2', 'ACC001', DATE '2026-04-10', -500.00, 'USD', 'BIG EXPENSE',
             'Big Expense', 'ofx', false),
            ('T3', 'ACC002', DATE '2026-04-15', -5.00, 'USD', 'TINY',
             'Tiny', 'ofx', false)
        """  # test input, not user data
    )


def seed_pending_dedup_pair(db: Database) -> None:
    """Two same-amount rows from different sources, proposed as duplicates, undecided.

    The shape issue #409 reported: dedup escalated the pair instead of merging
    it, so both rows stay in ``core.fct_transactions`` and every total covering
    them counts the payment twice. Needs the core tables already created.
    """
    # keep the repo off this helper module's import path
    from moneybin.repositories.match_decisions_repo import (
        MatchDecisionsRepo,
    )

    db.execute(
        """
        INSERT INTO core.fct_transactions (transaction_id, account_id, amount)
        VALUES ('dup_ofx', 'acct_11112222', -25.00),
               ('dup_csv', 'acct_11112222', -25.00)
        """
    )
    MatchDecisionsRepo(db).insert(
        match_id="match00000001",
        source_transaction_id_a="ofx-1",
        source_type_a="ofx",
        source_origin_a="test-bank",
        source_transaction_id_b="csv-1",
        source_type_b="tabular",
        source_origin_b="test-export",
        account_id="acct_11112222",
        confidence_score=0.58,
        match_signals={"date_distance": 0},
        match_status="pending",
        match_tier="3",
        decided_by="auto",
        actor="test",
    )


def install_prep_view(db: Database, name: str) -> None:
    """Install one shipped ``prep`` model as a view, its SELECT parsed off disk."""
    # Deferred: sqlmesh is heavy, and most importers of this module never call this.
    from sqlglot import exp
    from sqlmesh.core.dialect import parse

    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    path = SQLMESH_ROOT / "models" / "prep" / f"{name}.sql"
    query = next(
        node for node in parse(path.read_text()) if isinstance(node, exp.Select)
    )
    db.execute(
        f"CREATE OR REPLACE VIEW prep.{name} AS {query.sql(dialect='duckdb')}"  # shipped model name from a test, not user input
    )


def spy_restate(
    monkeypatch: pytest.MonkeyPatch, remaining: Callable[[], int]
) -> list[tuple[tuple[TableRef, ...], str, int]]:
    """Replace ``restate_after_write`` with a recorder.

    Each call records its inputs, its ``committed_change``, and ``remaining()``
    taken at that moment, so a test can show the restate ran after the
    writer's delete committed.
    """
    calls: list[tuple[tuple[TableRef, ...], str, int]] = []

    def spy(
        _db: Database, inputs: Collection[TableRef], *, committed_change: str
    ) -> None:
        calls.append((tuple(inputs), committed_change, remaining()))

    monkeypatch.setattr("moneybin.services.restate.restate_after_write", spy)
    return calls
