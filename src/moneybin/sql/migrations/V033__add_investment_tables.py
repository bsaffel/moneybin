"""V033: create the investment tables + account cost-basis default column.

Creates ``app.securities``, ``raw.manual_investment_transactions``, and
``app.lot_selections``, and adds ``app.account_settings.default_cost_basis_method``
(per ``docs/specs/investments-data-model.md``). The same DDL ships in
``src/moneybin/sql/schema/*.sql`` which ``init_schemas`` runs on every
Database open: fresh installs get everything at open time; pre-existing
databases get it via this migration. ``CREATE TABLE IF NOT EXISTS`` and
``ADD COLUMN IF NOT EXISTS`` keep both paths idempotent.

Pure additive DDL — no backfill, no reshape.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_SECURITIES_SQL = """
CREATE TABLE IF NOT EXISTS app.securities (
    security_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    security_type VARCHAR NOT NULL
        CHECK (security_type IN ('equity', 'etf', 'mutual_fund', 'bond', 'crypto', 'cash', 'other')),
    ticker VARCHAR,
    exchange VARCHAR,
    cusip VARCHAR,
    isin VARCHAR,
    figi VARCHAR,
    coingecko_id VARCHAR,
    is_cash_equivalent BOOLEAN,
    cost_basis_method VARCHAR
        CHECK (cost_basis_method IN ('fifo', 'hifo', 'specific', 'average')),
    currency_code VARCHAR NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_CREATE_MANUAL_INVESTMENT_TRANSACTIONS_SQL = """
CREATE TABLE IF NOT EXISTS raw.manual_investment_transactions (
    source_transaction_id VARCHAR PRIMARY KEY,
    source_type VARCHAR NOT NULL DEFAULT 'manual',
    source_origin VARCHAR NOT NULL DEFAULT 'user',
    import_id VARCHAR NOT NULL,
    account_id VARCHAR NOT NULL,
    security_id VARCHAR,
    security_ref VARCHAR,
    type VARCHAR NOT NULL,
    subtype VARCHAR,
    event_group_id VARCHAR,
    trade_date DATE NOT NULL,
    settlement_date DATE,
    original_acquisition_date DATE,
    quantity DECIMAL(28, 10),
    price DECIMAL(28, 10),
    amount DECIMAL(18, 2),
    fees DECIMAL(18, 2),
    currency_code VARCHAR DEFAULT 'USD',
    description VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR NOT NULL,
    investment_transaction_id VARCHAR
)
"""

_CREATE_LOT_SELECTIONS_SQL = """
CREATE TABLE IF NOT EXISTS app.lot_selections (
    investment_transaction_id VARCHAR NOT NULL,
    lot_id VARCHAR NOT NULL,
    quantity DECIMAL(28, 10) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (investment_transaction_id, lot_id)
)
"""

# (column_name, comment_text) — applied as COMMENT ON COLUMN after CREATE.
# COMMENT ON COLUMN replaces existing comments, so this is safe to re-run.
_SECURITIES_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "security_id",
        "Stable surrogate (truncated UUID4, 12 hex); never derived from ticker",
    ),
    ("name", "Human-readable label"),
    (
        "security_type",
        "Instrument classification; 'cash' = money-market/sweep positions",
    ),
    ("ticker", "Exchange ticker; nullable, not unique (tickers get reused)"),
    ("exchange", "Listing exchange; disambiguates duplicate tickers"),
    (
        "cusip",
        "9-char CUSIP if supplied by user data; licensed — accepted, never redistributed",
    ),
    ("isin", "ISIN if supplied; international identifier"),
    ("figi", "OpenFIGI identifier (open mapping aid)"),
    ("coingecko_id", "CoinGecko slug for crypto price lookup (Pillar C)"),
    ("is_cash_equivalent", "Highly liquid, treat-like-cash flag; NULL = unknown"),
    (
        "cost_basis_method",
        "Per-security election override; NULL falls back to account default",
    ),
    ("currency_code", "Instrument's denominating currency; no FX conversion in v1"),
    ("created_at", "When the catalog entry was created"),
    ("updated_at", "When last modified; service sets explicitly on UPDATE"),
]

_MANUAL_INVESTMENT_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "source_transaction_id",
        "Truncated UUID4 (12 hex), prefixed 'manual_' for source-clarity in joins",
    ),
    ("source_type", "Discriminator; constant 'manual' for this table"),
    ("source_origin", "Origin tag; always 'user' for manual entries"),
    (
        "import_id",
        "FK to raw.import_log.import_id; one batch per CLI call or MCP bulk call",
    ),
    ("account_id", "FK to core.dim_accounts; resolved at entry"),
    (
        "security_id",
        "FK to app.securities; resolved at entry; NULL for cash-only events",
    ),
    (
        "security_ref",
        "User-supplied security reference as typed (resolution audit trail)",
    ),
    ("type", "Core taxonomy value (CLI/MCP validate at entry)"),
    ("subtype", "Per-type refinement (tax character, reinvest funding source)"),
    ("event_group_id", "Links legs of one economic event (reinvest pair, merger legs)"),
    ("trade_date", "Trade date (drives holding period); NOT settlement date"),
    ("settlement_date", "Settlement date if supplied; informational"),
    (
        "original_acquisition_date",
        "transfer_in only: shares' original acquisition date",
    ),
    ("quantity", "Units; signed per spec Requirement 6"),
    ("price", "Per-unit price; NULL for non-priced events"),
    ("amount", "Cash effect; signed per spec Requirement 6"),
    ("fees", "Commissions/fees component; folded into cost basis"),
    ("currency_code", "Denominating currency as supplied"),
    ("description", "Free-text description"),
    ("created_at", "When the row was inserted"),
    ("created_by", "'cli' or 'mcp'; future-extensible for multi-user identity"),
    (
        "investment_transaction_id",
        "Predicted gold-key (content hash); populated at INSERT",
    ),
]

_LOT_SELECTIONS_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "investment_transaction_id",
        "FK to the disposal row in core.fct_investment_transactions",
    ),
    (
        "lot_id",
        "FK to core.fct_investment_lots; content-hash id, stable across rebuilds",
    ),
    ("quantity", "Units to draw from this lot for this disposal"),
    ("created_at", "When the selection was recorded"),
]


def _apply_column_comments(
    conn: object, table: str, comments: list[tuple[str, str]]
) -> None:
    for column, comment in comments:
        # COMMENT ON COLUMN does not accept parameterized values; inline a
        # single-quoted literal with standard SQL escaping. Column names come
        # from the static lists above, not user input.
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN {table}.{column} IS '{escaped}'"  # noqa: S608  # static identifier + escaped literal
        )


def migrate(conn: object) -> None:
    """Create the investment tables + settings column. Idempotent."""
    logger.debug("V033: CREATE TABLE IF NOT EXISTS app.securities")
    conn.execute(_CREATE_SECURITIES_SQL)  # type: ignore[union-attr]
    _apply_column_comments(conn, "app.securities", _SECURITIES_COLUMN_COMMENTS)

    logger.debug("V033: CREATE TABLE IF NOT EXISTS raw.manual_investment_transactions")
    conn.execute(_CREATE_MANUAL_INVESTMENT_TRANSACTIONS_SQL)  # type: ignore[union-attr]
    _apply_column_comments(
        conn,
        "raw.manual_investment_transactions",
        _MANUAL_INVESTMENT_COLUMN_COMMENTS,
    )

    logger.debug("V033: CREATE TABLE IF NOT EXISTS app.lot_selections")
    conn.execute(_CREATE_LOT_SELECTIONS_SQL)  # type: ignore[union-attr]
    _apply_column_comments(conn, "app.lot_selections", _LOT_SELECTIONS_COLUMN_COMMENTS)

    # DuckDB rejects `ADD COLUMN ... CHECK (...)` ("Adding columns with
    # constraints not yet supported"), so app.account_settings is rebuilt
    # wholesale with the CHECK baked in — the V032 user_categories idiom.
    cols: list[tuple[str]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'account_settings'
        """
    ).fetchall()
    existing = {c[0] for c in cols}

    if "default_cost_basis_method" not in existing:
        logger.debug(
            "V033: rebuilding app.account_settings to add default_cost_basis_method"
        )
        conn.execute(  # type: ignore[union-attr]
            "CREATE TABLE app.account_settings__v033_tmp AS "
            "SELECT * FROM app.account_settings"
        )
        conn.execute("DROP TABLE app.account_settings")  # type: ignore[union-attr]
        conn.execute(  # type: ignore[union-attr]
            """
            CREATE TABLE app.account_settings (
                account_id           VARCHAR NOT NULL PRIMARY KEY,
                display_name         VARCHAR,
                official_name        VARCHAR,
                last_four            VARCHAR,
                account_subtype      VARCHAR,
                holder_category      VARCHAR,
                iso_currency_code    VARCHAR,
                credit_limit         DECIMAL(18, 2),
                archived             BOOLEAN NOT NULL DEFAULT FALSE,
                include_in_net_worth BOOLEAN NOT NULL DEFAULT TRUE,
                default_cost_basis_method VARCHAR
                    CHECK (default_cost_basis_method IN ('fifo', 'hifo', 'specific', 'average')),
                updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(  # type: ignore[union-attr]
            """
            INSERT INTO app.account_settings (
                account_id, display_name, official_name, last_four,
                account_subtype, holder_category, iso_currency_code,
                credit_limit, archived, include_in_net_worth, updated_at
            )
            SELECT
                account_id, display_name, official_name, last_four,
                account_subtype, holder_category, iso_currency_code,
                credit_limit, archived, include_in_net_worth, updated_at
            FROM app.account_settings__v033_tmp
            """
        )
        conn.execute("DROP TABLE app.account_settings__v033_tmp")  # type: ignore[union-attr]

    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN app.account_settings.default_cost_basis_method "
        "IS 'Per-account cost-basis default (investments-data-model.md); "
        "NULL falls back to global FIFO'"
    )

    logger.debug("V033: investment tables + cost-basis default ready")
