"""Schema catalog service — produces the LLM-facing schema document.

Joins live DuckDB catalog metadata (table/column types and comments) with
hand-authored example queries, filtered to the curated interface tables
declared in `moneybin.tables`.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb

from moneybin import error_codes
from moneybin.database import Database, get_database
from moneybin.errors import UserError
from moneybin.privacy.sql_query import ALLOWED_QUERY_SCHEMAS
from moneybin.tables import GSHEET_CONNECTIONS, IMPORT_LOG, INTERFACE_TABLES

logger = logging.getLogger(__name__)

CONVENTIONS: dict[str, str] = {
    "amount_sign": "negative = expense, positive = income",
    "currency": "DECIMAL(18,2); ISO 4217 codes in currency_code columns",
    "dates": "DATE type; transaction_date is the canonical posting date",
    "ids": (
        "Deterministic SHA-256 truncated to 16 hex chars; "
        "see core.fct_transactions.transaction_id"
    ),
}


@dataclass(frozen=True)
class Example:
    """A single example query for a table."""

    question: str
    sql: str


EXAMPLES: dict[str, list[Example]] = {
    "core.fct_transactions": [
        Example(
            question="Total spending by category last month",
            sql="""
                SELECT category, currency_code, SUM(amount_absolute) AS total
                FROM core.fct_transactions
                WHERE transaction_direction = 'expense'
                  AND transaction_year_month = STRFTIME(CURRENT_DATE - INTERVAL 1 MONTH, '%Y-%m')
                GROUP BY category, currency_code
                ORDER BY ROW_NUMBER() OVER (
                    PARTITION BY currency_code ORDER BY total DESC
                ), currency_code
            """,
        ),
        Example(
            question="Transactions for one account within a date range "
            "(substitute YOUR_ACCOUNT_ID and the real dates)",
            sql="""
                SELECT transaction_date, description, amount, category
                FROM core.fct_transactions
                WHERE account_id = 'YOUR_ACCOUNT_ID'
                  AND transaction_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
                ORDER BY transaction_date DESC
            """,
        ),
        Example(
            question="Monthly spending trend (last 12 months)",
            sql="""
                SELECT transaction_year_month, currency_code,
                       SUM(amount_absolute) AS total_spent
                FROM core.fct_transactions
                WHERE transaction_direction = 'expense'
                  AND transaction_date >= CURRENT_DATE - INTERVAL 12 MONTH
                GROUP BY transaction_year_month, currency_code
                ORDER BY transaction_year_month, currency_code
            """,
        ),
        Example(
            question="Notes on a specific transaction "
            "(substitute YOUR_TRANSACTION_ID; LIST(STRUCT) column)",
            sql="""
                SELECT notes
                FROM core.fct_transactions
                WHERE transaction_id = 'YOUR_TRANSACTION_ID'
                LIMIT 50
            """,
        ),
        Example(
            question="Transactions tagged 'tax:business' "
            "(use ANY(tags), not len(tags) > 0, for NULL-safe filtering)",
            sql="""
                SELECT transaction_id, transaction_date, amount, description, tags
                FROM core.fct_transactions
                WHERE 'tax:business' = ANY(tags)
                ORDER BY transaction_date DESC
                LIMIT 50
            """,
        ),
        Example(
            question="Transactions that have at least one note "
            "(use note_count > 0, not len(notes) > 0, to avoid 3-valued logic)",
            sql="""
                SELECT transaction_id, transaction_date, amount, description, note_count
                FROM core.fct_transactions
                WHERE note_count > 0
                ORDER BY transaction_date DESC
                LIMIT 50
            """,
        ),
    ],
    "core.fct_transaction_lines": [
        Example(
            question="Split-line rows only "
            "(parent transaction is excluded for split transactions)",
            sql="""
                SELECT transaction_id, line_id, line_amount, line_category, line_kind
                FROM core.fct_transaction_lines
                WHERE line_kind = 'split'
                ORDER BY transaction_id, line_id
                LIMIT 50
            """,
        ),
        Example(
            question="Spending by category at the line grain "
            "(splits expand to N rows; unsplit transactions count once as 'whole')",
            sql="""
                SELECT line_category, currency_code, SUM(ABS(line_amount)) AS total
                FROM core.fct_transaction_lines
                WHERE line_amount < 0
                GROUP BY line_category, currency_code
                ORDER BY ROW_NUMBER() OVER (
                    PARTITION BY currency_code ORDER BY total DESC
                ), currency_code
            """,
        ),
    ],
    "core.dim_accounts": [
        Example(
            question="List all accounts with their institution",
            sql="""
                SELECT account_id, account_type, institution_name, source_type
                FROM core.dim_accounts
                ORDER BY institution_name, account_type
            """,
        ),
        Example(
            question="Join accounts to transactions to label by institution",
            sql="""
                SELECT a.institution_name, t.currency_code, COUNT(*) AS txn_count,
                       SUM(t.amount_absolute) AS total_volume
                FROM core.fct_transactions t
                JOIN core.dim_accounts a USING (account_id)
                GROUP BY a.institution_name, t.currency_code
                ORDER BY t.currency_code, total_volume DESC
            """,
        ),
    ],
    "core.bridge_transfers": [
        Example(
            question="Confirmed transfer pairs with debit and credit transaction IDs",
            sql="""
                SELECT b.transfer_id, b.debit_transaction_id,
                       b.credit_transaction_id, b.amount, b.date_offset_days
                FROM core.bridge_transfers b
                ORDER BY b.transfer_id
            """,
        ),
    ],
    "core.bridge_merchant_entities": [
        Example(
            question="Every transaction sharing one source merchant entity "
            "(substitute YOUR_ENTITY_ID)",
            sql="""
                SELECT t.transaction_id, t.transaction_date, t.amount,
                       t.currency_code, b.source_merchant_name
                FROM core.bridge_merchant_entities b
                JOIN core.fct_transactions t
                  ON t.transaction_id = b.transaction_id
                WHERE b.merchant_entity_source_type = 'plaid'
                AND b.merchant_entity_id = 'YOUR_ENTITY_ID'
                ORDER BY t.transaction_date DESC
            """,
        ),
    ],
    "core.dim_categories": [
        Example(
            question="All active categories",
            sql="""
                SELECT category_id, category, subcategory, description
                FROM core.dim_categories
                WHERE is_active
                ORDER BY category, subcategory
            """,
        ),
    ],
    "core.bridge_category_source_map": [
        Example(
            question="Resolve a Plaid category code to a MoneyBin category "
            "(substitute YOUR_DETAILED_CODE and YOUR_PRIMARY_CODE)",
            sql="""
                SELECT category_id FROM core.bridge_category_source_map
                WHERE source_type = 'plaid'
                AND source_category_code IN ('YOUR_DETAILED_CODE', 'YOUR_PRIMARY_CODE')
                ORDER BY code_level = 'detailed' DESC LIMIT 1
            """,
        ),
    ],
    "app.budgets": [
        Example(
            question="Active budgets with their target amounts",
            sql="""
                SELECT * FROM app.budgets ORDER BY category
            """,
        ),
    ],
    "app.transaction_notes": [
        Example(
            question="Notes for a specific transaction "
            "(substitute YOUR_TRANSACTION_ID)",
            sql="""
                SELECT note_id, transaction_id, text, author, created_at
                FROM app.transaction_notes
                WHERE transaction_id = 'YOUR_TRANSACTION_ID'
                ORDER BY created_at
            """,
        ),
    ],
    "app.transaction_tags": [
        Example(
            question="Tags applied to a specific transaction "
            "(substitute YOUR_TRANSACTION_ID)",
            sql="""
                SELECT transaction_id, tag, applied_at, applied_by
                FROM app.transaction_tags
                WHERE transaction_id = 'YOUR_TRANSACTION_ID'
                ORDER BY tag
            """,
        ),
    ],
    "app.transaction_splits": [
        Example(
            question="Split children of a parent transaction "
            "(substitute YOUR_TRANSACTION_ID)",
            sql="""
                SELECT split_id, transaction_id, amount, category, subcategory, ord
                FROM app.transaction_splits
                WHERE transaction_id = 'YOUR_TRANSACTION_ID'
                ORDER BY ord, split_id
            """,
        ),
    ],
    "app.imports": [
        Example(
            question="User-applied labels on import batches",
            sql="""
                SELECT import_id, labels, updated_at, updated_by
                FROM app.imports
                ORDER BY updated_at DESC
                LIMIT 50
            """,
        ),
    ],
    "app.audit_log": [
        Example(
            question="Most recent audit events across all actions",
            sql="""
                SELECT audit_id, occurred_at, actor, action,
                       target_schema, target_table, target_id
                FROM app.audit_log
                ORDER BY occurred_at DESC
                LIMIT 50
            """,
        ),
    ],
    "app.user_reports": [
        Example(
            question="Saved reports and when each was last updated",
            sql="""
                SELECT report_id, name, description, updated_at
                FROM app.user_reports
                WHERE is_active
                ORDER BY updated_at DESC, report_id
                LIMIT 50
            """,
        ),
    ],
    "core.dim_merchants": [
        Example(
            question="Merchants with their canonical names",
            sql="""
                SELECT merchant_id, canonical_name, raw_pattern
                FROM core.dim_merchants
                ORDER BY canonical_name
            """,
        ),
    ],
    "app.categorization_rules": [
        Example(
            question="Active categorization rules",
            sql="""
                SELECT rule_id, merchant_pattern, category, subcategory, priority
                FROM app.categorization_rules
                WHERE is_active
                ORDER BY priority DESC
            """,
        ),
    ],
    "app.transaction_categories": [
        Example(
            question="Per-transaction category assignments",
            sql="""
                SELECT transaction_id, category, subcategory, categorized_by
                FROM app.transaction_categories
                ORDER BY categorized_at DESC
                LIMIT 100
            """,
        ),
    ],
    "app.account_settings": [
        Example(
            question="User-customized account settings (display names, subtypes, etc.)",
            sql="""
                SELECT account_id, display_name, account_subtype, holder_category,
                       archived, include_in_net_worth
                FROM app.account_settings
                ORDER BY account_id
            """,
        ),
    ],
    "app.profile_settings": [
        Example(
            question="Which currency does this profile treat as its home currency?",
            sql="""
                SELECT home_currency
                FROM app.profile_settings
            """,
        ),
    ],
    "app.balance_assertions": [
        Example(
            question="All user-entered balance assertions",
            sql="""
                SELECT account_id, assertion_date, balance, notes
                FROM app.balance_assertions
                ORDER BY account_id, assertion_date DESC
            """,
        ),
        Example(
            question="Latest balance assertion per account",
            sql="""
                SELECT DISTINCT ON (account_id) account_id, assertion_date AS latest_date, balance
                FROM app.balance_assertions
                ORDER BY account_id, assertion_date DESC
            """,
        ),
    ],
    "core.fct_balances": [
        Example(
            question="All balance observations for one account",
            sql="""
                SELECT balance_date, currency_code, balance, source_type, source_ref
                FROM core.fct_balances
                WHERE account_id = 'YOUR_ACCOUNT_ID'
                ORDER BY balance_date DESC
            """,
        ),
    ],
    "core.fct_balances_daily": [
        Example(
            question="Current balance for every account (latest date per account)",
            sql="""
                SELECT account_id, balance_date, currency_code, balance,
                       is_observed, observation_source
                FROM core.fct_balances_daily
                WHERE balance_date = (
                    SELECT MAX(balance_date) FROM core.fct_balances_daily AS b2
                    WHERE b2.account_id = fct_balances_daily.account_id
                )
                ORDER BY account_id
            """,
        ),
        Example(
            question="Daily balance history for one account",
            sql="""
                SELECT balance_date, currency_code, balance, is_observed,
                       reconciliation_delta
                FROM core.fct_balances_daily
                WHERE account_id = 'YOUR_ACCOUNT_ID'
                ORDER BY balance_date DESC
            """,
        ),
    ],
    "core.uncategorized_queue": [
        Example(
            question="Highest-impact uncategorized transactions",
            sql="""
                SELECT account_name, txn_date, amount, currency_code, description,
                       age_days, priority_score
                FROM core.uncategorized_queue
                ORDER BY priority_score DESC
                LIMIT 25
            """,
        ),
    ],
    "reports.net_worth": [
        Example(
            question="Net worth today, one row per currency",
            sql="""
                SELECT currency_code, balance_date, account_count,
                       total_assets, total_liabilities, net_worth
                FROM reports.net_worth
                WHERE balance_date = (SELECT MAX(balance_date) FROM reports.net_worth)
                ORDER BY currency_code
            """,
        ),
        Example(
            question="Net worth trend over the last 12 months (monthly, per currency)",
            sql="""
                SELECT
                    currency_code,
                    STRFTIME(balance_date, '%Y-%m') AS month,
                    LAST(net_worth ORDER BY balance_date) AS end_of_month_net_worth
                FROM reports.net_worth
                WHERE balance_date >= CURRENT_DATE - INTERVAL 12 MONTH
                GROUP BY month, currency_code
                ORDER BY month, currency_code
            """,
        ),
    ],
    "reports.cash_flow": [
        Example(
            question="Monthly net cash flow across all accounts, per currency",
            sql="""
                SELECT currency_code, year_month, SUM(net) AS total_net
                FROM reports.cash_flow
                GROUP BY year_month, currency_code
                ORDER BY year_month, currency_code
            """,
        ),
        Example(
            question="Top outflow categories over the last 12 months, per currency",
            sql="""
                SELECT category, currency_code, SUM(outflow) AS total_outflow
                FROM reports.cash_flow
                WHERE year_month >= strftime(current_date - INTERVAL 12 MONTH, '%Y-%m')
                GROUP BY category, currency_code
                ORDER BY ROW_NUMBER() OVER (
                    PARTITION BY currency_code ORDER BY total_outflow ASC
                ), currency_code
            """,
        ),
    ],
    "reports.spending_trend": [
        Example(
            question="Latest month's spending with MoM/YoY deltas",
            sql="""
                SELECT category, currency_code, year_month,
                       total_spend, mom_pct, yoy_pct
                FROM reports.spending_trend
                WHERE year_month = (SELECT MAX(year_month) FROM reports.spending_trend)
                ORDER BY ROW_NUMBER() OVER (
                    PARTITION BY currency_code ORDER BY total_spend DESC
                ), currency_code
            """,
        ),
    ],
    "reports.recurring_subscriptions": [
        Example(
            question="Active high-confidence subscriptions ordered by annualized cost",
            sql="""
                SELECT merchant_normalized, currency_code, cadence,
                       confidence, avg_amount, annualized_cost
                FROM reports.recurring_subscriptions
                WHERE status = 'active' AND confidence >= 0.7
                ORDER BY ROW_NUMBER() OVER (
                    PARTITION BY currency_code ORDER BY annualized_cost DESC
                ), currency_code
            """,
        ),
    ],
    "reports.merchant_activity": [
        Example(
            question="Top merchants by lifetime spend",
            sql="""
                SELECT merchant_normalized, currency_code, top_category,
                       ROW_NUMBER() OVER (
                           PARTITION BY currency_code ORDER BY total_spend DESC
                       ) AS rank_in_currency,
                       txn_count, total_spend
                FROM reports.merchant_activity
                QUALIFY rank_in_currency <= 25
                ORDER BY rank_in_currency, currency_code
            """,
        ),
    ],
    "reports.large_transactions": [
        Example(
            question="Top 100 transactions by absolute amount, per currency",
            sql="""
                SELECT account_name, merchant_normalized, category,
                       currency_code, txn_date, amount
                FROM reports.large_transactions
                WHERE is_top_100
                ORDER BY ROW_NUMBER() OVER (
                    PARTITION BY currency_code ORDER BY ABS(amount) DESC
                ), currency_code
            """,
        ),
        Example(
            question="Account-level outliers (modified z-score > 2.5), per currency",
            sql="""
                SELECT account_name, currency_code, txn_date,
                       amount_zscore_account, amount
                FROM reports.large_transactions
                WHERE amount_zscore_account > 2.5
                ORDER BY ROW_NUMBER() OVER (
                    PARTITION BY currency_code ORDER BY amount_zscore_account DESC
                ), currency_code
            """,
        ),
    ],
    "reports.balance_drift": [
        Example(
            question="Reconciliation drift sorted by absolute delta, per currency",
            sql="""
                SELECT account_name, currency_code, status, assertion_date,
                       asserted_balance, computed_balance, drift
                FROM reports.balance_drift
                WHERE status IN ('drift', 'warning')
                ORDER BY ROW_NUMBER() OVER (
                    PARTITION BY currency_code ORDER BY drift_abs DESC
                ), currency_code
            """,
        ),
    ],
    "core.dim_securities": [
        Example(
            question="Look up a security by ticker (substitute YOUR_TICKER)",
            sql="""
                SELECT security_id, name, security_type, exchange, currency_code
                FROM core.dim_securities
                WHERE ticker = 'YOUR_TICKER'
            """,
        ),
        Example(
            question="Full securities catalog, grouped by instrument type",
            sql="""
                SELECT security_type, security_id, name, ticker
                FROM core.dim_securities
                ORDER BY security_type, name
            """,
        ),
    ],
    "core.fct_investment_transactions": [
        Example(
            question="Investment transactions for one account within a date range "
            "(substitute YOUR_ACCOUNT_ID and the real dates)",
            sql="""
                SELECT trade_date, type, subtype, security_id, quantity, price, amount
                FROM core.fct_investment_transactions
                WHERE account_id = 'YOUR_ACCOUNT_ID'
                  AND trade_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31'
                ORDER BY trade_date DESC
            """,
        ),
        Example(
            question="Dividend and interest income received, by security",
            sql="""
                SELECT security_id, currency_code, SUM(amount) AS total_income
                FROM core.fct_investment_transactions
                WHERE type IN ('dividend', 'interest')
                GROUP BY security_id, currency_code
                ORDER BY ROW_NUMBER() OVER (
                    PARTITION BY currency_code ORDER BY total_income DESC
                ), currency_code
            """,
        ),
    ],
    "core.fct_investment_lots": [
        Example(
            question="Open tax lots for one account and security "
            "(substitute YOUR_ACCOUNT_ID and YOUR_SECURITY_ID)",
            sql="""
                SELECT lot_id, acquisition_date, remaining_quantity, cost_basis_remaining
                FROM core.fct_investment_lots
                WHERE account_id = 'YOUR_ACCOUNT_ID'
                  AND security_id = 'YOUR_SECURITY_ID'
                  AND is_open
                ORDER BY acquisition_date
            """,
        ),
        Example(
            question="Total remaining cost basis across all open lots in an account "
            "(substitute YOUR_ACCOUNT_ID)",
            sql="""
                SELECT currency_code, SUM(cost_basis_remaining) AS total_basis
                FROM core.fct_investment_lots
                WHERE account_id = 'YOUR_ACCOUNT_ID' AND is_open
                GROUP BY currency_code
                ORDER BY currency_code
            """,
        ),
    ],
    "core.fct_realized_gains": [
        Example(
            question="Realized gains/losses by term (short vs. long) for the current year",
            sql="""
                SELECT term, currency_code, SUM(gain_loss) AS total_gain_loss
                FROM core.fct_realized_gains
                WHERE YEAR(disposal_date) = YEAR(CURRENT_DATE)
                GROUP BY term, currency_code
                ORDER BY term, currency_code
            """,
        ),
        Example(
            question="Realized gains for one security, most recent disposals first "
            "(substitute YOUR_SECURITY_ID)",
            sql="""
                SELECT disposal_date, quantity, proceeds, cost_basis, gain_loss, term
                FROM core.fct_realized_gains
                WHERE security_id = 'YOUR_SECURITY_ID'
                ORDER BY disposal_date DESC
            """,
        ),
    ],
    "core.dim_holdings": [
        Example(
            question="Current holdings across all accounts",
            sql="""
                SELECT account_id, security_id, quantity, cost_basis, average_cost
                FROM core.dim_holdings
                ORDER BY account_id, security_id
            """,
        ),
        Example(
            question="Holdings for one account with average cost per unit "
            "(substitute YOUR_ACCOUNT_ID)",
            sql="""
                SELECT security_id, quantity, cost_basis, average_cost, currency_code
                FROM core.dim_holdings
                WHERE account_id = 'YOUR_ACCOUNT_ID'
                ORDER BY cost_basis DESC
            """,
        ),
        Example(
            question="What are my positions worth, and how fresh is each price? "
            "(market_value is NULL — never zero — when valuation_status is "
            "'unpriced', 'withheld' or 'source_overlap')",
            sql="""
                SELECT security_id, quantity, cost_basis, market_value,
                       unrealized_gain, valuation_status, days_since_observed
                FROM core.dim_holdings
                ORDER BY market_value DESC NULLS LAST
            """,
        ),
        Example(
            question="Which positions carry no market value, and why",
            sql="""
                SELECT account_id, security_id, quantity, cost_basis,
                       valuation_status
                FROM core.dim_holdings
                WHERE valuation_status IN ('unpriced', 'withheld', 'source_overlap')
                ORDER BY account_id, security_id
            """,
        ),
    ],
    "core.fct_security_prices": [
        Example(
            question="Price history for one security, most recent first "
            "(substitute YOUR_SECURITY_ID)",
            sql="""
                SELECT price_date, close, quote_currency, source_type
                FROM core.fct_security_prices
                WHERE security_id = 'YOUR_SECURITY_ID'
                ORDER BY price_date DESC
            """,
        ),
        Example(
            question="Latest known close for every security",
            sql="""
                SELECT security_id, quote_currency, close, price_date
                FROM core.fct_security_prices
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY security_id, quote_currency
                    ORDER BY price_date DESC
                ) = 1
            """,
        ),
    ],
}

_BEYOND_NOTE = (
    "The tables above are the curated query surface. sql_query also reads raw "
    "ingest (raw) and staging (prep) — use them only when the curated tables "
    "cannot answer the question, and note they carry 34 column declarations "
    "with every other value masked only when it is a text or integer value "
    "shaped like an SSN or a run of 8 or more digits; a DECIMAL or FLOAT is "
    "not scanned at all. Provenance (meta) and seed data (seeds) are not "
    "readable through this surface."
)
# `SHOW ALL TABLES`, not a `duckdb_tables()` / `duckdb_views()` query. This
# string is served only on the agent path, so it has to run through
# `sql_query` — and a table-valued function parses to a table node whose
# schema and name are both empty, which the schema gate can never resolve to
# an allowed schema. Every catalog-function spelling of this query is refused
# (`details.disallowed: [""]` is that empty name). `SHOW` names no table, so
# `_shown_schema` returns None, the gate finds nothing to resolve, and it
# passes — while still listing views, which is the whole point: every `prep`
# model and every runtime-minted `raw.gsheet_<alias>` / `raw.pdf_<alias>` seed
# view is a view, not a table. It also carries column names and types, which
# `duckdb_tables().comment` did not. This is the same statement `sql_query`'s
# own refusal hint recommends; the two discovery paths must not disagree.
_BEYOND_QUERY = "SHOW ALL TABLES"


@contextmanager
def _read_snapshot() -> Generator[Database]:
    """Yield a connection whose every read sees one state of the database.

    Both builders below read the catalog more than once, and one connection is
    not one snapshot: DuckDB autocommits each statement, so a relation another
    connection commits between two reads lands in the later one and not the
    earlier. That describes a database state that never existed — a seed view
    listed as uncurated while its curated entry is already there.
    """
    with get_database(read_only=True) as db:
        db.begin()
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise


def build_schema_doc() -> dict[str, Any]:
    """Return the schema document for the LLM-facing catalog.

    Reads `duckdb_tables()` and `duckdb_columns()` for every interface
    table that exists in the live database; missing tables are silently
    skipped (the test/dev DB may not have every interface table).
    """
    with _read_snapshot() as db:
        return _schema_doc(db)


def _schema_doc(db: Database) -> dict[str, Any]:
    """Build the document from a connection the caller already holds.

    Split out for `build_live_catalog`, which needs the curated names and the
    relation rows to come from one snapshot. Across two connections, a seed
    view created between them is listed as uncurated even though
    `sql_schema(table=...)` already answers it with a curated entry.
    """
    interface_names = [t.full_name for t in INTERFACE_TABLES]
    placeholders = ",".join(["?"] * len(interface_names))
    # Union tables and views — `duckdb_tables()` excludes views, but
    # interface objects like `core.dim_categories` are SQLMesh-managed views.
    rows = db.execute(
        f"""
        WITH interface_objects AS (
            SELECT schema_name, table_name, comment, 'table' AS kind
            FROM duckdb_tables()
            UNION ALL
            SELECT schema_name, view_name AS table_name, comment, 'view' AS kind
            FROM duckdb_views()
            WHERE NOT internal
        )
        SELECT
            t.schema_name || '.' || t.table_name AS full_name,
            COALESCE(t.comment, '') AS table_comment,
            t.kind,
            c.column_name,
            c.data_type,
            c.is_nullable,
            COALESCE(c.comment, '') AS column_comment
        FROM interface_objects t
        JOIN duckdb_columns() c
          ON t.schema_name = c.schema_name AND t.table_name = c.table_name
        WHERE t.schema_name || '.' || t.table_name IN ({placeholders})
        ORDER BY t.schema_name, t.table_name, c.column_index
        """,  # noqa: S608  # INTERFACE_TABLES is a compile-time allowlist, not user input
        interface_names,
    ).fetchall()
    seed_view_entries = _gsheet_seed_views(db)
    pdf_view_entries = _pdf_seed_views(db)

    tables_by_name: dict[str, dict[str, Any]] = {}
    for full_name, table_comment, kind, col_name, dtype, nullable, col_comment in rows:
        entry = tables_by_name.setdefault(
            full_name,
            {
                "name": full_name,
                "purpose": table_comment,
                "kind": kind,
                "columns": [],
                "examples": [
                    {"question": ex.question, "sql": ex.sql}
                    for ex in EXAMPLES.get(full_name, [])
                ],
            },
        )
        entry["columns"].append({
            "name": col_name,
            "type": dtype,
            "nullable": bool(nullable),
            "comment": col_comment,
        })

    tables = list(tables_by_name.values())
    tables.extend(seed_view_entries)
    tables.extend(pdf_view_entries)
    logger.info(f"Schema doc built: {len(tables)} interface tables present")

    return {
        "version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "conventions": dict(CONVENTIONS),
        "tables": tables,
        "beyond_the_interface": {
            "note": _BEYOND_NOTE,
            "catalog_query": _BEYOND_QUERY,
        },
    }


def build_live_catalog(schema: str | None = None) -> list[dict[str, Any]]:
    """Return every queryable relation as ``{name, schema, kind, curated}``.

    The curated document (`build_schema_doc`) covers the `TableRef`s tagged
    ``audience="interface"`` plus the runtime `raw.gsheet_<alias>` /
    `raw.pdf_<alias>` seed views, so most relations `sql_query` reads happily —
    every other `raw` and `prep` model, and the internal `app` tables — have no
    entry anywhere. This listing closes that gap without widening the curated
    surface: it carries names and kinds, never columns or data.

    ``curated`` is read off that document rather than off ``INTERFACE_TABLES``,
    because the seed views are curated at runtime and can never be `TableRef`s
    — their alias is chosen at connection time. Deriving it from the static set
    would flag a relation uncurated here while `sql_schema(table=...)` answers
    it with a full entry, and send the agent to a bare `DESCRIBE` instead.

    Bounded by ``ALLOWED_QUERY_SCHEMAS`` so listing can never exceed querying.
    That makes it a strictly narrower disclosure than the `SHOW ALL TABLES`
    the catalog footer recommends, which reaches `meta` and `seeds` because it
    names no table for the query gate to resolve.
    """
    schemas = sorted(ALLOWED_QUERY_SCHEMAS)
    if schema is not None:
        # DuckDB folds unquoted identifiers, so `sql_query` reads `RAW.x` and
        # `tables_outside_schemas` normalizes to match. Comparing the caller's
        # raw casing against a lowercase allowlist is what once refused
        # `DESCRIBE CORE.dim_accounts` while allowing the equivalent SELECT.
        schema = schema.lower()
        if schema not in ALLOWED_QUERY_SCHEMAS:
            raise UserError(
                f"Queries are limited to these schemas: {', '.join(schemas)}.",
                code=error_codes.SQL_SCHEMA_NOT_ALLOWED,
                hint=(
                    "Those carry per-column privacy classifications or a "
                    "content-net floor, which is what makes masking sound; "
                    "every other schema is internal and has neither."
                ),
                details={"disallowed": [schema]},
            )
        schemas = [schema]

    placeholders = ",".join(["?"] * len(schemas))
    with _read_snapshot() as db:
        curated = {t["name"] for t in _schema_doc(db)["tables"]}
        rows = db.execute(
            f"""
            SELECT schema_name, table_name, 'table' AS kind
            FROM duckdb_tables()
            WHERE schema_name IN ({placeholders})
            UNION ALL
            SELECT schema_name, view_name AS table_name, 'view' AS kind
            FROM duckdb_views()
            WHERE NOT internal AND schema_name IN ({placeholders})
            ORDER BY schema_name, table_name
            """,  # noqa: S608  # placeholders only; schema values are parameterized
            schemas + schemas,
        ).fetchall()

    return [
        {
            "name": f"{schema_name}.{table_name}",
            "schema": schema_name,
            "kind": kind,
            "curated": f"{schema_name}.{table_name}" in curated,
        }
        for schema_name, table_name, kind in rows
    ]


def _gsheet_seed_views(db: Database) -> list[dict[str, Any]]:
    """Return one entry per active seed connection whose view exists in the catalog.

    Skips disconnected connections, connections without an alias (transactions
    adapter), and aliases whose view hasn't been materialized yet (e.g. a
    connection created but never pulled). Mirrors interface-table entry shape.

    Takes the caller's live read-only ``db`` rather than opening a second
    connection — build_schema_doc already holds one open.
    """
    try:
        connections = db.execute(
            f"""
            SELECT connection_id, alias
            FROM {GSHEET_CONNECTIONS.full_name}
            WHERE adapter = 'seed'
              AND status != 'disconnected'
              AND alias IS NOT NULL
            ORDER BY created_at ASC, connection_id ASC
            """  # noqa: S608  # GSHEET_CONNECTIONS is a TableRef constant
        ).fetchall()
    except duckdb.CatalogException:
        # Table absent on bare DBs before init_schemas — no seed views to add.
        return []

    if not connections:
        return []

    # Catalog membership filter — skip aliases without a materialized view.
    live_views = {
        row[0]
        for row in db.execute(
            """
            SELECT view_name FROM duckdb_views()
            WHERE schema_name = 'raw' AND view_name LIKE 'gsheet_%'
            """
        ).fetchall()
    }

    entries: list[dict[str, Any]] = []
    for _connection_id, alias in connections:
        view_name = f"gsheet_{alias}"
        if view_name not in live_views:
            continue
        cols = db.execute(
            """
            SELECT column_name, data_type
            FROM duckdb_columns()
            WHERE schema_name = 'raw' AND table_name = ?
            ORDER BY column_index
            """,
            [view_name],
        ).fetchall()
        entries.append({
            "name": f"raw.{view_name}",
            "kind": "view",
            "purpose": (
                f"Live mirror of Google Sheets seed connection (alias={alias})."
            ),
            "columns": [
                {
                    "name": col_name,
                    "type": dtype,
                    "nullable": True,
                    "comment": "",
                }
                for col_name, dtype in cols
            ],
            "examples": [],
        })
    return entries


def _pdf_seed_views(db: Database) -> list[dict[str, Any]]:
    """Return one entry per successfully-imported PDF whose view exists in the catalog.

    Skips failed or in-progress imports, and aliases whose view hasn't been
    created yet. Mirrors the _gsheet_seed_views entry shape.

    Takes the caller's live read-only ``db`` rather than opening a second
    connection — build_schema_doc already holds one open.
    """
    try:
        aliases = db.execute(
            f"""
            SELECT DISTINCT source_origin
            FROM {IMPORT_LOG.full_name}
            WHERE source_type = 'pdf'
              AND status = 'complete'
            ORDER BY source_origin ASC
            """  # noqa: S608 — compile-time TableRef constant, no user input
        ).fetchall()
    except duckdb.CatalogException:
        # Table absent on bare DBs before init_schemas — no PDF views to add.
        return []

    if not aliases:
        return []

    # Catalog membership filter — skip aliases without a materialized view.
    live_views = {
        row[0]
        for row in db.execute(
            """
            SELECT view_name FROM duckdb_views()
            WHERE schema_name = 'raw' AND view_name LIKE 'pdf_%'
            """
        ).fetchall()
    }

    entries: list[dict[str, Any]] = []
    for (alias,) in aliases:
        view_name = f"pdf_{alias}"
        if view_name not in live_views:
            continue
        cols = db.execute(
            """
            SELECT column_name, data_type
            FROM duckdb_columns()
            WHERE schema_name = 'raw' AND table_name = ?
            ORDER BY column_index
            """,
            [view_name],
        ).fetchall()
        entries.append({
            "name": f"raw.{view_name}",
            "kind": "view",
            "purpose": f"PDF seed data imported from file (alias={alias}).",
            "columns": [
                {
                    "name": col_name,
                    "type": dtype,
                    "nullable": True,
                    "comment": "",
                }
                for col_name, dtype in cols
            ],
            "examples": [],
        })
    return entries
