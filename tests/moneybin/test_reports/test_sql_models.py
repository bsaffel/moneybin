"""Live execution tests for SQL-backed report models."""

from __future__ import annotations

import math
import re
from collections.abc import Generator
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import SQLMESH_ROOT, Database

_REPORT_MODELS = SQLMESH_ROOT / "models" / "reports"


def _model_body(name: str) -> str:
    """Return a report model's executable SQL without its MODEL header."""
    raw = (_REPORT_MODELS / f"{name}.sql").read_text()
    return re.sub(
        r"^.*?MODEL\s*\(.*?\);\s*",
        "",
        raw,
        count=1,
        flags=re.DOTALL,
    ).strip()


def _install_report(db: Database, name: str) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute(  # noqa: S608  # test-selected shipped model name
        f"CREATE OR REPLACE VIEW reports.{name} AS {_model_body(name)}"
    )


@pytest.fixture()
def model_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    database = Database(
        tmp_path / "report-model.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    yield database
    database.close()


def _install_balance_drift_sources(
    model_db: Database, *, currency: str | None = "USD"
) -> None:
    model_db.execute("""
        CREATE TABLE core.dim_accounts (
            account_id VARCHAR,
            display_name VARCHAR,
            archived BOOLEAN,
            currency_code VARCHAR
        )
    """)
    model_db.execute("""
        CREATE TABLE core.fct_balances_daily (
            account_id VARCHAR,
            balance_date DATE,
            balance DECIMAL(18, 2),
            is_observed BOOLEAN,
            reconciliation_delta DECIMAL(18, 2),
            currency_code VARCHAR
        )
    """)
    model_db.execute(
        """
        INSERT INTO core.dim_accounts VALUES (?, ?, ?, ?)
        """,
        ["checking", "Checking", False, currency],
    )


def test_balance_drift_uses_independent_transaction_derived_position(
    model_db: Database,
) -> None:
    """Assertion drift compares against the pre-assertion computed position."""
    _install_balance_drift_sources(model_db)
    model_db.execute(
        """
        INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
        VALUES (?, ?, ?)
        """,
        ["checking", "2026-04-01", Decimal("1200.00")],
    )
    # Independently derived: the transaction-based position is $1,000 and the
    # user assertion is $1,200, so fct_balances_daily resets to $1,200 and
    # records reconciliation_delta = $1,200 - $1,000 = $200.
    model_db.execute(
        """
        INSERT INTO core.fct_balances_daily VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            "checking",
            "2026-04-01",
            Decimal("1200.00"),
            True,
            Decimal("200.00"),
            "USD",
        ],
    )

    _install_report(model_db, "balance_drift")

    row = model_db.execute(
        """
        SELECT asserted_balance, computed_balance, drift, drift_abs, status
        FROM reports.balance_drift
        """
    ).fetchone()
    assert row == (
        Decimal("1200.00"),
        Decimal("1000.00"),
        Decimal("200.00"),
        Decimal("200.00"),
        "drift",
    )


def test_balance_drift_without_reconciliation_adjustment_uses_daily_balance(
    model_db: Database,
) -> None:
    """An interpolated daily balance has no reconciliation adjustment."""
    _install_balance_drift_sources(model_db)
    model_db.execute(
        """
        INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
        VALUES (?, ?, ?)
        """,
        ["checking", "2026-04-01", Decimal("1200.00")],
    )
    model_db.execute(
        """
        INSERT INTO core.fct_balances_daily VALUES (?, ?, ?, ?, ?, ?)
        """,
        ["checking", "2026-04-01", Decimal("1200.00"), False, None, "USD"],
    )

    _install_report(model_db, "balance_drift")

    row = model_db.execute(
        """
        SELECT computed_balance, drift, status
        FROM reports.balance_drift
        """
    ).fetchone()
    assert row == (Decimal("1200.00"), Decimal("0.00"), "clean")


def test_balance_drift_first_observation_has_no_independent_position(
    model_db: Database,
) -> None:
    """The first observed balance has no prior transaction-derived anchor."""
    _install_balance_drift_sources(model_db)
    model_db.execute(
        """
        INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
        VALUES (?, ?, ?)
        """,
        ["checking", "2026-04-01", Decimal("1200.00")],
    )
    model_db.execute(
        """
        INSERT INTO core.fct_balances_daily VALUES (?, ?, ?, ?, ?, ?)
        """,
        ["checking", "2026-04-01", Decimal("1200.00"), True, None, "USD"],
    )

    _install_report(model_db, "balance_drift")

    row = model_db.execute(
        """
        SELECT computed_balance, drift, status
        FROM reports.balance_drift
        """
    ).fetchone()
    assert row == (None, None, "no-data")


def test_spending_trend_uses_zero_filled_calendar_months(
    model_db: Database,
) -> None:
    """Calendar comparisons include missing category-months as zero spend."""
    model_db.execute("""
        CREATE TABLE core.dim_accounts (
            account_id VARCHAR,
            archived BOOLEAN
        )
    """)
    model_db.execute("""
        CREATE TABLE core.fct_transactions (
            account_id VARCHAR,
            transaction_date DATE,
            amount DECIMAL(18, 2),
            category VARCHAR,
            is_transfer BOOLEAN,
            currency_code VARCHAR
        )
    """)
    model_db.execute(
        """
        INSERT INTO core.dim_accounts VALUES (?, ?)
        """,
        ["checking", False],
    )
    # Independently derived calendar series for Food:
    #   2024-01 = 100, 2024-02 = 0, 2024-03 = 300
    #   2025-03 = 500
    # Therefore Mar-2024 trailing average = (100 + 0 + 300) / 3,
    # and Mar-2025 YoY delta = 500 - 300. Every row is USD, so per-currency
    # segmentation leaves these figures untouched (Requirement 7).
    model_db.execute(
        """
        INSERT INTO core.fct_transactions VALUES
            (?, ?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?, ?)
        """,
        [
            "checking",
            "2024-01-15",
            Decimal("-100.00"),
            "Food",
            False,
            "USD",
            "checking",
            "2024-03-15",
            Decimal("-300.00"),
            "Food",
            False,
            "USD",
            "checking",
            "2025-03-15",
            Decimal("-500.00"),
            "Food",
            False,
            "USD",
        ],
    )

    _install_report(model_db, "spending_trend")

    february = model_db.execute(
        """
        SELECT total_spend, txn_count, prev_month_spend, mom_delta, mom_pct
        FROM reports.spending_trend
        WHERE category = 'Food' AND year_month = '2024-02'
        """
    ).fetchone()
    assert february == (
        Decimal("0.00"),
        0,
        Decimal("100.00"),
        Decimal("-100.00"),
        -1.0,
    )

    march_2024 = model_db.execute(
        """
        SELECT prev_month_spend, mom_delta, mom_pct, trailing_3mo_avg
        FROM reports.spending_trend
        WHERE category = 'Food' AND year_month = '2024-03'
        """
    ).fetchone()
    assert march_2024 is not None
    assert march_2024[:3] == (Decimal("0.00"), Decimal("300.00"), None)
    assert math.isclose(float(march_2024[3]), (100 + 0 + 300) / 3)

    march_2025 = model_db.execute(
        """
        SELECT prev_year_spend, yoy_delta, yoy_pct, trailing_3mo_avg
        FROM reports.spending_trend
        WHERE category = 'Food' AND year_month = '2025-03'
        """
    ).fetchone()
    assert march_2025 is not None
    assert march_2025[:2] == (Decimal("300.00"), Decimal("200.00"))
    assert math.isclose(float(march_2025[2]), 2 / 3)
    assert math.isclose(float(march_2025[3]), (0 + 0 + 500) / 3)


# --------------------------------------------------------------------------
# No-silent-blend invariant (multi-currency.md Requirements 5 and 7).
#
# Every report that sums money segments per currency_code. Each fixture below
# holds equal amounts in two currencies, so a blended figure is exactly double
# a segmented one — the two answers can never be confused.
# --------------------------------------------------------------------------


def _install_transaction_sources(model_db: Database) -> None:
    """Create the core tables the transaction-grain report models read."""
    model_db.execute("""
        CREATE TABLE core.dim_accounts (
            account_id VARCHAR,
            display_name VARCHAR,
            archived BOOLEAN,
            include_in_net_worth BOOLEAN,
            currency_code VARCHAR
        )
    """)
    model_db.execute("""
        CREATE TABLE core.fct_transactions (
            transaction_id VARCHAR,
            account_id VARCHAR,
            transaction_date DATE,
            amount DECIMAL(18, 2),
            description VARCHAR,
            merchant_id VARCHAR,
            merchant_name VARCHAR,
            category VARCHAR,
            is_transfer BOOLEAN,
            currency_code VARCHAR
        )
    """)
    model_db.execute(
        """
        INSERT INTO core.dim_accounts
            (account_id, display_name, archived, include_in_net_worth, currency_code)
        VALUES ('checking', 'Checking', FALSE, TRUE, 'USD')
        """
    )


def _months_ago(count: int) -> date:
    """First day of the month ``count`` months before the current one."""
    today = date.today()
    index = today.year * 12 + today.month - 1 - count
    return date(index // 12, index % 12 + 1, 1)


def _add_transaction(
    model_db: Database,
    *,
    transaction_id: str,
    date: str,
    amount: str,
    currency: str | None,
    category: str | None = "Food",
    merchant_id: str | None = "m1",
    merchant_name: str | None = "Cafe",
) -> None:
    model_db.execute(
        """
        INSERT INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             merchant_id, merchant_name, category, is_transfer, currency_code)
        VALUES (?, 'checking', ?, ?, 'txn', ?, ?, ?, FALSE, ?)
        """,
        [
            transaction_id,
            date,
            Decimal(amount),
            merchant_id,
            merchant_name,
            category,
            currency,
        ],
    )


def test_net_worth_segments_totals_per_currency(model_db: Database) -> None:
    """A USD and a EUR balance never sum into one net-worth figure."""
    model_db.execute("""
        CREATE TABLE core.dim_accounts (
            account_id VARCHAR,
            include_in_net_worth BOOLEAN,
            archived BOOLEAN
        )
    """)
    model_db.execute("""
        CREATE TABLE core.fct_balances_daily (
            account_id VARCHAR,
            balance_date DATE,
            balance DECIMAL(18, 2),
            currency_code VARCHAR
        )
    """)
    model_db.execute(
        """
        INSERT INTO core.dim_accounts VALUES ('usd', TRUE, FALSE), ('eur', TRUE, FALSE)
        """
    )
    model_db.execute(
        """
        INSERT INTO core.fct_balances_daily VALUES
            ('usd', '2024-01-01', 100.00, 'USD'),
            ('eur', '2024-01-01', 100.00, 'EUR')
        """
    )

    _install_report(model_db, "net_worth")

    rows = model_db.execute(
        """
        SELECT currency_code, net_worth, account_count
        FROM reports.net_worth
        WHERE balance_date = '2024-01-01'
        ORDER BY currency_code
        """
    ).fetchall()
    assert rows == [
        ("EUR", Decimal("100.00"), 1),
        ("USD", Decimal("100.00"), 1),
    ]


def test_net_worth_keeps_unknown_currency_as_its_own_segment(
    model_db: Database,
) -> None:
    """A NULL currency_code segments on its own, never folded into a known one."""
    model_db.execute("""
        CREATE TABLE core.dim_accounts (
            account_id VARCHAR,
            include_in_net_worth BOOLEAN,
            archived BOOLEAN
        )
    """)
    model_db.execute("""
        CREATE TABLE core.fct_balances_daily (
            account_id VARCHAR,
            balance_date DATE,
            balance DECIMAL(18, 2),
            currency_code VARCHAR
        )
    """)
    model_db.execute(
        """
        INSERT INTO core.dim_accounts
        VALUES ('usd', TRUE, FALSE), ('unknown', TRUE, FALSE)
        """
    )
    model_db.execute(
        """
        INSERT INTO core.fct_balances_daily VALUES
            ('usd', '2024-01-01', 100.00, 'USD'),
            ('unknown', '2024-01-01', 100.00, NULL)
        """
    )

    _install_report(model_db, "net_worth")

    rows = model_db.execute(
        """
        SELECT currency_code, net_worth
        FROM reports.net_worth
        WHERE balance_date = '2024-01-01'
        ORDER BY currency_code NULLS LAST
        """
    ).fetchall()
    assert rows == [("USD", Decimal("100.00")), (None, Decimal("100.00"))]


def test_cash_flow_segments_one_account_holding_two_currencies(
    model_db: Database,
) -> None:
    """A USD card carrying a EUR purchase reports each currency separately."""
    _install_transaction_sources(model_db)
    _add_transaction(
        model_db, transaction_id="t1", date="2024-01-05", amount="-100", currency="USD"
    )
    _add_transaction(
        model_db, transaction_id="t2", date="2024-01-06", amount="-100", currency="EUR"
    )

    _install_report(model_db, "cash_flow")

    rows = model_db.execute(
        """
        SELECT currency_code, outflow, net, txn_count
        FROM reports.cash_flow
        WHERE year_month = '2024-01'
        ORDER BY currency_code
        """
    ).fetchall()
    assert rows == [
        ("EUR", Decimal("-100.00"), Decimal("-100.00"), 1),
        ("USD", Decimal("-100.00"), Decimal("-100.00"), 1),
    ]


def test_spending_trend_segments_one_category_across_currencies(
    model_db: Database,
) -> None:
    """Equal USD and EUR spend in one category never becomes a doubled total."""
    _install_transaction_sources(model_db)
    _add_transaction(
        model_db, transaction_id="t1", date="2024-01-05", amount="-100", currency="USD"
    )
    _add_transaction(
        model_db, transaction_id="t2", date="2024-01-06", amount="-100", currency="EUR"
    )

    _install_report(model_db, "spending_trend")

    rows = model_db.execute(
        """
        SELECT currency_code, total_spend, txn_count
        FROM reports.spending_trend
        WHERE year_month = '2024-01' AND category = 'Food'
        ORDER BY currency_code
        """
    ).fetchall()
    assert rows == [
        ("EUR", Decimal("100.00"), 1),
        ("USD", Decimal("100.00"), 1),
    ]


def test_spending_trend_densifies_only_observed_currency_pairs(
    model_db: Database,
) -> None:
    """The calendar grid fills months per observed (category, currency) pair.

    Densifying every category against every currency would invent
    (Rent, EUR) rows for a user who only ever paid rent in USD.
    """
    _install_transaction_sources(model_db)
    _add_transaction(
        model_db,
        transaction_id="t1",
        date="2024-01-05",
        amount="-100",
        currency="USD",
        category="Rent",
    )
    _add_transaction(
        model_db,
        transaction_id="t2",
        date="2024-02-06",
        amount="-100",
        currency="EUR",
        category="Food",
    )

    _install_report(model_db, "spending_trend")

    pairs = model_db.execute(
        """
        SELECT DISTINCT category, currency_code
        FROM reports.spending_trend
        ORDER BY category
        """
    ).fetchall()
    assert pairs == [("Food", "EUR"), ("Rent", "USD")]


def test_merchant_activity_segments_one_merchant_across_currencies(
    model_db: Database,
) -> None:
    """One merchant billed in two currencies yields one row per currency."""
    _install_transaction_sources(model_db)
    _add_transaction(
        model_db, transaction_id="t1", date="2024-01-05", amount="-100", currency="USD"
    )
    _add_transaction(
        model_db, transaction_id="t2", date="2024-01-06", amount="-100", currency="EUR"
    )

    _install_report(model_db, "merchant_activity")

    rows = model_db.execute(
        """
        SELECT currency_code, total_spend, txn_count
        FROM reports.merchant_activity
        WHERE merchant_id = 'm1'
        ORDER BY currency_code
        """
    ).fetchall()
    assert rows == [
        ("EUR", Decimal("100.00"), 1),
        ("USD", Decimal("100.00"), 1),
    ]


def test_large_transactions_scores_against_a_same_currency_baseline(
    model_db: Database,
) -> None:
    """Category z-scores use a per-currency median and MAD, not a blended one.

    USD spend in Food is 10/20/30/40/50 (median 30, MAD 10) and EUR spend is
    1000/2000/3000/4000/5000 (median 3000, MAD 1000). Both scales put their
    largest charge at (max - median) / (1.4826 * MAD) = 20 / 14.826 = 1.349.
    Blending the ten rows moves the median to (50 + 1000) / 2 = 525, which
    scores the largest USD charge far below zero instead.
    """
    _install_transaction_sources(model_db)
    for index, amount in enumerate((10, 20, 30, 40, 50), start=1):
        _add_transaction(
            model_db,
            transaction_id=f"usd{index}",
            date="2024-01-05",
            amount=f"-{amount}",
            currency="USD",
        )
    for index, amount in enumerate((1000, 2000, 3000, 4000, 5000), start=1):
        _add_transaction(
            model_db,
            transaction_id=f"eur{index}",
            date="2024-01-05",
            amount=f"-{amount}",
            currency="EUR",
        )

    _install_report(model_db, "large_transactions")

    rows = model_db.execute(
        """
        SELECT currency_code, amount_zscore_category
        FROM reports.large_transactions
        WHERE transaction_id IN ('usd5', 'eur5')
        ORDER BY currency_code
        """
    ).fetchall()
    assert [row[0] for row in rows] == ["EUR", "USD"]
    expected = 20 / (1.4826 * 10)
    for _, zscore in rows:
        assert zscore is not None
        assert math.isclose(float(zscore), expected, rel_tol=1e-9)


def test_large_transactions_ranks_top_100_within_each_currency(
    model_db: Database,
) -> None:
    """is_top_100 compares like units — a small USD charge is not outranked."""
    _install_transaction_sources(model_db)
    _add_transaction(
        model_db, transaction_id="usd1", date="2024-01-05", amount="-5", currency="USD"
    )
    for index in range(1, 102):
        _add_transaction(
            model_db,
            transaction_id=f"eur{index}",
            date="2024-01-05",
            amount=f"-{1000 + index}",
            currency="EUR",
        )

    _install_report(model_db, "large_transactions")

    usd_flag = model_db.execute(
        "SELECT is_top_100 FROM reports.large_transactions WHERE transaction_id = 'usd1'"
    ).fetchone()
    assert usd_flag == (True,)
    smallest_eur = model_db.execute(
        "SELECT is_top_100 FROM reports.large_transactions WHERE transaction_id = 'eur1'"
    ).fetchone()
    assert smallest_eur == (False,)


def test_recurring_subscriptions_does_not_interleave_two_currency_streams(
    model_db: Database,
) -> None:
    """Two same-priced monthly streams stay two monthly candidates.

    Merging them interleaves the dates, halving the inferred cadence and
    misreporting two monthly subscriptions as one biweekly charge.
    """
    _install_transaction_sources(model_db)
    for index in range(1, 5):
        month_start = _months_ago(index)
        _add_transaction(
            model_db,
            transaction_id=f"usd{index}",
            date=month_start.isoformat(),
            amount="-20",
            currency="USD",
        )
        _add_transaction(
            model_db,
            transaction_id=f"eur{index}",
            date=month_start.replace(day=15).isoformat(),
            amount="-20",
            currency="EUR",
        )

    _install_report(model_db, "recurring_subscriptions")

    rows = model_db.execute(
        """
        SELECT currency_code, cadence, occurrence_count
        FROM reports.recurring_subscriptions
        WHERE merchant_id = 'm1'
        ORDER BY currency_code
        """
    ).fetchall()
    assert rows == [("EUR", "monthly", 4), ("USD", "monthly", 4)]


def test_balance_drift_withholds_a_drift_across_two_currencies(
    model_db: Database,
) -> None:
    """A balance stating its own currency is never subtracted from another one.

    Reachable through an ordinary sequence this milestone itself creates: an
    OFX or Plaid observation states its currency (fct_balances prefers the
    observation's own over the account's), `system doctor` flags the account as
    unknown-currency, and the user runs the fix it recommends —
    `accounts set --currency` — choosing a different code. The account override
    and the historical observation then disagree, and subtracting them produces
    a number in no unit at all, labelled with the newer one.
    """
    _install_balance_drift_sources(model_db, currency="USD")
    model_db.execute(
        """
        INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
        VALUES ('checking', '2024-01-02', 105.00)
        """
    )
    model_db.execute(
        """
        INSERT INTO core.fct_balances_daily VALUES
            ('checking', '2024-01-02', 100.00, FALSE, NULL, 'EUR')
        """
    )

    _install_report(model_db, "balance_drift")

    row = model_db.execute(
        "SELECT computed_balance, drift, status FROM reports.balance_drift"
    ).fetchone()
    # 5.00 here would be 105 USD minus 100 EUR reported as USD.
    assert row == (None, None, "currency-mismatch")


@pytest.mark.parametrize(
    ("account_currency", "observation_currency"),
    [("USD", None), (None, "USD"), (None, None)],
    ids=["observation-unknown", "account-unknown", "both-unknown"],
)
def test_balance_drift_still_reports_when_one_side_has_no_currency(
    model_db: Database,
    account_currency: str | None,
    observation_currency: str | None,
) -> None:
    """An unknown currency is not a contradiction, so the drift still reports.

    The guard withholds only when two *known* currencies disagree. Withholding
    on an unknown side instead would blank this report for every account nobody
    has assigned a currency to yet — which is the state a fresh import leaves
    behind, and precisely the population `system doctor`'s currency_integrity
    check exists to walk the user out of.

    The both-known cases are covered above; only these three branches of the
    `NOT ... IS NULL AND NOT ... IS NULL` guard were unexercised, so a change
    that started (or stopped) withholding here would not have been caught.
    """
    _install_balance_drift_sources(model_db, currency=account_currency)
    model_db.execute(
        """
        INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
        VALUES ('checking', '2024-01-02', 105.00)
        """
    )
    model_db.execute(
        """
        INSERT INTO core.fct_balances_daily VALUES
            ('checking', '2024-01-02', 100.00, FALSE, NULL, ?)
        """,
        [observation_currency],
    )

    _install_report(model_db, "balance_drift")

    row = model_db.execute(
        "SELECT computed_balance, drift, status FROM reports.balance_drift"
    ).fetchone()
    # 105.00 asserted - 100.00 computed = 5.00 drift; the model's own bands are
    # clean (<1) / warning (<10) / drift (>=10), so 5.00 is 'warning'. The point
    # is that a position is computed at all — the withheld case reports
    # (None, None, 'currency-mismatch').
    assert row == (Decimal("100.00"), Decimal("5.00"), "warning")


def test_balance_drift_reports_the_currency_the_position_is_denominated_in(
    model_db: Database,
) -> None:
    """Drift is a same-account comparison, so it names one currency per row."""
    _install_balance_drift_sources(model_db, currency="EUR")
    model_db.execute(
        """
        INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
        VALUES ('checking', '2024-01-02', 105.00)
        """
    )
    model_db.execute(
        """
        INSERT INTO core.fct_balances_daily VALUES
            ('checking', '2024-01-02', 100.00, FALSE, NULL, 'EUR')
        """
    )

    _install_report(model_db, "balance_drift")

    row = model_db.execute(
        "SELECT currency_code, drift FROM reports.balance_drift"
    ).fetchone()
    assert row == ("EUR", Decimal("5.00"))
