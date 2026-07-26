"""Tests for the in-tree report runners (ported from the old ReportsService).

Runners build SQL; these execute that SQL against stub ``reports.*`` views to
verify the dynamic shapes (cash_flow ``by``, balance_drift account resolution)
and the enum-allowlist ValueError branches the surfaces rely on.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ReportSemantics, Runner
from moneybin.reports._framework.registry import spec_of
from moneybin.reports.definitions import ALL_REPORTS
from moneybin.reports.definitions._shared import CASHFLOW_GROUPINGS
from moneybin.reports.definitions.balance_drift import balance_drift
from moneybin.reports.definitions.cash_flow import cash_flow
from moneybin.reports.definitions.large_transactions import large_transactions
from moneybin.reports.definitions.merchant_activity import merchant_activity
from moneybin.reports.definitions.recurring_subscriptions import recurring_subscriptions
from moneybin.reports.definitions.spending_trend import spending_trend

pytestmark = pytest.mark.unit

_CORE_REPORT_IDS = {
    "spending": "core:spending",
    "cashflow": "core:cashflow",
    "recurring": "core:recurring",
    "merchants": "core:merchants",
    "large_transactions": "core:large_transactions",
    "balance_drift": "core:balance_drift",
}
_FLOW_REPORTS = frozenset(_CORE_REPORT_IDS) - {"balance_drift"}
_NO_FX_V1 = (
    "no FX conversion in v1; rows are segmented per currency_code, never blended"
)
_EXPECTED_DESCRIPTIONS = {
    "spending": (
        "Spending amounts are positive absolute outflows; comparison deltas "
        "are current spend minus comparison-period spend."
    ),
    "recurring": (
        "Average and annualized costs are positive absolute outflows in each "
        "row's own currency_code."
    ),
    "merchants": (
        "total_spend is positive absolute outflow; total_outflow is negative; "
        "total_inflow is positive; avg_amount and median_amount are signed."
    ),
    "balance_drift": (
        "Drift is asserted balance minus the independent transaction-derived "
        "position for assertion_date."
    ),
}


def _rows(db: Database, runner: Runner, **params: Any) -> list[dict[str, Any]]:
    rq = runner(db, **params)
    cur = db.execute(rq.sql, list(rq.params))
    cols = [d[0] for d in cur.description] if cur.description else []
    return [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]


def test_core_report_definitions_have_complete_financial_semantics() -> None:
    specs = {spec.name: spec for spec in map(spec_of, ALL_REPORTS)}

    assert {name: spec.report_id for name, spec in specs.items()} == _CORE_REPORT_IDS
    for name, spec in specs.items():
        assert spec.columns
        assert all(column.description for column in spec.columns)
        assert {column.name: column.data_class for column in spec.columns} == dict(
            spec.classes
        )

        semantics = spec.semantics
        assert isinstance(semantics, ReportSemantics)
        assert semantics.unit
        assert semantics.sign
        assert semantics.valuation_basis
        assert semantics.fx_basis
        assert semantics.time_basis
        assert semantics.exclusions
        assert semantics.provenance == (spec.view.full_name,)

        monetary_classes = {DataClass.TXN_AMOUNT, DataClass.BALANCE}
        assert monetary_classes.intersection(spec.classes.values())
        assert semantics.unit == "currency"
        # Rows are segmented, so the per-row column is the authority for
        # which currency an amount is in — not one envelope-level field.
        assert semantics.currency == "currency_code"
        assert semantics.fx_basis == _NO_FX_V1

        if name in _FLOW_REPORTS:
            assert semantics.kind == "flow"
            assert "inclusive" in semantics.time_basis
        else:
            assert semantics.kind == "position"
            assert semantics.comparison_window
            assert semantics.denominator
            assert semantics.time_basis == (
                "asserted and transaction-derived positions compared as of "
                "assertion_date; freshness measured from assertion_date through "
                "current date"
            )

    assert specs["spending"].semantics.denominator == (
        "previous-month spend for mom_pct; prior-year spend for yoy_pct; "
        "available calendar months up to three for trailing_3mo_avg, including "
        "zero-spend months"
    )
    assert specs["spending"].semantics.time_basis == (
        "inclusive eligible-data calendar-month period with zero-filled missing "
        "category-months"
    )
    assert specs["spending"].semantics.comparison_window == (
        "previous calendar month, same calendar month one year earlier, and "
        "trailing three calendar months including current month"
    )
    assert specs["merchants"].semantics.denominator == "txn_count for avg_amount"
    assert specs["large_transactions"].semantics.exclusions == (
        "transfers",
        "archived accounts",
        "account z-scores for zero median absolute deviation",
        "category z-scores for fewer than five transactions or zero median "
        "absolute deviation",
    )
    assert specs["balance_drift"].semantics.valuation_basis == (
        "transaction-derived position reconstructed from daily balance minus "
        "reconciliation_delta"
    )
    assert specs["balance_drift"].semantics.comparison_window == (
        "asserted position versus independent transaction-derived position on "
        "assertion_date"
    )

    for name, expected in _EXPECTED_DESCRIPTIONS.items():
        assert expected in specs[name].description
        assert "negative = expense, positive = income" not in specs[name].description


def _install_cash_flow_view(db: Database) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute("""
        CREATE OR REPLACE VIEW reports.cash_flow AS
        SELECT * FROM (VALUES
            ('2026-01', 'A1', 'Alpha', 'Food', 'USD', 100.0, -30.0, 70.0, 5),
            ('2026-01', 'A1', 'Alpha', 'Travel', 'USD', 0.0, -50.0, -50.0, 2),
            ('2026-01', 'A2', 'Alpha', 'Food', 'USD', 50.0, -10.0, 40.0, 3),
            -- A1/Food again in EUR: the only row that can tell a runner
            -- grouping by currency_code from one that drops it. Every `by`
            -- branch collapses this into the USD row above if the column
            -- leaves the GROUP BY, and the account/category assertions below
            -- cannot see that happen.
            ('2026-01', 'A1', 'Alpha', 'Food', 'EUR', 20.0, -8.0, 12.0, 1)
        ) AS t(year_month, account_id, account_name, category, currency_code,
               inflow, outflow, net, txn_count)
    """)


def test_cashflow_by_account_keeps_accounts_distinct(db: Database) -> None:
    _install_cash_flow_view(db)
    rows = _rows(db, cash_flow, by="account", from_month="2026-01", to_month="2026-12")
    assert {r["account_id"] for r in rows} == {"A1", "A2"}
    assert "category" not in rows[0]  # not grouped → column omitted entirely


def test_cashflow_by_category_groups_by_category(db: Database) -> None:
    _install_cash_flow_view(db)
    rows = _rows(db, cash_flow, by="category", from_month="2026-01", to_month="2026-12")
    assert {r["category"] for r in rows} == {"Food", "Travel"}
    assert "account_id" not in rows[0]  # not grouped → column omitted entirely


def test_cashflow_default_groups_by_account_and_category(db: Database) -> None:
    # The default by="account-and-category" fires BOTH grouping branches; the
    # GROUP BY must carry account_id AND category, keeping (account, category)
    # pairs distinct rather than collapsing them.
    _install_cash_flow_view(db)
    rows = _rows(db, cash_flow, from_month="2026-01", to_month="2026-12")
    assert "account_id" in rows[0] and "category" in rows[0]
    assert {(r["account_id"], r["category"]) for r in rows} == {
        ("A1", "Food"),
        ("A1", "Travel"),
        ("A2", "Food"),
    }


@pytest.mark.parametrize("by", sorted(CASHFLOW_GROUPINGS))
def test_cashflow_never_blends_currencies_whichever_by_is_chosen(
    db: Database, by: str
) -> None:
    """`currency_code` is in the GROUP BY for every `by` value, not just the default.

    The runner assembles `group_cols` per branch, so each one is a separate
    chance to drop the column — and dropping it re-blends the very rows the
    view segmented. Parametrized across all four branches because a fix applied
    to one is not a fix applied to the others. Parametrized over
    CASHFLOW_GROUPINGS itself so a new grouping is covered on the commit that
    adds it, rather than whenever someone remembers this list exists.
    """
    _install_cash_flow_view(db)

    rows = _rows(db, cash_flow, by=by, from_month="2026-01", to_month="2026-12")

    assert {r["currency_code"] for r in rows} == {"USD", "EUR"}
    eur = [r for r in rows if r["currency_code"] == "EUR"]
    assert len(eur) == 1
    # 12.0 is the EUR row alone; 82.0 would be it summed into A1/Food's USD net.
    assert eur[0]["net"] == 12.0


def test_cashflow_defaults_to_12_month_window() -> None:
    rq = cash_flow(None)  # type: ignore[arg-type]  # runner builds pure SQL, ignores db
    assert rq.period is not None
    assert any("last 12 months" in a for a in rq.actions)
    assert len(rq.params) == 2  # from + to bounds applied


@pytest.mark.parametrize(
    ("runner", "kwargs"),
    [
        (cash_flow, {"by": "bogus"}),
        (spending_trend, {"compare": "bogus"}),
        (recurring_subscriptions, {"status": "bogus"}),
        (recurring_subscriptions, {"cadence": "hourly"}),
        (merchant_activity, {"sort": "bogus"}),
        (large_transactions, {"anomaly": "bogus"}),
        (balance_drift, {"status": "bogus"}),
    ],
)
def test_runner_rejects_bad_enum(runner: Runner, kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="Unknown"):
        runner(None, **kwargs)  # type: ignore[arg-type]  # validation precedes db use


@pytest.mark.parametrize(
    ("runner", "kwargs", "match"),
    [
        # numeric range guards — out-of-range silently returned empty/odd results
        (recurring_subscriptions, {"min_confidence": 1.5}, "min_confidence"),
        (recurring_subscriptions, {"min_confidence": -0.1}, "min_confidence"),
        (large_transactions, {"top": 0}, "top"),
        (merchant_activity, {"top": -5}, "top"),
        # month/date format guards — malformed strings silently mis-windowed
        (cash_flow, {"from_month": "2024-1"}, "YYYY-MM"),
        (spending_trend, {"to_month": "2024/12"}, "YYYY-MM"),
        (balance_drift, {"since": "2024/01/01"}, "since"),
    ],
)
def test_runner_rejects_bad_input(
    runner: Runner, kwargs: dict[str, Any], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        runner(None, **kwargs)  # type: ignore[arg-type]  # validation precedes db use


def _install_balance_drift(db: Database) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute("""
        CREATE OR REPLACE VIEW core.dim_accounts AS
        SELECT * FROM (VALUES
            ('A1', 'Alpha', false),
            ('A2', 'Beta', false),
            ('A3', 'Joint', false),
            ('A4', 'Joint', false)
        ) AS t(account_id, display_name, archived)
    """)
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute("""
        CREATE OR REPLACE VIEW reports.balance_drift AS
        SELECT * FROM (VALUES
            ('A1', 'Alpha', 'USD', DATE '2026-04-01', 1000.00, 1010.00, 10.00,
             10.00, 1.0, 5, 'drift'),
            ('A2', 'Beta', 'USD', DATE '2026-04-01', 500.00, 500.00, 0.00, 0.00,
             0.0, 5, 'clean')
        ) AS t(account_id, account_name, currency_code, assertion_date,
               asserted_balance, computed_balance, drift, drift_abs, drift_pct,
               days_since_assertion, status)
    """)


def _install_mixed_currency_drift(db: Database) -> None:
    """A JPY account whose nominal drift dwarfs a USD account's.

    ¥120,000 of drift is roughly $800 — the same order of magnitude — but
    ranked on raw `drift_abs` the JPY rows occupy every leading slot.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute("""
        CREATE OR REPLACE VIEW reports.balance_drift AS
        SELECT * FROM (VALUES
            ('J1', 'Tokyo', 'JPY', DATE '2026-04-01', 900000.00, 1020000.00,
             120000.00, 120000.00, 13.3, 5, 'drift'),
            ('J2', 'Osaka', 'JPY', DATE '2026-04-01', 800000.00, 910000.00,
             110000.00, 110000.00, 13.8, 5, 'drift'),
            ('J3', 'Kyoto', 'JPY', DATE '2026-04-01', 700000.00, 800000.00,
             100000.00, 100000.00, 14.3, 5, 'drift'),
            ('U1', 'Checking', 'USD', DATE '2026-04-01', 1000.00, 1800.00,
             800.00, 800.00, 80.0, 5, 'drift'),
            ('U2', 'Savings', 'USD', DATE '2026-04-01', 500.00, 1200.00,
             700.00, 700.00, 140.0, 5, 'drift')
        ) AS t(account_id, account_name, currency_code, assertion_date,
               asserted_balance, computed_balance, drift, drift_abs, drift_pct,
               days_since_assertion, status)
    """)


def test_balance_drift_ranks_within_currency_not_across(db: Database) -> None:
    """The row cap must not be able to starve a currency.

    `execute.py` truncates with `records[:max_rows]`, so a global
    `ORDER BY drift_abs DESC` lets one high-denomination currency fill the cap
    and drop every other currency out of the response entirely — not merely
    mis-ranked, absent, with nothing in the payload saying so. This is the same
    reason `top` became per-currency (`multi-currency.md` Requirement 5).

    Asserted on the leading three rows because three is where the starvation
    shows: JPY has exactly three rows, so a global ordering returns JPY, JPY,
    JPY and no USD at all.
    """
    _install_mixed_currency_drift(db)

    leading = [r["currency_code"] for r in _rows(db, balance_drift)[:3]]

    assert set(leading) == {"JPY", "USD"}, (
        f"first three rows are {leading}; a cap here would hide a whole currency"
    )


def test_balance_drift_resolves_account_id(db: Database) -> None:
    _install_balance_drift(db)
    rows = _rows(db, balance_drift, account="A1")
    assert {r["account_id"] for r in rows} == {"A1"}


def test_balance_drift_resolves_display_name(db: Database) -> None:
    _install_balance_drift(db)
    rows = _rows(db, balance_drift, account="Beta")
    assert {r["account_id"] for r in rows} == {"A2"}


def test_balance_drift_ambiguous_account_raises(db: Database) -> None:
    from moneybin.services.account_service import AmbiguousAccountError

    _install_balance_drift(db)
    with pytest.raises(AmbiguousAccountError):
        balance_drift(db, account="Joint")


def _install_recurring(db: Database) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute("""
        CREATE OR REPLACE VIEW reports.recurring_subscriptions AS
        SELECT * FROM (VALUES
            ('m1', 'Netflix', 'USD', -15.99, 'monthly', 30.0, 1.5, 12,
             DATE '2025-01-01', DATE '2025-12-01', 'active', -191.88, 0.95)
        ) AS t(merchant_id, merchant_normalized, currency_code, avg_amount,
               cadence, interval_days_avg, interval_days_stddev,
               occurrence_count, first_seen, last_seen, status,
               annualized_cost, confidence)
    """)


def test_recurring_projects_all_declared_interval_columns(db: Database) -> None:
    # The runner SELECT must project every column it declares in `classes`,
    # including interval_days_avg/stddev — otherwise reports_recurring silently
    # drops two metrics it claims to return (the masking declares them, but the
    # query never selected them).
    _install_recurring(db)
    rows = _rows(db, recurring_subscriptions)
    assert "interval_days_avg" in rows[0]
    assert "interval_days_stddev" in rows[0]


# --------------------------------------------------------------------------
# Runner-level ranking is per currency (multi-currency.md Requirements 5 and 7).
#
# Segmenting the model is not enough: a runner that ranks the segmented rows
# with one cross-currency ORDER BY ... LIMIT compares unlike units, and at the
# default `top` it can drop an entire currency out of the result. Each fixture
# below pairs one small-denomination row against enough large-denomination
# rows to fill the limit on its own.
# --------------------------------------------------------------------------


def _install_large_transactions_view(db: Database, *, jpy_rows: int) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute(
        """
        CREATE OR REPLACE VIEW reports.large_transactions AS
        SELECT 'usd1' AS transaction_id, 'A1' AS account_id, 'Alpha' AS account_name,
               DATE '2026-01-01' AS txn_date, -40.00 AS amount, 'small' AS description,
               'm1' AS merchant_id, 'Corner Store' AS merchant_normalized,
               'Food' AS category, 'USD' AS currency_code,
               3.0 AS amount_zscore_account, 3.0 AS amount_zscore_category,
               TRUE AS is_top_100
        UNION ALL
        SELECT 'jpy' || i, 'A2', 'Beta', DATE '2026-01-01', -(8000.0 + i), 'big',
               'm2', 'Depato', 'Food', 'JPY', 3.0, 3.0, TRUE
        FROM GENERATE_SERIES(1, ?) AS t(i)
    """.replace("?", str(jpy_rows))
    )  # noqa: S608  # test-controlled row count


def test_large_transactions_keeps_each_currency_inside_the_top_n(
    db: Database,
) -> None:
    """A ¥8,001 charge must not outrank a $40 one out of the result."""
    _install_large_transactions_view(db, jpy_rows=30)

    rows = _rows(db, large_transactions, top=25)

    assert "usd1" in {row["transaction_id"] for row in rows}
    per_currency = Counter(str(row["currency_code"]) for row in rows)
    assert per_currency == Counter({"JPY": 25, "USD": 1})


def _install_merchant_activity_view(db: Database, *, jpy_rows: int) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute(
        """
        CREATE OR REPLACE VIEW reports.merchant_activity AS
        SELECT 'm_usd' AS merchant_id, 'Corner Store' AS merchant_normalized,
               'USD' AS currency_code, 40.00 AS total_spend, 0.00 AS total_inflow,
               -40.00 AS total_outflow, 2 AS txn_count, -20.00 AS avg_amount,
               -20.00 AS median_amount, DATE '2026-01-01' AS first_seen,
               DATE '2026-01-02' AS last_seen, 1 AS active_months,
               'Food' AS top_category, 1 AS account_count
        UNION ALL
        SELECT 'm_jpy' || i, 'Depato' || i, 'JPY', 8000.0 + i, 0.00, -(8000.0 + i),
               2, -4000.0, -4000.0, DATE '2026-01-01', DATE '2026-01-02', 1,
               'Food', 1
        FROM GENERATE_SERIES(1, ?) AS t(i)
    """.replace("?", str(jpy_rows))
    )  # noqa: S608  # test-controlled row count


def test_merchants_keeps_each_currency_inside_the_top_n(db: Database) -> None:
    """A ¥-billed merchant list must not evict every $-billed merchant."""
    _install_merchant_activity_view(db, jpy_rows=30)

    rows = _rows(db, merchant_activity, top=25, sort="spend")

    assert "m_usd" in {row["merchant_id"] for row in rows}
    assert sum(1 for row in rows if row["currency_code"] == "JPY") == 25


def test_recurring_groups_candidates_by_the_currency_they_are_billed_in(
    db: Database,
) -> None:
    """Ordering by cost alone would interleave ¥ and $ candidates."""
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute("""
        CREATE OR REPLACE VIEW reports.recurring_subscriptions AS
        SELECT * FROM (VALUES
            ('m1', 'Netflix', 'USD', 15.99, 'monthly', 30.0, 1.5, 12,
             DATE '2025-01-01', DATE '2025-12-01', 'active', 191.88, 0.95),
            ('m2', 'Depato', 'JPY', 1200.0, 'monthly', 30.0, 1.5, 12,
             DATE '2025-01-01', DATE '2025-12-01', 'active', 14400.0, 0.95),
            ('m3', 'Spotify', 'USD', 11.99, 'monthly', 30.0, 1.5, 12,
             DATE '2025-01-01', DATE '2025-12-01', 'active', 143.88, 0.95)
        ) AS t(merchant_id, merchant_normalized, currency_code, avg_amount,
               cadence, interval_days_avg, interval_days_stddev,
               occurrence_count, first_seen, last_seen, status,
               annualized_cost, confidence)
    """)

    rows = _rows(db, recurring_subscriptions, min_confidence=0.5, status="active")

    # Grouped by currency, cost-ordered within each — not one ¥14,400 row
    # sitting above both dollar subscriptions on nominal magnitude.
    assert [row["merchant_id"] for row in rows] == ["m2", "m1", "m3"]
