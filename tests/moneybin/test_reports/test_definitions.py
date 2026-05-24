"""Tests for the in-tree report runners (ported from the old ReportsService).

Runners build SQL; these execute that SQL against stub ``reports.*`` views to
verify the dynamic shapes (cash_flow ``by``, balance_drift account resolution)
and the enum-allowlist ValueError branches the surfaces rely on.
"""

from __future__ import annotations

from typing import Any

import pytest

from moneybin.database import Database
from moneybin.reports._framework.contract import Runner
from moneybin.reports.definitions.balance_drift import balance_drift
from moneybin.reports.definitions.cash_flow import cash_flow
from moneybin.reports.definitions.large_transactions import large_transactions
from moneybin.reports.definitions.merchant_activity import merchant_activity
from moneybin.reports.definitions.recurring_subscriptions import recurring_subscriptions
from moneybin.reports.definitions.spending_trend import spending_trend

pytestmark = pytest.mark.unit


def _rows(db: Database, runner: Runner, **params: Any) -> list[dict[str, Any]]:
    rq = runner(db, **params)
    cur = db.execute(rq.sql, list(rq.params))
    cols = [d[0] for d in cur.description] if cur.description else []
    return [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]


def _install_cash_flow_view(db: Database) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute("""
        CREATE OR REPLACE VIEW reports.cash_flow AS
        SELECT * FROM (VALUES
            ('2026-01', 'A1', 'Alpha', 'Food', 100.0, -30.0, 70.0, 5),
            ('2026-01', 'A1', 'Alpha', 'Travel', 0.0, -50.0, -50.0, 2),
            ('2026-01', 'A2', 'Alpha', 'Food', 50.0, -10.0, 40.0, 3)
        ) AS t(year_month, account_id, account_name, category, inflow, outflow, net, txn_count)
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
            ('A1', 'Alpha', DATE '2026-04-01', 1000.00, 1010.00, 10.00, 10.00,
             1.0, 5, 'drift'),
            ('A2', 'Beta', DATE '2026-04-01', 500.00, 500.00, 0.00, 0.00,
             0.0, 5, 'clean')
        ) AS t(account_id, account_name, assertion_date, asserted_balance,
               computed_balance, drift, drift_abs, drift_pct,
               days_since_assertion, status)
    """)


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
            ('m1', 'Netflix', -15.99, 'monthly', 30.0, 1.5, 12,
             DATE '2025-01-01', DATE '2025-12-01', 'active', -191.88, 0.95)
        ) AS t(merchant_id, merchant_normalized, avg_amount, cadence,
               interval_days_avg, interval_days_stddev, occurrence_count,
               first_seen, last_seen, status, annualized_cost, confidence)
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
