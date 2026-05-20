"""Tests for ReportsService.

Covers the dynamic SQL builder shapes (especially the ``cash_flow`` ``by``
parameter) and the enum-allowlist ``ValueError`` branches that the MCP
tools rely on for input validation. Each test stubs the relevant
``reports.*`` view with a known shape so the SQL composition is exercised
end-to-end against DuckDB without needing a full SQLMesh transform.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.services.reports_service import (
    CASHFLOW_GROUPINGS,
    DRIFT_STATUSES,
    LARGE_TXN_ANOMALIES,
    MERCHANTS_SORTS,
    RECURRING_CADENCES,
    RECURRING_STATUSES,
    SPENDING_COMPARES,
    ReportsService,
)

pytestmark = pytest.mark.unit


def _install_cash_flow_view(db: Database) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    # year_month is 'YYYY-MM' VARCHAR per reports.cash_flow's SQLMesh model.
    db.execute("""
        CREATE OR REPLACE VIEW reports.cash_flow AS
        SELECT * FROM (VALUES
            ('2026-01', 'A1', 'Alpha', 'Food', 100.0, -30.0, 70.0, 5),
            ('2026-01', 'A1', 'Alpha', 'Travel', 0.0, -50.0, -50.0, 2),
            ('2026-01', 'A2', 'Alpha', 'Food', 50.0, -10.0, 40.0, 3),
            ('2026-02', 'A1', 'Alpha', 'Food', 200.0, -60.0, 140.0, 7)
        ) AS t(year_month, account_id, account_name, category, inflow, outflow, net, txn_count)
    """)


class TestCashFlow:
    """The ``cash_flow`` method's dynamic ``by`` parameter is the highest-risk surface."""

    def test_by_account_groups_by_account_id_and_name(self, db: Database) -> None:
        _install_cash_flow_view(db)
        payload = ReportsService(db).cash_flow(by="account")
        # account_id and account_name present; category is None
        assert all(r.account_id is not None for r in payload.rows)
        assert all(r.account_name is not None for r in payload.rows)
        assert all(r.category is None for r in payload.rows)
        # Two accounts with the same display_name "Alpha" must not collapse.
        account_ids = {r.account_id for r in payload.rows}
        assert account_ids == {"A1", "A2"}

    def test_by_category_groups_by_category_only(self, db: Database) -> None:
        _install_cash_flow_view(db)
        payload = ReportsService(db).cash_flow(by="category")
        assert all(r.category is not None for r in payload.rows)
        assert all(r.account_id is None for r in payload.rows)
        assert all(r.account_name is None for r in payload.rows)
        categories = {r.category for r in payload.rows}
        assert categories == {"Food", "Travel"}

    def test_by_account_and_category_groups_by_both(self, db: Database) -> None:
        _install_cash_flow_view(db)
        payload = ReportsService(db).cash_flow(by="account-and-category")
        assert all(r.account_id is not None for r in payload.rows)
        assert all(r.account_name is not None for r in payload.rows)
        assert all(r.category is not None for r in payload.rows)
        # Two A1/Food/2026-01 rows do not collapse with A2/Food/2026-01.
        assert len(payload.rows) == 4

    def test_invalid_by_raises(self, db: Database) -> None:
        with pytest.raises(ValueError, match="Unknown by"):
            ReportsService(db).cash_flow(by="by_account")

    def test_from_to_bounds_filter_rows(self, db: Database) -> None:
        _install_cash_flow_view(db)
        # from_month accepts 'YYYY-MM' or 'YYYY-MM-DD' — the day is stripped.
        payload = ReportsService(db).cash_flow(
            from_month="2026-02-01", by="account-and-category"
        )
        # Only February rows survive.
        year_months = {r.year_month for r in payload.rows}
        assert year_months == {"2026-02"}


class TestAllowlistRejection:
    """Each enum-validated method rejects unknown values with ValueError."""

    def test_spending_trend_invalid_compare(self, db: Database) -> None:
        with pytest.raises(ValueError, match="Unknown compare"):
            ReportsService(db).spending_trend(compare="bogus")

    def test_recurring_invalid_status(self, db: Database) -> None:
        with pytest.raises(ValueError, match="Unknown status"):
            ReportsService(db).recurring_subscriptions(status="bogus")

    def test_recurring_invalid_cadence(self, db: Database) -> None:
        with pytest.raises(ValueError, match="Unknown cadence"):
            ReportsService(db).recurring_subscriptions(cadence="hourly")

    def test_merchants_invalid_sort(self, db: Database) -> None:
        with pytest.raises(ValueError, match="Unknown sort"):
            ReportsService(db).merchant_activity(sort="bogus")

    def test_large_transactions_invalid_anomaly(self, db: Database) -> None:
        with pytest.raises(ValueError, match="Unknown anomaly"):
            ReportsService(db).large_transactions(anomaly="bogus")

    def test_balance_drift_invalid_status(self, db: Database) -> None:
        with pytest.raises(ValueError, match="Unknown status"):
            ReportsService(db).balance_drift(status="bogus")


class TestAllowlistConstants:
    """Module-level constants are the single source of truth for enum vocabularies."""

    def test_cashflow_groupings_includes_known_values(self) -> None:
        assert "account" in CASHFLOW_GROUPINGS
        assert "category" in CASHFLOW_GROUPINGS
        assert "account-and-category" in CASHFLOW_GROUPINGS

    def test_recurring_cadences_include_irregular(self) -> None:
        # Drift bug surfaced by /simplify: CLI rejected "irregular" while
        # MCP accepted it; both should accept after the refactor.
        assert "irregular" in RECURRING_CADENCES

    def test_merchants_sorts_map_to_sql(self) -> None:
        # Allowlist values are SQL fragments interpolated into ORDER BY.
        assert all(" DESC" in clause for clause in MERCHANTS_SORTS.values())

    def test_spending_compares(self) -> None:
        assert set(SPENDING_COMPARES) == {"yoy", "mom", "trailing"}

    def test_drift_statuses(self) -> None:
        assert "no-data" in DRIFT_STATUSES
        assert "all" in DRIFT_STATUSES

    def test_large_txn_anomalies(self) -> None:
        assert set(LARGE_TXN_ANOMALIES) == {"none", "account", "category"}

    def test_recurring_statuses(self) -> None:
        assert set(RECURRING_STATUSES) == {"active", "inactive", "all"}


# TestUncategorizedQueueTyping removed — ReportsService.uncategorized_queue was
# consolidated into CategorizationService.list_uncategorized_transactions (PR #9).
# Account-filter and sort behavior is covered by
# tests/moneybin/test_mcp/test_v1_tools.py::TestCategorizePendingGet.


def _install_dim_accounts(db: Database) -> None:
    """Minimal core.dim_accounts shape for the account filter tests.

    Includes the collision pair (Joint / Joint) so the resolver's
    ambiguity branch can be exercised, plus an archived account
    sharing a display_name with an active one so the archived-filter
    behavior can be verified separately.
    """
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute("""
        CREATE OR REPLACE VIEW core.dim_accounts AS
        SELECT * FROM (VALUES
            ('A1', 'Alpha', false),
            ('A2', 'Beta', false),
            ('A3', 'Joint', false),
            ('A4', 'Joint', false),
            ('A5_OLD', 'Chase Checking', true),
            ('A5_NEW', 'Chase Checking', false)
        ) AS t(account_id, display_name, archived)
    """)


def _install_balance_drift_with_accounts(db: Database) -> None:
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


class TestAccountFilterResolver:
    """``balance_drift`` resolves account via AccountService.

    The filter accepts ``account_id`` or ``display_name`` and binds the
    resolved id to the SQL ``WHERE`` clause. Ambiguous display_name
    matches raise instead of silently doubling.

    Note: uncategorized_queue account-filter tests removed — that
    functionality moved to CategorizationService.list_uncategorized_transactions;
    covered by tests/moneybin/test_mcp/test_v1_tools.py::TestCategorizePendingGet.
    """

    def test_balance_drift_filter_accepts_account_id(self, db: Database) -> None:
        _install_dim_accounts(db)
        _install_balance_drift_with_accounts(db)
        payload = ReportsService(db).balance_drift(account="A1")
        ids = {row.account_id for row in payload.rows}
        assert ids == {"A1"}

    def test_balance_drift_filter_accepts_display_name(self, db: Database) -> None:
        _install_dim_accounts(db)
        _install_balance_drift_with_accounts(db)
        payload = ReportsService(db).balance_drift(account="Beta")
        ids = {row.account_id for row in payload.rows}
        assert ids == {"A2"}

    def test_balance_drift_filter_ambiguous_account_errors(self, db: Database) -> None:
        from moneybin.services.account_service import AmbiguousAccountError

        _install_dim_accounts(db)
        _install_balance_drift_with_accounts(db)
        with pytest.raises(AmbiguousAccountError):
            ReportsService(db).balance_drift(account="Joint")
