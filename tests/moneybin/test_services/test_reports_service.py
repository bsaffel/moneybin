"""Tests for ReportsService.

Covers the dynamic SQL builder shapes (especially the ``cash_flow`` ``by``
parameter) and the enum-allowlist ``ValueError`` branches that the MCP
tools rely on for input validation. Each test stubs the relevant
``reports.*`` view with a known shape so the SQL composition is exercised
end-to-end against DuckDB without needing a full SQLMesh transform.
"""

from __future__ import annotations

from decimal import Decimal

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
        cols, rows = ReportsService(db).cash_flow(by="account")
        assert "account_id" in cols
        assert "account_name" in cols
        assert "category" not in cols
        # Two accounts with the same display_name "Alpha" must not collapse.
        account_ids = {row[cols.index("account_id")] for row in rows}
        assert account_ids == {"A1", "A2"}

    def test_by_category_groups_by_category_only(self, db: Database) -> None:
        _install_cash_flow_view(db)
        cols, rows = ReportsService(db).cash_flow(by="category")
        assert "category" in cols
        assert "account_id" not in cols
        assert "account_name" not in cols
        categories = {row[cols.index("category")] for row in rows}
        assert categories == {"Food", "Travel"}

    def test_by_account_and_category_groups_by_both(self, db: Database) -> None:
        _install_cash_flow_view(db)
        cols, rows = ReportsService(db).cash_flow(by="account-and-category")
        assert "account_id" in cols
        assert "account_name" in cols
        assert "category" in cols
        # Two A1/Food/2026-01 rows do not collapse with A2/Food/2026-01.
        assert len(rows) == 4

    def test_invalid_by_raises(self, db: Database) -> None:
        with pytest.raises(ValueError, match="Unknown by"):
            ReportsService(db).cash_flow(by="by_account")

    def test_from_to_bounds_filter_rows(self, db: Database) -> None:
        _install_cash_flow_view(db)
        # from_month accepts 'YYYY-MM' or 'YYYY-MM-DD' — the day is stripped.
        cols, rows = ReportsService(db).cash_flow(
            from_month="2026-02-01", by="account-and-category"
        )
        # Only February rows survive.
        year_months = {row[cols.index("year_month")] for row in rows}
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


class TestUncategorizedQueueTyping:
    """``min_amount`` accepts Decimal, float, and int (typing-locked union)."""

    def _install(self, db: Database) -> None:
        db.execute("CREATE SCHEMA IF NOT EXISTS reports")
        db.execute("""
            CREATE OR REPLACE VIEW reports.uncategorized_queue AS
            SELECT * FROM (VALUES
                ('T1', 'A1', 'Alpha', DATE '2026-04-01', -25.00, 'COFFEE', 'Coffee', 30, 750.0, 'ofx', NULL),
                ('T2', 'A1', 'Alpha', DATE '2026-04-10', -500.00, 'BIG', 'Big', 20, 10000.0, 'ofx', NULL)
            ) AS t(transaction_id, account_id, account_name, txn_date, amount,
                   description, merchant_normalized, age_days, priority_score,
                   source_type, source_id)
        """)

    def test_min_amount_decimal(self, db: Database) -> None:
        self._install(db)
        cols, rows = ReportsService(db).uncategorized_queue(min_amount=Decimal("100"))
        # Only T2 (|-500| >= 100) survives.
        assert len(rows) == 1
        assert rows[0][cols.index("transaction_id")] == "T2"

    def test_min_amount_float(self, db: Database) -> None:
        self._install(db)
        _, rows = ReportsService(db).uncategorized_queue(min_amount=100.0)
        assert len(rows) == 1

    def test_min_amount_int(self, db: Database) -> None:
        self._install(db)
        _, rows = ReportsService(db).uncategorized_queue(min_amount=100)
        assert len(rows) == 1

    def test_source_columns_in_projection(self, db: Database) -> None:
        # Regression: provenance columns must survive the service projection
        # so downstream tooling can trace rows back to their origin.
        self._install(db)
        cols, _ = ReportsService(db).uncategorized_queue()
        assert "source_type" in cols
        assert "source_id" in cols
