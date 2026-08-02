# tests/moneybin/test_services/test_spending_service.py
"""Tests for SpendingService."""

import pytest

from moneybin.database import Database
from moneybin.services.spending_service import (
    CategoryBreakdown,
    MonthlySpending,
    SpendingService,
    SpendingSummary,
)
from tests.moneybin.db_helpers import create_core_tables_raw


@pytest.fixture()
def empty_db(db: Database) -> Database:
    """Return a Database with tables created but no data."""
    create_core_tables_raw(db.conn)
    return db


@pytest.fixture()
def spending_db(db: Database) -> Database:
    """Return a Database with core + app tables and test transactions seeded."""
    conn = db.conn
    create_core_tables_raw(conn)

    # Two months of transactions, anchored to CURRENT_DATE rather than to literal
    # dates. by_category() filters on
    # `transaction_year_month >= strftime(CURRENT_DATE - INTERVAL '3 months')`, so a
    # hardcoded fixture silently ages out of its own window and the assertions below
    # start reporting an empty breakdown — a failure with no bug behind it, arriving
    # on a calendar boundary rather than on a commit. The previous fixture sat in
    # 2026-03/2026-04 and expired at the 2026-08-01 UTC month rollover.
    #
    # The anchors are the two PRECEDING months, never the current one: that keeps
    # every row in the past (a fixture dated into the future is its own puzzle) and
    # still leaves a full month of margin before the three-month floor. Every derived
    # part column is computed from the same date so they cannot drift apart.
    conn.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month, transaction_year_quarter
        )
        SELECT
            t.txn_id, 'A1', t.txn_date, t.amount,
            ABS(t.amount), t.direction, t.description,
            t.txn_type, false, 'USD', 'ofx',
            t.txn_date, CURRENT_TIMESTAMP,
            YEAR(t.txn_date), MONTH(t.txn_date), DAY(t.txn_date),
            DAYOFWEEK(t.txn_date), STRFTIME(t.txn_date, '%Y-%m'),
            STRFTIME(t.txn_date, '%Y') || '-Q' || CAST(QUARTER(t.txn_date) AS VARCHAR)
        FROM (VALUES
            ('T1', (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 1 MONTH + INTERVAL 9 DAY)::DATE, -50.00, 'expense', 'Coffee', 'DEBIT'),
            ('T2', (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 1 MONTH + INTERVAL 14 DAY)::DATE, 5000.00, 'income', 'Payroll', 'CREDIT'),
            ('T3', (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 2 MONTH + INTERVAL 9 DAY)::DATE, -200.00, 'expense', 'Groceries', 'DEBIT'),
            ('T4', (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL 2 MONTH + INTERVAL 19 DAY)::DATE, 5000.00, 'income', 'Payroll', 'CREDIT')
        ) AS t(txn_id, txn_date, amount, direction, description, txn_type)
    """)  # noqa: S608  # test input, not executing SQL

    # Insert transaction_categories for by_category tests
    conn.execute("""
        INSERT INTO app.transaction_categories
            (transaction_id, category, subcategory, categorized_at, categorized_by)
        VALUES
        ('T1', 'Food & Drink', 'Coffee Shops', CURRENT_TIMESTAMP, 'user'),
        ('T3', 'Food & Drink', 'Groceries', CURRENT_TIMESTAMP, 'user')
    """)  # noqa: S608  # test input, not executing SQL

    return db


class TestSpendingSummary:
    """Tests for SpendingService.summary()."""

    @pytest.mark.unit
    def test_returns_monthly_data(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.summary(months=3)
        assert isinstance(result, SpendingSummary)
        assert len(result.months) >= 2

    @pytest.mark.unit
    def test_monthly_spending_fields(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.summary(months=3)
        month = result.months[0]
        assert isinstance(month, MonthlySpending)
        assert hasattr(month, "period")
        assert hasattr(month, "income")
        assert hasattr(month, "expenses")
        assert hasattr(month, "net")
        assert hasattr(month, "transaction_count")

    @pytest.mark.unit
    def test_to_envelope_structure(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.summary(months=3)
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"
        assert isinstance(d["data"], list)
        assert len(d["actions"]) > 0


class TestSpendingByCategory:
    """Tests for SpendingService.by_category()."""

    @pytest.mark.unit
    def test_returns_category_breakdown(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.by_category(months=3)
        assert isinstance(result, CategoryBreakdown)
        assert len(result.categories) > 0

    @pytest.mark.unit
    def test_period_label_set(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.by_category(months=3)
        assert result.period_label != ""

    @pytest.mark.unit
    def test_to_envelope_structure(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.by_category(months=3)
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"


class TestMonthsValidation:
    """Tests for months parameter validation."""

    @pytest.mark.unit
    def test_summary_rejects_zero_months(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        with pytest.raises(ValueError, match="months must be between 1 and 120"):
            service.summary(months=0)

    @pytest.mark.unit
    def test_summary_rejects_negative_months(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        with pytest.raises(ValueError, match="months must be between 1 and 120"):
            service.summary(months=-1)

    @pytest.mark.unit
    def test_summary_rejects_over_120_months(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        with pytest.raises(ValueError, match="months must be between 1 and 120"):
            service.summary(months=121)

    @pytest.mark.unit
    def test_summary_allows_valid_months(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.summary(months=1)
        assert isinstance(result, SpendingSummary)

    @pytest.mark.unit
    def test_by_category_rejects_zero_months(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        with pytest.raises(ValueError, match="months must be between 1 and 120"):
            service.by_category(months=0)

    @pytest.mark.unit
    def test_summary_skips_validation_with_start_date(
        self, spending_db: Database
    ) -> None:
        """Months validation is skipped when start_date is provided."""
        service = SpendingService(spending_db)
        result = service.summary(months=0, start_date="2026-01-01")
        assert isinstance(result, SpendingSummary)


class TestEmptyResults:
    """Tests for service behavior with no data in tables."""

    @pytest.mark.unit
    def test_summary_empty_db(self, empty_db: Database) -> None:
        service = SpendingService(empty_db)
        result = service.summary(months=3)
        assert isinstance(result, SpendingSummary)
        assert result.months == []

    @pytest.mark.unit
    def test_by_category_empty_db(self, empty_db: Database) -> None:
        service = SpendingService(empty_db)
        result = service.by_category(months=3)
        assert isinstance(result, CategoryBreakdown)
        assert result.categories == []
