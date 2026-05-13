# tests/moneybin/test_services/test_spending_service.py
"""Tests for SpendingService."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

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
def empty_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Yield a Database with tables created but no data."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    create_core_tables_raw(database.conn)
    yield database
    database.close()


@pytest.fixture()
def spending_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Yield a Database with core + app tables and test transactions seeded."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    conn = database.conn
    create_core_tables_raw(conn)

    # Insert test transactions spanning 2 months
    conn.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month, transaction_year_quarter
        ) VALUES
        ('T1', 'A1', '2026-04-10', -50.00, 50.00, 'expense', 'Coffee', 'DEBIT', false, 'USD', 'ofx', '2026-04-10', CURRENT_TIMESTAMP, 2026, 4, 10, 3, '2026-04', '2026-Q2'),
        ('T2', 'A1', '2026-04-15', 5000.00, 5000.00, 'income', 'Payroll', 'CREDIT', false, 'USD', 'ofx', '2026-04-15', CURRENT_TIMESTAMP, 2026, 4, 15, 1, '2026-04', '2026-Q2'),
        ('T3', 'A1', '2026-03-10', -200.00, 200.00, 'expense', 'Groceries', 'DEBIT', false, 'USD', 'ofx', '2026-03-10', CURRENT_TIMESTAMP, 2026, 3, 10, 1, '2026-03', '2026-Q1'),
        ('T4', 'A1', '2026-03-20', 5000.00, 5000.00, 'income', 'Payroll', 'CREDIT', false, 'USD', 'ofx', '2026-03-20', CURRENT_TIMESTAMP, 2026, 3, 20, 4, '2026-03', '2026-Q1')
    """)  # noqa: S608  # test input, not executing SQL

    # Insert transaction_categories for by_category tests
    conn.execute("""
        INSERT INTO app.transaction_categories
            (transaction_id, category, subcategory, categorized_at, categorized_by)
        VALUES
        ('T1', 'Food & Drink', 'Coffee Shops', CURRENT_TIMESTAMP, 'user'),
        ('T3', 'Food & Drink', 'Groceries', CURRENT_TIMESTAMP, 'user')
    """)  # noqa: S608  # test input, not executing SQL

    yield database
    database.close()


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
