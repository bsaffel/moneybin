# tests/moneybin/test_services/test_spending_service.py
"""Tests for SpendingService."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.services.spending_service import (
    CategoryBreakdown,
    MonthlySpending,
    SpendingService,
    SpendingSummary,
)
from tests.moneybin.db_helpers import create_core_tables_raw


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

    db_module._database_instance = database  # type: ignore[attr-defined]
    yield database
    db_module._database_instance = None  # type: ignore[attr-defined]
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
    def test_to_envelope_structure(self, spending_db: Database) -> None:
        service = SpendingService(spending_db)
        result = service.by_category(months=3)
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"
