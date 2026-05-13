# tests/moneybin/test_services/test_budget_service.py
"""Tests for BudgetService."""

from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.budget_service import (
    BudgetCategoryStatus,
    BudgetService,
    BudgetSetResult,
    BudgetStatusResult,
)
from tests.moneybin.db_helpers import create_core_tables_raw


@pytest.fixture()
def budget_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Yield a Database with core + app tables and test data seeded."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    conn = database.conn
    create_core_tables_raw(conn)

    # Insert transactions for budget status tests
    conn.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month,
            transaction_year_quarter
        ) VALUES
        ('T1', 'A1', '2026-04-10', -50.00, 50.00, 'expense',
         'Coffee Shop', 'DEBIT', false, 'USD', 'ofx',
         '2026-04-10', CURRENT_TIMESTAMP,
         2026, 4, 10, 3, '2026-04', '2026-Q2'),
        ('T2', 'A1', '2026-04-12', -30.00, 30.00, 'expense',
         'Bakery', 'DEBIT', false, 'USD', 'ofx',
         '2026-04-12', CURRENT_TIMESTAMP,
         2026, 4, 12, 5, '2026-04', '2026-Q2')
    """)  # noqa: S608  # test input, not executing SQL

    # Categorize transactions
    conn.execute("""
        INSERT INTO app.transaction_categories
            (transaction_id, category, subcategory, categorized_at,
             categorized_by)
        VALUES
        ('T1', 'Food & Drink', 'Coffee Shops', CURRENT_TIMESTAMP, 'user'),
        ('T2', 'Food & Drink', 'Bakeries', CURRENT_TIMESTAMP, 'user')
    """)  # noqa: S608  # test input, not executing SQL

    yield database
    database.close()


class TestSetBudget:
    """Tests for BudgetService.set_budget()."""

    @pytest.mark.unit
    def test_creates_new_budget(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        result = service.set_budget(
            "Food & Drink", Decimal("200.00"), start_month="2026-04"
        )
        assert isinstance(result, BudgetSetResult)
        assert result.category == "Food & Drink"
        assert result.monthly_amount == Decimal("200.00")
        assert result.action == "created"

    @pytest.mark.unit
    def test_updates_existing_budget(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        # Create first
        service.set_budget("Food & Drink", Decimal("200.00"), start_month="2026-04")
        # Update
        result = service.set_budget(
            "Food & Drink", Decimal("300.00"), start_month="2026-04"
        )
        assert result.action == "updated"
        assert result.monthly_amount == Decimal("300.00")

    @pytest.mark.unit
    def test_to_envelope_sensitivity_low(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        result = service.set_budget(
            "Food & Drink", Decimal("200.00"), start_month="2026-04"
        )
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"
        assert d["data"]["category"] == "Food & Drink"
        assert d["data"]["action"] == "created"


class TestBudgetStatus:
    """Tests for BudgetService.status()."""

    @pytest.mark.unit
    def test_returns_status_result(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        # Set a budget first
        service.set_budget("Food & Drink", Decimal("200.00"), start_month="2026-04")
        result = service.status(month="2026-04")
        assert isinstance(result, BudgetStatusResult)
        assert result.month == "2026-04"
        assert len(result.categories) == 1

    @pytest.mark.unit
    def test_status_fields(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        service.set_budget("Food & Drink", Decimal("200.00"), start_month="2026-04")
        result = service.status(month="2026-04")
        cat = result.categories[0]
        assert isinstance(cat, BudgetCategoryStatus)
        assert cat.category == "Food & Drink"
        assert cat.budget == Decimal("200.00")
        # T1 (-50) + T2 (-30) = 80 spent
        assert cat.spent == Decimal("80.00")
        assert cat.remaining == Decimal("120.00")
        assert cat.status == "OK"

    @pytest.mark.unit
    def test_status_over_budget(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        # Set budget lower than spending (80.00)
        service.set_budget("Food & Drink", Decimal("50.00"), start_month="2026-04")
        result = service.status(month="2026-04")
        cat = result.categories[0]
        assert cat.status == "OVER"
        assert cat.remaining < 0

    @pytest.mark.unit
    def test_status_warning_threshold(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        # Budget of 90 with 80 spent = 88.9% => WARNING
        service.set_budget("Food & Drink", Decimal("90.00"), start_month="2026-04")
        result = service.status(month="2026-04")
        cat = result.categories[0]
        assert cat.status == "WARNING"

    @pytest.mark.unit
    def test_to_envelope_structure(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        service.set_budget("Food & Drink", Decimal("200.00"), start_month="2026-04")
        result = service.status(month="2026-04")
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"
        assert d["summary"]["period"] == "2026-04"
        assert isinstance(d["data"], list)
        assert len(d["actions"]) > 0


class TestEmptyResults:
    """Tests for service behavior with no budgets set."""

    @pytest.mark.unit
    def test_status_no_budgets(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        result = service.status(month="2026-04")
        assert isinstance(result, BudgetStatusResult)
        assert result.categories == []
