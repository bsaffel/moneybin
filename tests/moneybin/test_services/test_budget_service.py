# tests/moneybin/test_services/test_budget_service.py
"""Tests for BudgetService."""

from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.privacy.payloads.budget import (
    BudgetCategoryStatusRow,
    BudgetSetPayload,
    BudgetStatusPayload,
)
from moneybin.services.budget_service import (
    BudgetService,
    BudgetSetResult,
)
from moneybin.services.categorization import CategorizationService
from tests.moneybin.db_helpers import create_core_tables_raw, seed_categories_view


@pytest.fixture()
def budget_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Yield a Database with core + app tables and test data seeded."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
        read_only=False,
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
            "Food & Drink", Decimal("200.00"), start_month="2026-04", actor="cli"
        )
        assert isinstance(result, BudgetSetResult)
        assert result.category == "Food & Drink"
        assert result.monthly_amount == Decimal("200.00")
        assert result.action == "created"

    @pytest.mark.unit
    def test_updates_existing_budget(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        # Create first
        service.set_budget(
            "Food & Drink", Decimal("200.00"), start_month="2026-04", actor="cli"
        )
        # Update
        result = service.set_budget(
            "Food & Drink", Decimal("300.00"), start_month="2026-04", actor="cli"
        )
        assert result.action == "updated"
        assert result.monthly_amount == Decimal("300.00")

    @pytest.mark.unit
    def test_update_cross_month_overlap_reports_stored_start_month(
        self, budget_db: Database
    ) -> None:
        # A later request whose start_month differs from the stored window must
        # still match the overlap and report the STORED start_month, since
        # update() does not rewrite start_month.
        service = BudgetService(budget_db)
        service.set_budget(
            "Food & Drink", Decimal("200.00"), start_month="2026-01", actor="cli"
        )
        result = service.set_budget(
            "Food & Drink", Decimal("300.00"), start_month="2026-06", actor="cli"
        )
        assert result.action == "updated"
        assert result.start_month == "2026-01"  # stored window, not the request
        # And the DB row's start_month is unchanged.
        row = budget_db.execute(
            "SELECT start_month FROM app.budgets WHERE category = 'Food & Drink'"
        ).fetchone()
        assert row == ("2026-01",)

    @pytest.mark.unit
    def test_to_payload_shape(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        result = service.set_budget(
            "Food & Drink", Decimal("200.00"), start_month="2026-04", actor="cli"
        )
        payload = result.to_payload()
        assert isinstance(payload, BudgetSetPayload)
        assert payload.category == "Food & Drink"
        assert payload.monthly_amount == Decimal("200.00")
        assert payload.action == "created"
        assert payload.start_month == "2026-04"


class TestBudgetStatus:
    """Tests for BudgetService.status()."""

    @pytest.mark.unit
    def test_returns_status_result(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        # Set a budget first
        service.set_budget(
            "Food & Drink", Decimal("200.00"), start_month="2026-04", actor="cli"
        )
        result = service.status(month="2026-04")
        assert isinstance(result, BudgetStatusPayload)
        assert result.month == "2026-04"
        assert len(result.categories) == 1

    @pytest.mark.unit
    def test_status_fields(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        service.set_budget(
            "Food & Drink", Decimal("200.00"), start_month="2026-04", actor="cli"
        )
        result = service.status(month="2026-04")
        cat = result.categories[0]
        assert isinstance(cat, BudgetCategoryStatusRow)
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
        service.set_budget(
            "Food & Drink", Decimal("50.00"), start_month="2026-04", actor="cli"
        )
        result = service.status(month="2026-04")
        cat = result.categories[0]
        assert cat.status == "OVER"
        assert cat.remaining < 0

    @pytest.mark.unit
    def test_status_warning_threshold(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        # Budget of 90 with 80 spent = 88.9% => WARNING
        service.set_budget(
            "Food & Drink", Decimal("90.00"), start_month="2026-04", actor="cli"
        )
        result = service.status(month="2026-04")
        cat = result.categories[0]
        assert cat.status == "WARNING"

    @pytest.mark.unit
    def test_status_envelope_via_build_envelope(self, budget_db: Database) -> None:
        """build_envelope(data=BudgetStatusPayload) produces the expected envelope shape."""
        from moneybin.protocol.envelope import build_envelope

        service = BudgetService(budget_db)
        service.set_budget(
            "Food & Drink", Decimal("200.00"), start_month="2026-04", actor="cli"
        )
        result = service.status(month="2026-04")
        envelope = build_envelope(
            data=result,
            sensitivity="low",
            period=result.month,
            actions=["Use reports_spending for detailed category breakdown"],
        )
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "low"
        assert d["summary"]["period"] == "2026-04"
        assert isinstance(d["data"]["categories"], list)
        assert len(d["actions"]) > 0


class TestEmptyResults:
    """Tests for service behavior with no budgets set."""

    @pytest.mark.unit
    def test_status_no_budgets(self, budget_db: Database) -> None:
        service = BudgetService(budget_db)
        result = service.status(month="2026-04")
        assert isinstance(result, BudgetStatusPayload)
        assert result.categories == []


class TestSetBudgetCategoryIdResolution:
    """Phase 1 dual-write: set_budget INSERT populates category_id."""

    @pytest.mark.unit
    def test_known_category_resolves_to_category_id(self, budget_db: Database) -> None:
        seed_categories_view(budget_db)
        cat_id = CategorizationService(budget_db).create_category("Hobbies")
        BudgetService(budget_db).set_budget(
            "Hobbies", Decimal("100.00"), start_month="2026-05", actor="cli"
        )
        row = budget_db.execute(
            "SELECT category_id FROM app.budgets WHERE category = 'Hobbies'"
        ).fetchone()
        assert row == (cat_id,)

    @pytest.mark.unit
    def test_orphan_text_stays_null(self, budget_db: Database) -> None:
        seed_categories_view(budget_db)
        BudgetService(budget_db).set_budget(
            "NeverDefined", Decimal("50.00"), start_month="2026-05", actor="cli"
        )
        row = budget_db.execute(
            "SELECT category, category_id FROM app.budgets "
            "WHERE category = 'NeverDefined'"
        ).fetchone()
        assert row == ("NeverDefined", None)
