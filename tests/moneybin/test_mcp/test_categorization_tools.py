"""Tests for MCP categorization tools."""

import json
from typing import Any

import pytest

from moneybin.mcp import server
from moneybin.mcp.tools import (
    get_categorization_stats,
    list_categories,
    list_categorization_rules,
    list_merchants,
)
from moneybin.mcp.write_tools import (
    categorize_transaction,
    create_categorization_rule,
    create_category,
    create_merchant_mapping,
    delete_categorization_rule,
    seed_categories,
    toggle_category,
)

# ---------------------------------------------------------------------------
# Shared INSERT SQL
# ---------------------------------------------------------------------------

_INSERT_TRANSACTIONS = """
    INSERT INTO core.fct_transactions (
        transaction_id, account_id, transaction_date, amount,
        amount_absolute, transaction_direction, description, memo,
        transaction_type, is_pending, currency_code, source_system,
        source_extracted_at, loaded_at,
        transaction_year, transaction_month, transaction_day,
        transaction_day_of_week, transaction_year_month,
        transaction_year_quarter
    ) VALUES
    ('TXN001', 'ACC001', '2025-06-15', -4.50, 4.50, 'expense',
     'SQ *STARBUCKS #1234 SEATTLE WA', 'Coffee', 'DEBIT', false,
     'USD', 'ofx', '2025-01-24', CURRENT_TIMESTAMP,
     2025, 6, 15, 0, '2025-06', '2025-Q2'),
    ('TXN002', 'ACC001', '2025-06-20', 3000.00, 3000.00, 'income',
     'ACME CORP PAYROLL', 'Payroll', 'CREDIT', false, 'USD', 'ofx',
     '2025-01-24', CURRENT_TIMESTAMP,
     2025, 6, 20, 5, '2025-06', '2025-Q2'),
    ('TXN003', 'ACC002', '2025-06-25', -52.13, 52.13, 'expense',
     'AMZN MKTP US*ABC123', 'Amazon', 'DEBIT', false, 'USD', 'ofx',
     '2025-01-24', CURRENT_TIMESTAMP,
     2025, 6, 25, 3, '2025-06', '2025-Q2')
"""


# ---------------------------------------------------------------------------
# Read tools
# ---------------------------------------------------------------------------


class TestListCategories:
    """Tests for list_categories tool."""

    @pytest.mark.unit
    def test_empty_returns_empty_list(self) -> None:
        result = list_categories()
        data: list[dict[str, Any]] = json.loads(result)
        assert data == []

    @pytest.mark.unit
    def test_returns_categories(self) -> None:
        with server.get_write_db() as db:
            db.execute("""
                INSERT INTO app.categories
                (category_id, category, subcategory, is_default, is_active)
                VALUES ('FND', 'Food & Drink', NULL, true, true)
            """)
        result = list_categories()
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 1
        assert data[0]["category"] == "Food & Drink"


class TestListCategorizationRules:
    """Tests for list_categorization_rules tool."""

    @pytest.mark.unit
    def test_empty_returns_empty_list(self) -> None:
        result = list_categorization_rules()
        data: list[dict[str, Any]] = json.loads(result)
        assert data == []

    @pytest.mark.unit
    def test_returns_rules(self) -> None:
        with server.get_write_db() as db:
            db.execute("""
                INSERT INTO app.categorization_rules
                (rule_id, name, merchant_pattern, match_type, category,
                 priority, is_active, created_by, created_at, updated_at)
                VALUES ('R001', 'Test Rule', 'TEST', 'contains', 'Other',
                        10, true, 'user', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """)
        result = list_categorization_rules()
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 1
        assert data[0]["name"] == "Test Rule"


class TestListMerchants:
    """Tests for list_merchants tool."""

    @pytest.mark.unit
    def test_empty_returns_empty_list(self) -> None:
        result = list_merchants()
        data: list[dict[str, Any]] = json.loads(result)
        assert data == []


class TestGetCategorizationStats:
    """Tests for get_categorization_stats tool."""

    @pytest.fixture(autouse=True)
    def _insert_data(self) -> None:  # pyright: ignore[reportUnusedFunction] — pytest autouse fixture
        with server.get_write_db() as db:
            db.execute(_INSERT_TRANSACTIONS)

    @pytest.mark.unit
    def test_returns_stats(self) -> None:
        result = get_categorization_stats()
        data: dict[str, Any] = json.loads(result)
        assert data["total"] == 3
        assert data["uncategorized"] == 3


# ---------------------------------------------------------------------------
# Write tools
# ---------------------------------------------------------------------------


class TestCategorizeTransaction:
    """Tests for categorize_transaction tool."""

    @pytest.fixture(autouse=True)
    def _insert_data(self) -> None:  # pyright: ignore[reportUnusedFunction] — pytest autouse fixture
        with server.get_write_db() as db:
            db.execute(_INSERT_TRANSACTIONS)

    @pytest.mark.unit
    def test_categorizes_transaction(self) -> None:
        result = categorize_transaction("TXN001", "Food & Drink", "Coffee Shops")
        assert "categorized" in result
        assert "Food & Drink" in result

    @pytest.mark.unit
    def test_default_categorized_by_user(self) -> None:
        categorize_transaction("TXN001", "Food & Drink")
        db = server.get_db()
        row = db.execute("""
            SELECT categorized_by FROM app.transaction_categories
            WHERE transaction_id = 'TXN001'
        """).fetchone()
        assert row is not None
        assert row[0] == "user"

    @pytest.mark.unit
    def test_auto_creates_merchant_mapping(self) -> None:
        categorize_transaction("TXN001", "Food & Drink", "Coffee Shops")
        db = server.get_db()
        merchants = db.execute("""
            SELECT canonical_name, category FROM app.merchants
        """).fetchall()
        assert len(merchants) >= 1


class TestCreateCategory:
    """Tests for create_category tool."""

    @pytest.mark.unit
    def test_creates_custom_category(self) -> None:
        result = create_category("Childcare", "Daycare", "Daycare expenses")
        assert "Created" in result
        assert "Childcare" in result

    @pytest.mark.unit
    def test_duplicate_returns_exists(self) -> None:
        create_category("Childcare", "Daycare")
        result = create_category("Childcare", "Daycare")
        assert "already exists" in result


class TestToggleCategory:
    """Tests for toggle_category tool."""

    @pytest.mark.unit
    def test_disables_category(self) -> None:
        with server.get_write_db() as db:
            db.execute("""
                INSERT INTO app.categories
                (category_id, category, is_default, is_active)
                VALUES ('TST', 'Test', true, true)
            """)
        result = toggle_category("TST", False)
        assert "disabled" in result


class TestSeedCategories:
    """Tests for seed_categories tool."""

    @pytest.mark.unit
    def test_seed_without_seed_table(self) -> None:
        # Should fail gracefully when seed table doesn't exist
        result = seed_categories()
        assert "Error" in result


class TestCreateMerchantMapping:
    """Tests for create_merchant_mapping tool."""

    @pytest.mark.unit
    def test_creates_merchant(self) -> None:
        result = create_merchant_mapping(
            "STARBUCKS",
            "Starbucks",
            "contains",
            "Food & Drink",
            "Coffee Shops",
        )
        assert "Created" in result
        assert "Starbucks" in result


class TestCreateCategorizationRule:
    """Tests for create_categorization_rule tool."""

    @pytest.mark.unit
    def test_creates_rule(self) -> None:
        result = create_categorization_rule(
            name="Starbucks -> Coffee",
            merchant_pattern="STARBUCKS",
            category="Food & Drink",
            subcategory="Coffee Shops",
        )
        assert "Created" in result
        assert "Starbucks -> Coffee" in result

    @pytest.mark.unit
    def test_with_amount_range(self) -> None:
        result = create_categorization_rule(
            name="Large Amazon",
            merchant_pattern="AMZN",
            category="Shopping",
            subcategory="Electronics",
            min_amount=-500.0,
            max_amount=-100.0,
        )
        assert "Created" in result


class TestDeleteCategorizationRule:
    """Tests for delete_categorization_rule tool."""

    @pytest.mark.unit
    def test_deletes_rule(self) -> None:
        with server.get_write_db() as db:
            db.execute("""
                INSERT INTO app.categorization_rules
                (rule_id, name, merchant_pattern, match_type, category,
                 priority, is_active, created_by, created_at, updated_at)
                VALUES ('R001', 'Test', 'TEST', 'contains', 'Other',
                        10, true, 'user', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """)
        result = delete_categorization_rule("R001")
        assert "Deleted" in result
