"""Tests for MCP resource definitions."""

import json
from typing import Any

import duckdb
import pytest

from moneybin.mcp.resources import (
    accounts_summary,
    investments_holdings,
    recent_transactions,
    schema_table_detail,
    schema_tables,
    spending_categories,
    w2_by_year,
)

# ---------------------------------------------------------------------------
# Schema resources
# ---------------------------------------------------------------------------


class TestSchemaResources:
    """Tests for schema-related resources."""

    @pytest.mark.unit
    def test_schema_tables_returns_list(self) -> None:
        result = schema_tables()
        data: list[Any] = json.loads(result)
        assert isinstance(data, list)
        assert len(data) >= 4  # At least the tables we created

    @pytest.mark.unit
    def test_schema_table_detail_with_schema(self) -> None:
        result = schema_table_detail("core.dim_accounts")
        data: list[dict[str, Any]] = json.loads(result)
        assert isinstance(data, list)
        column_names = [r["column_name"] for r in data]
        assert "account_id" in column_names
        assert "account_type" in column_names

    @pytest.mark.unit
    def test_schema_table_detail_without_schema(self) -> None:
        result = schema_table_detail("dim_accounts")
        data: list[Any] = json.loads(result)
        assert isinstance(data, list)
        assert len(data) > 0

    @pytest.mark.unit
    def test_schema_table_not_found(self) -> None:
        result = schema_table_detail("nonexistent_table")
        data: dict[str, Any] = json.loads(result)
        assert "error" in data


# ---------------------------------------------------------------------------
# Account and transaction resources
# ---------------------------------------------------------------------------


class TestAccountsResource:
    """Tests for accounts summary resource."""

    @pytest.mark.unit
    def test_returns_accounts_with_balances(self) -> None:
        result = accounts_summary()
        data: list[dict[str, Any]] = json.loads(result)
        assert isinstance(data, list)
        assert len(data) == 2
        by_id = {r["account_id"]: r for r in data}
        assert float(by_id["ACC001"]["ledger_balance"]) == 5000.0
        assert float(by_id["ACC002"]["ledger_balance"]) == 15000.0

    @pytest.mark.unit
    def test_includes_institution_name(self) -> None:
        result = accounts_summary()
        data: list[dict[str, Any]] = json.loads(result)
        names = {r["institution_name"] for r in data}
        assert "Test Bank" in names

    @pytest.mark.unit
    def test_includes_source_system(self) -> None:
        result = accounts_summary()
        data: list[dict[str, Any]] = json.loads(result)
        assert all(r["source_system"] == "ofx" for r in data)


class TestRecentTransactions:
    """Tests for recent transactions resource."""

    @pytest.fixture(autouse=True)
    def _insert_data(self, mcp_db: duckdb.DuckDBPyConnection) -> None:  # pyright: ignore[reportUnusedFunction] — pytest autouse fixture
        mcp_db.execute("""
            INSERT INTO core.fct_transactions (
                transaction_id, account_id, transaction_date, amount,
                amount_absolute, transaction_direction, description, memo,
                transaction_type, is_pending, currency_code, source_system,
                source_extracted_at, dbt_loaded_at,
                transaction_year, transaction_month, transaction_day,
                transaction_day_of_week, transaction_year_month,
                transaction_year_quarter
            ) VALUES
            ('TXN001', 'ACC001', CURRENT_DATE - INTERVAL '5 days',
             -42.50, 42.50, 'expense', 'Grocery Store', 'Weekly groceries',
             'DEBIT', false, 'USD', 'ofx',
             '2025-01-24', CURRENT_TIMESTAMP,
             2025, 6, 15, 0, '2025-06', '2025-Q2')
        """)

    @pytest.mark.unit
    def test_returns_recent_transactions(self) -> None:
        result = recent_transactions()
        data: list[Any] = json.loads(result)
        # Should have our transaction from 5 days ago
        assert isinstance(data, list)
        assert len(data) >= 1

    @pytest.mark.unit
    def test_transaction_has_canonical_fields(self) -> None:
        result = recent_transactions()
        data: list[dict[str, Any]] = json.loads(result)
        assert "transaction_date" in data[0]
        assert "description" in data[0]
        assert "source_system" in data[0]


# ---------------------------------------------------------------------------
# W2 resource
# ---------------------------------------------------------------------------


class TestW2Resource:
    """Tests for W2 tax year resource."""

    @pytest.fixture(autouse=True)
    def _insert_data(self, mcp_db: duckdb.DuckDBPyConnection) -> None:  # pyright: ignore[reportUnusedFunction] — pytest autouse fixture
        mcp_db.execute("""
            INSERT INTO raw.w2_forms (
                tax_year, employee_ssn, employer_ein, employee_first_name,
                employee_last_name, employer_name, wages, federal_income_tax,
                source_file, extracted_at
            ) VALUES
            (2024, '***-**-1234', '12-3456789', 'Jane', 'Smith',
             'BigCo', 90000.00, 15000.00, 'w2.pdf', '2025-02-01')
        """)

    @pytest.mark.unit
    def test_returns_w2_for_year(self) -> None:
        result = w2_by_year("2024")
        data: list[dict[str, Any]] = json.loads(result)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["employer_name"] == "BigCo"

    @pytest.mark.unit
    def test_no_data_for_year(self) -> None:
        result = w2_by_year("2020")
        data: dict[str, Any] = json.loads(result)
        assert "message" in data


# ---------------------------------------------------------------------------
# Not-implemented resources
# ---------------------------------------------------------------------------


class TestNotImplementedResources:
    """Tests for stub resources."""

    @pytest.mark.unit
    def test_investments_not_implemented(self) -> None:
        result = investments_holdings()
        assert "[Not Yet Available]" in result

    @pytest.mark.unit
    def test_spending_categories_not_implemented(self) -> None:
        result = spending_categories()
        assert "[Not Yet Available]" in result
