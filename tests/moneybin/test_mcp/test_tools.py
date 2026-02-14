"""Tests for MCP tool implementations."""

import json
from typing import Any

import duckdb
import pytest

from moneybin.mcp.tools import (
    describe_table,
    find_recurring_transactions,
    get_account_balances,
    get_balance_history,
    get_budget_status,
    get_investment_holdings,
    get_investment_performance,
    get_liabilities_summary,
    get_monthly_summary,
    get_net_worth,
    get_spending_by_category,
    get_tax_summary,
    get_w2_summary,
    list_accounts,
    list_institutions,
    list_tables,
    query_transactions,
    run_read_query,
)

# ---------------------------------------------------------------------------
# Shared INSERT SQL for test classes that need transactions or W2 data
# ---------------------------------------------------------------------------

_INSERT_TRANSACTIONS = """
    INSERT INTO core.fct_transactions (
        transaction_id, account_id, transaction_date, amount,
        amount_absolute, transaction_direction, description, memo,
        transaction_type, is_pending, currency_code, source_system,
        source_extracted_at, dbt_loaded_at,
        transaction_year, transaction_month, transaction_day,
        transaction_day_of_week, transaction_year_month,
        transaction_year_quarter
    ) VALUES
    ('TXN001', 'ACC001', '2025-06-15', -50.00, 50.00, 'expense',
     'Coffee Shop', 'Morning coffee', 'DEBIT', false, 'USD', 'ofx',
     '2025-01-24', CURRENT_TIMESTAMP,
     2025, 6, 15, 0, '2025-06', '2025-Q2'),
    ('TXN002', 'ACC001', '2025-06-20', 3000.00, 3000.00, 'income',
     'Employer Inc', 'Payroll', 'CREDIT', false, 'USD', 'ofx',
     '2025-01-24', CURRENT_TIMESTAMP,
     2025, 6, 20, 5, '2025-06', '2025-Q2'),
    ('TXN003', 'ACC002', '2025-06-25', -200.00, 200.00, 'expense',
     'Amazon', 'Order #12345', 'DEBIT', false, 'USD', 'ofx',
     '2025-01-24', CURRENT_TIMESTAMP,
     2025, 6, 25, 3, '2025-06', '2025-Q2')
"""

_INSERT_W2 = """
    INSERT INTO raw.w2_forms (
        tax_year, employee_ssn, employer_ein, employee_first_name,
        employee_last_name, employer_name, wages, federal_income_tax,
        social_security_wages, social_security_tax, medicare_wages,
        medicare_tax, source_file, extracted_at
    ) VALUES
    (2024, '***-**-1234', '12-3456789', 'John', 'Doe',
     'Acme Corp', 75000.00, 12000.00, 75000.00, 4650.00,
     75000.00, 1087.50, 'w2_2024.pdf', '2025-02-01')
"""


# ---------------------------------------------------------------------------
# Live tool tests
# ---------------------------------------------------------------------------


class TestListTables:
    """Tests for the list_tables tool."""

    @pytest.mark.unit
    def test_returns_json_array(self) -> None:
        result = list_tables()
        data: list[dict[str, Any]] = json.loads(result)
        assert isinstance(data, list)
        assert len(data) > 0

    @pytest.mark.unit
    def test_includes_core_tables(self) -> None:
        result = list_tables()
        data: list[dict[str, Any]] = json.loads(result)
        table_names = [r["table_name"] for r in data]
        assert "dim_accounts" in table_names
        assert "fct_transactions" in table_names


class TestDescribeTable:
    """Tests for the describe_table tool."""

    @pytest.fixture(autouse=True)
    def _insert_data(self, mcp_db: duckdb.DuckDBPyConnection) -> None:  # pyright: ignore[reportUnusedFunction] — pytest autouse fixture
        mcp_db.execute(_INSERT_TRANSACTIONS)

    @pytest.mark.unit
    def test_describes_existing_table(self) -> None:
        result = describe_table("fct_transactions", "core")
        assert "transaction_id" in result
        assert "amount" in result
        assert "Row count: 3" in result

    @pytest.mark.unit
    def test_unknown_table_returns_empty(self) -> None:
        result = describe_table("nonexistent_table", "core")
        assert "Row count:" in result


class TestListAccounts:
    """Tests for the list_accounts tool."""

    @pytest.mark.unit
    def test_returns_accounts(self) -> None:
        result = list_accounts()
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 2
        account_ids = [r["account_id"] for r in data]
        assert "ACC001" in account_ids
        assert "ACC002" in account_ids

    @pytest.mark.unit
    def test_includes_source_system(self) -> None:
        result = list_accounts()
        data: list[dict[str, Any]] = json.loads(result)
        assert all(r["source_system"] == "ofx" for r in data)


class TestQueryTransactions:
    """Tests for the query_transactions tool."""

    @pytest.fixture(autouse=True)
    def _insert_data(self, mcp_db: duckdb.DuckDBPyConnection) -> None:  # pyright: ignore[reportUnusedFunction] — pytest autouse fixture
        mcp_db.execute(_INSERT_TRANSACTIONS)

    @pytest.mark.unit
    def test_returns_all_transactions(self) -> None:
        result = query_transactions()
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 3

    @pytest.mark.unit
    def test_filter_by_payee(self) -> None:
        result = query_transactions(payee_pattern="%Coffee%")
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 1
        assert data[0]["description"] == "Coffee Shop"

    @pytest.mark.unit
    def test_filter_by_amount_range(self) -> None:
        result = query_transactions(min_amount=1000.0)
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 1
        assert float(data[0]["amount"]) == 3000.0

    @pytest.mark.unit
    def test_filter_by_account(self) -> None:
        result = query_transactions(account_id="ACC002")
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 1

    @pytest.mark.unit
    def test_limit_respected(self) -> None:
        result = query_transactions(limit=1)
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 1

    @pytest.mark.unit
    def test_includes_source_system(self) -> None:
        result = query_transactions()
        data: list[dict[str, Any]] = json.loads(result)
        assert all(r["source_system"] == "ofx" for r in data)


class TestGetAccountBalances:
    """Tests for the get_account_balances tool."""

    @pytest.mark.unit
    def test_returns_balances(self) -> None:
        result = get_account_balances()
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 2

    @pytest.mark.unit
    def test_filter_by_account(self) -> None:
        result = get_account_balances(account_id="ACC001")
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 1
        assert float(data[0]["ledger_balance"]) == 5000.0


class TestListInstitutions:
    """Tests for the list_institutions tool."""

    @pytest.mark.unit
    def test_returns_institutions(self) -> None:
        result = list_institutions()
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 2
        orgs = [r["organization"] for r in data]
        assert "Test Bank" in orgs


class TestGetW2Summary:
    """Tests for the get_w2_summary tool."""

    @pytest.fixture(autouse=True)
    def _insert_data(self, mcp_db: duckdb.DuckDBPyConnection) -> None:  # pyright: ignore[reportUnusedFunction] — pytest autouse fixture
        mcp_db.execute(_INSERT_W2)

    @pytest.mark.unit
    def test_returns_w2_data(self) -> None:
        result = get_w2_summary()
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 1
        assert data[0]["employer_name"] == "Acme Corp"
        assert float(data[0]["wages"]) == 75000.0

    @pytest.mark.unit
    def test_filter_by_year(self) -> None:
        result = get_w2_summary(tax_year=2024)
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 1

    @pytest.mark.unit
    def test_no_data_for_year(self) -> None:
        result = get_w2_summary(tax_year=2020)
        data: list[dict[str, Any]] = json.loads(result)
        assert len(data) == 0


class TestRunReadQuery:
    """Tests for the run_read_query tool."""

    @pytest.fixture(autouse=True)
    def _insert_data(self, mcp_db: duckdb.DuckDBPyConnection) -> None:  # pyright: ignore[reportUnusedFunction] — pytest autouse fixture
        mcp_db.execute(_INSERT_TRANSACTIONS)

    @pytest.mark.unit
    def test_valid_select(self) -> None:
        result = run_read_query("""SELECT COUNT(*) AS cnt FROM core.fct_transactions""")
        data: list[dict[str, Any]] = json.loads(result)
        assert data[0]["cnt"] == 3

    @pytest.mark.unit
    def test_rejects_write_query(self) -> None:
        result = run_read_query("""DROP TABLE core.fct_transactions""")
        assert "Query rejected" in result

    @pytest.mark.unit
    def test_rejects_insert(self) -> None:
        result = run_read_query("""INSERT INTO core.dim_accounts VALUES ('x')""")
        assert "Query rejected" in result


# ---------------------------------------------------------------------------
# Not-implemented tool tests
# ---------------------------------------------------------------------------


class TestNotImplementedTools:
    """Verify all stub tools return the expected format."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "tool_fn",
        [
            lambda: get_spending_by_category(),
            lambda: get_monthly_summary(),
            lambda: get_investment_holdings(),
            lambda: get_investment_performance(),
            lambda: get_liabilities_summary(),
            lambda: get_net_worth(),
            lambda: get_balance_history(),
            lambda: find_recurring_transactions(),
            lambda: get_budget_status(),
            lambda: get_tax_summary(),
        ],
    )
    def test_returns_not_implemented_message(self, tool_fn: object) -> None:
        result = tool_fn()  # type: ignore[operator]
        assert isinstance(result, str)
        assert "[Not Yet Available]" in result
        assert "MoneyBin docs" in result
