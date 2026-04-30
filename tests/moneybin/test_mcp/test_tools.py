# tests/moneybin/test_mcp/test_tools.py
"""Tests for v1 MCP tool registration and basic functionality.

These tests verify that the prototype tools have been successfully
migrated to v1 namespace-based tools. Individual tool logic is tested
in the service layer tests — these test the registration and wiring.
"""

import pytest

from moneybin.mcp.namespaces import NamespaceRegistry
from moneybin.mcp.tools.accounts import register_accounts_tools
from moneybin.mcp.tools.spending import register_spending_tools
from moneybin.mcp.tools.sql import register_sql_tools

pytestmark = pytest.mark.usefixtures("mcp_db")

_INSERT_TRANSACTIONS = """
    INSERT INTO core.fct_transactions (
        transaction_id, account_id, transaction_date, amount,
        amount_absolute, transaction_direction, description,
        transaction_type, is_pending, currency_code, source_type,
        source_extracted_at, loaded_at,
        transaction_year, transaction_month, transaction_day,
        transaction_day_of_week, transaction_year_month, transaction_year_quarter
    ) VALUES
    ('T1', 'ACC001', '2026-04-10', -50.00, 50.00, 'expense', 'Coffee Shop',
     'DEBIT', false, 'USD', 'ofx', '2026-04-10', CURRENT_TIMESTAMP,
     2026, 4, 10, 3, '2026-04', '2026-Q2'),
    ('T2', 'ACC001', '2026-04-15', 5000.00, 5000.00, 'income', 'Employer',
     'CREDIT', false, 'USD', 'ofx', '2026-04-15', CURRENT_TIMESTAMP,
     2026, 4, 15, 1, '2026-04', '2026-Q2')
"""


class TestV1ToolRegistration:
    """Verify v1 tools register correctly and produce envelope responses."""

    @pytest.mark.unit
    def test_spending_tools_register(self) -> None:
        registry = NamespaceRegistry()
        tools = register_spending_tools(registry)
        names = {t.name for t in tools}
        assert "spending.summary" in names
        assert "spending.by_category" in names

    @pytest.mark.unit
    def test_accounts_tools_register(self) -> None:
        registry = NamespaceRegistry()
        tools = register_accounts_tools(registry)
        names = {t.name for t in tools}
        assert "accounts.list" in names
        assert "accounts.balances" in names

    @pytest.mark.unit
    def test_accounts_list_returns_envelope(self, mcp_db: object) -> None:
        registry = NamespaceRegistry()
        tools = register_accounts_tools(registry)
        tool = next(t for t in tools if t.name == "accounts.list")
        result = tool.fn()
        parsed = result.to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert parsed["summary"]["sensitivity"] == "low"
        assert len(parsed["data"]) == 2  # 2 accounts from mcp_db fixture

    @pytest.mark.unit
    def test_sql_query_returns_envelope(self, mcp_db: object) -> None:
        from moneybin.mcp.server import get_db

        get_db().execute(_INSERT_TRANSACTIONS)

        registry = NamespaceRegistry()
        tools = register_sql_tools(registry)
        tool = next(t for t in tools if t.name == "sql.query")
        result = tool.fn(query="SELECT COUNT(*) AS cnt FROM core.fct_transactions")
        parsed = result.to_dict()
        assert "summary" in parsed
        assert parsed["data"][0]["cnt"] == 2
