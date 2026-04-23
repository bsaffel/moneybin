# tests/moneybin/test_mcp/test_v1_tools.py
"""Tests for v1 MCP tools."""

import json

import pytest

from moneybin.mcp.namespaces import NamespaceRegistry
from moneybin.mcp.tools.spending import register_spending_tools

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
    ('T1', 'ACC001', '2026-04-10', -50.00, 50.00, 'expense', 'Coffee Shop', 'DEBIT', false, 'USD', 'ofx', '2026-04-10', CURRENT_TIMESTAMP, 2026, 4, 10, 3, '2026-04', '2026-Q2'),
    ('T2', 'ACC001', '2026-04-15', 5000.00, 5000.00, 'income', 'Employer Inc', 'CREDIT', false, 'USD', 'ofx', '2026-04-15', CURRENT_TIMESTAMP, 2026, 4, 15, 1, '2026-04', '2026-Q2')
"""  # noqa: S608  # test input, not executing SQL


class TestSpendingSummaryTool:
    """Tests for spending.summary v1 tool."""

    def _insert_data(self, mcp_db: object) -> None:
        from moneybin.mcp.server import get_db

        get_db().execute(_INSERT_TRANSACTIONS)

    @pytest.mark.unit
    def test_returns_envelope(self, mcp_db: object) -> None:
        self._insert_data(mcp_db)
        registry = NamespaceRegistry()
        tools = register_spending_tools(registry)
        # Find spending.summary
        summary_tool = next(t for t in tools if t.name == "spending.summary")
        result = summary_tool.fn(months=3)
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert "summary" in parsed
        assert "data" in parsed
        assert "actions" in parsed
        assert parsed["summary"]["sensitivity"] == "low"

    @pytest.mark.unit
    def test_data_shape(self, mcp_db: object) -> None:
        self._insert_data(mcp_db)
        registry = NamespaceRegistry()
        tools = register_spending_tools(registry)
        summary_tool = next(t for t in tools if t.name == "spending.summary")
        parsed = json.loads(summary_tool.fn(months=3))
        data = parsed["data"]
        assert len(data) >= 1
        assert "period" in data[0]
        assert "income" in data[0]
        assert "expenses" in data[0]
        assert "net" in data[0]
        assert "transaction_count" in data[0]
