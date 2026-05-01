# tests/moneybin/test_mcp/test_v1_tools.py
"""Tests for v1 MCP tools.

These tests exercise the underlying tool functions directly. Registration
with the FastMCP server is covered by tests/mcp/test_visibility.py.
"""

import pytest

from moneybin.mcp.tools.spending import spending_summary

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
        result = spending_summary(months=3)
        from moneybin.protocol.envelope import ResponseEnvelope

        assert isinstance(result, ResponseEnvelope)
        parsed = result.to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert "actions" in parsed
        assert parsed["summary"]["sensitivity"] == "low"

    @pytest.mark.unit
    def test_data_shape(self, mcp_db: object) -> None:
        self._insert_data(mcp_db)
        parsed = spending_summary(months=3).to_dict()
        data = parsed["data"]
        assert len(data) >= 1
        assert "period" in data[0]
        assert "income" in data[0]
        assert "expenses" in data[0]
        assert "net" in data[0]
        assert "transaction_count" in data[0]
