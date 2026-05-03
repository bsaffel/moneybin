# tests/moneybin/test_mcp/test_tools.py
"""Tests for v1 MCP tool functions.

These tests exercise the underlying tool functions directly. Registration
with the FastMCP server is covered by tests/mcp/test_visibility.py.
"""

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.accounts import accounts_list, register_accounts_tools
from moneybin.mcp.tools.spending import register_spending_tools
from moneybin.mcp.tools.sql import register_sql_tools, sql_query, sql_schema

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
        srv = FastMCP("test")
        register_spending_tools(srv)
        # Synchronous accessor surface differs by version; resolve via asyncio.
        import asyncio

        names = {t.name for t in asyncio.run(srv._list_tools())}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert "spending_summary" in names
        assert "spending_by_category" in names

    @pytest.mark.unit
    def test_accounts_tools_register(self) -> None:
        import asyncio

        srv = FastMCP("test")
        register_accounts_tools(srv)
        names = {t.name for t in asyncio.run(srv._list_tools())}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert "accounts_list" in names
        assert "accounts_balances" in names

    @pytest.mark.unit
    def test_accounts_list_returns_envelope(self, mcp_db: object) -> None:
        import asyncio

        result = asyncio.run(accounts_list())
        parsed = result.to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert parsed["summary"]["sensitivity"] == "low"
        assert len(parsed["data"]) == 2  # 2 accounts from mcp_db fixture

    @pytest.mark.unit
    def test_sql_query_returns_envelope(self, mcp_db: object) -> None:
        import asyncio

        from moneybin.mcp.server import get_db

        get_db().execute(_INSERT_TRANSACTIONS)

        # Also exercise registration to ensure no smoke errors.
        register_sql_tools(FastMCP("test"))

        result = asyncio.run(
            sql_query(query="SELECT COUNT(*) AS cnt FROM core.fct_transactions")
        )
        parsed = result.to_dict()
        assert "summary" in parsed
        assert parsed["data"][0]["cnt"] == 2

    @pytest.mark.unit
    def test_sql_schema_returns_envelope(self, mcp_db: object) -> None:
        import asyncio

        result = asyncio.run(sql_schema())
        parsed = result.to_dict()
        assert parsed["summary"]["sensitivity"] == "low"
        data = parsed["data"]
        assert data["version"] == 1
        names = {t["name"] for t in data["tables"]}
        assert "core.fct_transactions" in names
        assert "core.dim_accounts" in names
