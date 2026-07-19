# tests/moneybin/test_mcp/test_tools.py
"""Tests for MCP tool functions.

These tests exercise the underlying tool functions directly. Registration
with the FastMCP server is covered by tests/mcp/test_visibility.py.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastmcp import Client, FastMCP

from moneybin.database import get_database
from moneybin.mcp.surface import ADMITTED_OUTPUT_SCHEMA_NAMES, STANDARD_TOOL_COUNT
from moneybin.mcp.tools.accounts import accounts, register_accounts_tools
from moneybin.mcp.tools.reports import register_reports_tools
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


class TestToolRegistration:
    """Verify tools register correctly and produce envelope responses."""

    @pytest.mark.integration
    async def test_live_registry_advertises_no_output_schemas(self) -> None:
        from moneybin.mcp.server import init_db, mcp

        init_db()
        async with Client(mcp) as client:
            tools = await client.list_tools()

        assert len(tools) == STANDARD_TOOL_COUNT
        advertised = frozenset(tool.name for tool in tools if tool.outputSchema)
        assert advertised == ADMITTED_OUTPUT_SCHEMA_NAMES

    @pytest.mark.unit
    async def test_reports_tools_register(self) -> None:
        srv = FastMCP("test")
        register_reports_tools(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert names == {"reports"}

    @pytest.mark.unit
    async def test_accounts_tools_register(self) -> None:

        srv = FastMCP("test")
        register_accounts_tools(srv)
        names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        assert names == {
            "accounts",
            "accounts_set",
            "accounts_balances",
            "accounts_balance_assert",
        }

    @pytest.mark.unit
    async def test_accounts_returns_envelope(self, mcp_db: object) -> None:

        result = accounts()
        parsed = result.to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert parsed["summary"]["sensitivity"] == "low"
        # data is now a typed payload dict with a "rows" key
        assert len(parsed["data"]["rows"]) == 2  # 2 accounts from mcp_db fixture

    @pytest.mark.unit
    async def test_accounts_includes_last_four_and_credit_limit(
        self, mcp_db: object
    ) -> None:
        """Middleware handles CRITICAL masking; the service always returns full fields."""
        result = accounts()
        parsed = result.to_dict()
        # All AccountSummary rows have last_four and credit_limit fields present
        for account in parsed["data"]["rows"]:
            assert "last_four" in account
            assert "credit_limit" in account

    @pytest.mark.unit
    async def test_sql_query_returns_envelope(self, mcp_db: Path) -> None:

        with get_database(read_only=False) as db:
            db.conn.execute(_INSERT_TRANSACTIONS)

        # Also exercise registration to ensure no smoke errors.
        register_sql_tools(FastMCP("test"))

        result = await sql_query(
            query="SELECT COUNT(*) AS cnt FROM core.fct_transactions"
        )
        parsed = result.to_dict()
        assert "summary" in parsed
        assert parsed["data"][0]["cnt"] == 2

    @pytest.mark.unit
    async def test_sql_schema_returns_envelope(self, mcp_db: object) -> None:

        result = await sql_schema()
        parsed = result.to_dict()
        # sql_schema uses dynamic_classification=True and sets low sensitivity explicitly
        # (schema metadata only — no financial data)
        assert parsed["summary"]["sensitivity"] == "low"
        data = parsed["data"]
        assert data["version"] == 1
        names = {t["name"] for t in data["tables"]}
        assert "core.fct_transactions" in names
        assert "core.dim_accounts" in names

    @pytest.mark.unit
    async def test_sql_schema_compact_default_omits_columns(
        self, mcp_db: object
    ) -> None:
        """The default (no-arg) response is the compact catalog, not the full doc."""
        result = await sql_schema()
        data = result.to_dict()["data"]
        # Compact entries carry counts, not the per-column detail.
        sample = next(iter(data["tables"]))
        assert "column_count" in sample
        assert "columns" not in sample
        # `beyond_the_interface` must survive into the compact view.
        assert data["beyond_the_interface"] is not None
        # Actions point at the drill-in / full-doc paths.
        actions = result.to_dict()["actions"]
        assert any("table='" in a for a in actions)

    @pytest.mark.unit
    async def test_sql_schema_full_doc_with_star(self, mcp_db: object) -> None:
        """table='*' returns the full schema document with column detail."""
        result = await sql_schema(table="*")
        data = result.to_dict()["data"]
        # Full doc keeps the per-column detail for every table.
        for entry in data["tables"]:
            assert "columns" in entry
            assert isinstance(entry["columns"], list)

    @pytest.mark.unit
    async def test_sql_schema_drill_into_single_table(self, mcp_db: object) -> None:
        """table='<schema.name>' returns only that table with full detail."""
        result = await sql_schema(table="core.fct_transactions")
        data = result.to_dict()["data"]
        assert [t["name"] for t in data["tables"]] == ["core.fct_transactions"]
        assert data["tables"][0]["columns"]

    @pytest.mark.unit
    async def test_sql_schema_unknown_table_returns_error_envelope(
        self, mcp_db: object
    ) -> None:
        """Unknown table routes through build_error_envelope (status='error')."""
        result = await sql_schema(table="core.nonexistent")
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "sql_unknown_table"
        assert "core.fct_transactions" in parsed["error"]["details"]["available_tables"]
