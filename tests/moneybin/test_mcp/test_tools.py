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

    @pytest.mark.unit
    async def test_sql_schema_wildcard_lists_one_schema(self, mcp_db: object) -> None:
        """table='<schema>.*' enumerates that schema's live relations.

        `raw` holds 21 `TableRef`s and zero `audience="interface"` tags, so the
        curated catalog cannot see it at all. Before this, the only enumeration
        was `SHOW ALL TABLES` through `sql_query`, which returned 117 KB on a
        real profile because DuckDB attaches every column name and type.
        """
        result = await sql_schema(table="raw.*")
        parsed = result.to_dict()
        assert parsed["status"] != "error"
        listed = {t["name"] for t in parsed["data"]["tables"]}
        assert "raw.ofx_institutions" in listed
        # Name and kind only — the point is that this fits in a context window.
        assert "columns" not in next(iter(parsed["data"]["tables"]))

    @pytest.mark.unit
    async def test_sql_schema_wildcard_refuses_a_schema_sql_query_refuses(
        self, mcp_db: object
    ) -> None:
        """Listing must not reach a schema the query gate would refuse."""
        result = await sql_schema(table="meta.*")
        parsed = result.to_dict()
        assert parsed["status"] == "error"
        assert parsed["error"]["code"] == "sql_schema_not_allowed"

    @pytest.mark.unit
    async def test_sql_schema_answers_one_schema_question_one_way(
        self, mcp_db: object
    ) -> None:
        """Both spellings of "is this schema queryable" must answer alike.

        `meta.*` refuses with `sql_schema_not_allowed`; an exact name under
        the same schema used to fall through to `sql_unknown_table` plus a
        list of curated names, which reads as a typo the agent should fix.
        The table is not unknown — the schema is fenced, and no spelling of
        the name gets in.
        """
        wildcard = await sql_schema(table="meta.*")
        exact = await sql_schema(table="meta.model_freshness")
        assert exact.to_dict()["status"] == "error"
        assert (
            exact.to_dict()["error"]["code"]
            == wildcard.to_dict()["error"]["code"]
            == "sql_schema_not_allowed"
        )

    @pytest.mark.unit
    async def test_sql_schema_unqualified_name_is_unknown_not_refused(
        self, mcp_db: object
    ) -> None:
        """A bare name names no schema, so it is a typo — not a fenced schema.

        The refusal above keys on the schema qualifier, so a name without one
        must not borrow it: `fct_transactions` is a caller who forgot `core.`,
        and the recovery is the available-table list, not "that schema is
        internal."
        """
        parsed = (await sql_schema(table="fct_transactions")).to_dict()
        assert parsed["error"]["code"] == "sql_unknown_table"
        assert "core.fct_transactions" in parsed["error"]["details"]["available_tables"]

    @pytest.mark.unit
    async def test_sql_schema_does_not_call_a_queryable_table_unknown(
        self, mcp_db: object
    ) -> None:
        """An uncurated relation exists; saying "Unknown table" is a false negative.

        Server instructions point agents at `sql_schema` as *the* schema
        surface, so "Unknown table: raw.ofx_institutions" reads as "does not
        exist" when `sql_query` reads that table fine. The refusal has to
        separate "not in the curated catalog" from "not in the database", and
        name the path that does work.
        """
        result = await sql_schema(table="raw.ofx_institutions")
        parsed = result.to_dict()
        assert parsed["error"]["code"] == "sql_table_not_curated"
        assert parsed["error"]["code"] != "sql_unknown_table"
        hint = parsed["error"]["hint"] or ""
        assert "DESCRIBE" in hint

    @pytest.mark.unit
    async def test_compact_catalog_entries_carry_relation_kind(
        self, mcp_db: object
    ) -> None:
        """The curated view must also say table vs view, not just the live one."""
        result = await sql_schema()
        for entry in result.to_dict()["data"]["tables"]:
            assert entry["kind"] in {"table", "view"}
