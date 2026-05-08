# tests/moneybin/test_mcp/test_categorization_tools.py
"""Tests for v1 categorization MCP tools.

Individual categorization logic is tested in test_categorization_service.py.
These tests verify the MCP tool wiring, envelope format, and basic end-to-end.
"""

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.categories import (
    categories_list,
    categories_toggle,
    register_categories_tools,
)
from moneybin.mcp.tools.merchants import register_merchants_tools
from moneybin.mcp.tools.transactions_categorize import (
    register_transactions_categorize_tools,
    transactions_categorize_stats,
)
from moneybin.mcp.tools.transactions_categorize_assist import (
    register_transactions_categorize_assist_tools,
)
from tests.moneybin.db_helpers import seed_categories_view

pytestmark = pytest.mark.usefixtures("mcp_db")


async def _registered_names() -> set[str]:
    srv = FastMCP("test")
    register_categories_tools(srv)
    register_merchants_tools(srv)
    register_transactions_categorize_tools(srv)
    register_transactions_categorize_assist_tools(srv)
    return {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]


class TestCategorizeToolRegistration:
    """Verify categorize tools register and return envelopes."""

    @pytest.mark.unit
    async def test_all_categorize_tools_register(self) -> None:
        names = await _registered_names()
        assert "categories_list" in names
        assert "transactions_categorize_rules_list" in names
        assert "merchants_list" in names
        assert "transactions_categorize_stats" in names
        assert "transactions_categorize_pending_list" in names
        assert "transactions_categorize_apply" in names
        assert "transactions_categorize_rules_create" in names
        assert "transactions_categorize_rule_delete" in names
        assert "merchants_create" in names
        assert "categories_create" in names
        assert "categories_toggle" in names
        assert "transactions_categorize_assist" in names

    @pytest.mark.unit
    async def test_categorize_stats_returns_envelope(self, mcp_db: object) -> None:
        parsed = (await transactions_categorize_stats()).to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert parsed["summary"]["sensitivity"] == "low"

    @pytest.mark.unit
    async def test_categorize_categories_returns_envelope(self, mcp_db: object) -> None:
        """List categories returns a valid envelope (empty when no data)."""
        cat_result = (await categories_list()).to_dict()
        assert "summary" in cat_result
        assert "data" in cat_result
        assert isinstance(cat_result["data"], list)

    @pytest.mark.unit
    async def test_register_includes_auto_rule_tools(self) -> None:
        names = await _registered_names()
        assert {
            "transactions_categorize_auto_review",
            "transactions_categorize_auto_accept",
            "transactions_categorize_auto_stats",
        } <= names


class TestToggleCategoryWritePath:
    """categories_toggle routes writes to the right backing table."""

    @pytest.mark.unit
    async def test_toggle_default_category_writes_override(
        self, mcp_db: object
    ) -> None:
        from moneybin.database import Database

        assert isinstance(mcp_db, Database)
        seed_categories_view(mcp_db)

        await categories_toggle(category_id="FND", is_active=False)

        rows = mcp_db.execute(
            "SELECT category_id, is_active FROM app.category_overrides"
        ).fetchall()
        assert rows == [("FND", False)]

    @pytest.mark.unit
    async def test_toggle_user_category_updates_user_categories(
        self, mcp_db: object
    ) -> None:
        from moneybin.database import Database

        assert isinstance(mcp_db, Database)
        seed_categories_view(mcp_db)
        mcp_db.execute("""
            INSERT INTO app.user_categories
            (category_id, category, subcategory, is_active)
            VALUES ('CUSTOM1', 'Childcare', 'Daycare', true)
        """)

        await categories_toggle(category_id="CUSTOM1", is_active=False)

        rows = mcp_db.execute(
            "SELECT is_active FROM app.user_categories WHERE category_id = ?",
            ["CUSTOM1"],
        ).fetchall()
        assert rows == [(False,)]
        # Override table is for defaults only — user toggles must not write here.
        override_count = mcp_db.execute(
            "SELECT COUNT(*) FROM app.category_overrides"
        ).fetchone()
        assert override_count == (0,)
