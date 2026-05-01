# tests/moneybin/test_mcp/test_categorization_tools.py
"""Tests for v1 categorization MCP tools.

Individual categorization logic is tested in test_categorization_service.py.
These tests verify the MCP tool wiring, envelope format, and basic end-to-end.
"""

import asyncio

import pytest
from fastmcp import FastMCP

from moneybin.mcp.tools.categorize import (
    categorize_categories,
    categorize_stats,
    register_categorize_tools,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


def _registered_names() -> set[str]:
    srv = FastMCP("test")
    register_categorize_tools(srv)
    return {t.name for t in asyncio.run(srv._list_tools())}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]


class TestCategorizeToolRegistration:
    """Verify categorize tools register and return envelopes."""

    @pytest.mark.unit
    def test_all_categorize_tools_register(self) -> None:
        names = _registered_names()
        assert "categorize_categories" in names
        assert "categorize_rules" in names
        assert "categorize_merchants" in names
        assert "categorize_stats" in names
        assert "categorize_uncategorized" in names
        assert "categorize_bulk" in names
        assert "categorize_create_rules" in names
        assert "categorize_delete_rule" in names
        assert "categorize_create_merchants" in names
        assert "categorize_create_category" in names
        assert "categorize_toggle_category" in names

    @pytest.mark.unit
    def test_categorize_stats_returns_envelope(self, mcp_db: object) -> None:
        parsed = categorize_stats().to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert parsed["summary"]["sensitivity"] == "low"

    @pytest.mark.unit
    def test_categorize_categories_returns_envelope(self, mcp_db: object) -> None:
        """List categories returns a valid envelope (empty when no data)."""
        cat_result = categorize_categories().to_dict()
        assert "summary" in cat_result
        assert "data" in cat_result
        assert isinstance(cat_result["data"], list)

    @pytest.mark.unit
    def test_register_includes_auto_rule_tools(self) -> None:
        names = _registered_names()
        assert {
            "categorize_auto_review",
            "categorize_auto_confirm",
            "categorize_auto_stats",
        } <= names
