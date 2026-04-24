# tests/moneybin/test_mcp/test_categorization_tools.py
"""Tests for v1 categorization MCP tools.

Individual categorization logic is tested in test_categorization_service.py.
These tests verify the MCP tool wiring, envelope format, and basic end-to-end.
"""

import json

import pytest

from moneybin.mcp.namespaces import NamespaceRegistry
from moneybin.mcp.tools.categorize import register_categorize_tools

pytestmark = pytest.mark.usefixtures("mcp_db")


class TestCategorizeToolRegistration:
    """Verify categorize tools register and return envelopes."""

    @pytest.mark.unit
    def test_all_categorize_tools_register(self) -> None:
        registry = NamespaceRegistry()
        tools = register_categorize_tools(registry)
        names = {t.name for t in tools}
        assert "categorize.categories" in names
        assert "categorize.rules" in names
        assert "categorize.merchants" in names
        assert "categorize.stats" in names
        assert "categorize.uncategorized" in names
        assert "categorize.bulk" in names
        assert "categorize.create_rules" in names
        assert "categorize.delete_rule" in names
        assert "categorize.create_merchants" in names
        assert "categorize.create_category" in names
        assert "categorize.toggle_category" in names
        assert "categorize.seed" in names

    @pytest.mark.unit
    def test_categorize_stats_returns_envelope(self, mcp_db: object) -> None:
        registry = NamespaceRegistry()
        tools = register_categorize_tools(registry)
        tool = next(t for t in tools if t.name == "categorize.stats")
        result = tool.fn()
        parsed = json.loads(result)
        assert "summary" in parsed
        assert "data" in parsed
        assert parsed["summary"]["sensitivity"] == "low"

    @pytest.mark.unit
    def test_categorize_seed_returns_envelope_on_error(self, mcp_db: object) -> None:
        """Seed fails gracefully when seeds.categories table is absent.

        The seeds.categories table is created by SQLMesh, which isn't
        available in unit tests. The tool should still return a valid
        envelope with an error payload.
        """
        registry = NamespaceRegistry()
        tools = register_categorize_tools(registry)

        seed_tool = next(t for t in tools if t.name == "categorize.seed")
        seed_result = json.loads(seed_tool.fn())
        # Returns a valid envelope even on failure
        assert "summary" in seed_result
        assert "data" in seed_result
        assert "error" in seed_result["data"]

    @pytest.mark.unit
    def test_categorize_categories_returns_envelope(self, mcp_db: object) -> None:
        """List categories returns a valid envelope (empty when no data)."""
        registry = NamespaceRegistry()
        tools = register_categorize_tools(registry)
        cat_tool = next(t for t in tools if t.name == "categorize.categories")
        cat_result = json.loads(cat_tool.fn())
        assert "summary" in cat_result
        assert "data" in cat_result
        assert isinstance(cat_result["data"], list)
