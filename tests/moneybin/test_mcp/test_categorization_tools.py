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
    categorize_seed,
    categorize_stats,
    register_categorize_tools,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


def _registered_names() -> set[str]:
    srv = FastMCP("test")
    register_categorize_tools(srv)
    return {t.name for t in asyncio.run(srv._list_tools())}  # noqa: SLF001


class TestCategorizeToolRegistration:
    """Verify categorize tools register and return envelopes."""

    @pytest.mark.unit
    def test_all_categorize_tools_register(self) -> None:
        names = _registered_names()
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
        parsed = categorize_stats().to_dict()
        assert "summary" in parsed
        assert "data" in parsed
        assert parsed["summary"]["sensitivity"] == "low"

    @pytest.mark.unit
    def test_categorize_seed_returns_envelope(self, mcp_db: object) -> None:
        """Seed materializes the SQLMesh seed table and seeds categories.

        ``ensure_seed_table`` runs a targeted SQLMesh plan to create
        ``seeds.categories`` before seeding, so this succeeds even
        without a prior ``sqlmesh apply``.
        """
        seed_result = categorize_seed().to_dict()
        assert "summary" in seed_result
        assert "data" in seed_result
        assert "seeded_count" in seed_result["data"]

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
            "categorize.auto_review",
            "categorize.auto_confirm",
            "categorize.auto_stats",
        } <= names
