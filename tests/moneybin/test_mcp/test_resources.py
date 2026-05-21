"""Tests for MCP v1 resource definitions."""

from __future__ import annotations

import json

import pytest

from moneybin.mcp.resources import resource_schema

pytestmark = pytest.mark.usefixtures("mcp_db")


# ---------------------------------------------------------------------------
# moneybin://schema
# ---------------------------------------------------------------------------


class TestResourceSchema:
    """Tests for moneybin://schema resource."""

    @pytest.mark.unit
    def test_returns_json(self) -> None:
        result = resource_schema()
        # Must parse as JSON without error
        data = json.loads(result)
        assert isinstance(data, (dict, list))

    @pytest.mark.unit
    def test_contains_table_info(self) -> None:
        result = resource_schema()
        # Schema doc should contain at least one of the canonical layer names
        # consumers query against (core, app, reports).
        assert "core" in result or "app" in result or "reports" in result
