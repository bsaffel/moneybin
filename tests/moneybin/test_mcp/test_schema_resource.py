"""Tests for the moneybin://schema MCP resource."""

from __future__ import annotations

import json

import pytest

from moneybin.database import Database
from moneybin.mcp.resources import resource_schema

pytestmark = pytest.mark.unit


def test_resource_schema_returns_valid_json(schema_catalog_db: Database) -> None:
    """Parsed JSON must have version, tables, conventions, and beyond_the_interface."""
    result = resource_schema()
    data = json.loads(result)
    assert data["version"] == 1
    assert isinstance(data["tables"], list)
    assert "conventions" in data
    assert "beyond_the_interface" in data


def test_resource_schema_contains_core_interface_tables(
    schema_catalog_db: Database,
) -> None:
    """core.fct_transactions and core.dim_accounts must appear in tables[].name."""
    result = resource_schema()
    data = json.loads(result)
    names = {t["name"] for t in data["tables"]}
    assert "core.fct_transactions" in names
    assert "core.dim_accounts" in names


def test_resource_schema_tables_have_required_fields(
    schema_catalog_db: Database,
) -> None:
    """Each table entry has the expected top-level keys."""
    data = json.loads(resource_schema())
    assert data["tables"], "fixture should have seeded core tables"
    for table in data["tables"]:
        assert "name" in table
        assert "purpose" in table
        assert "columns" in table
        assert "examples" in table


def test_resource_schema_columns_have_name_and_type(
    schema_catalog_db: Database,
) -> None:
    """Each column entry has at least name and type."""
    data = json.loads(resource_schema())
    assert data["tables"], "fixture should have seeded core tables"
    for table in data["tables"]:
        for col in table["columns"]:
            assert "name" in col
            assert "type" in col
