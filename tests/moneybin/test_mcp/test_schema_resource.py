"""Tests for the moneybin://schema MCP resource."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.mcp.resources import resource_schema
from tests.moneybin.db_helpers import apply_core_table_comments, create_core_tables_raw


@pytest.fixture()
def schema_mcp_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Database with core tables and comments applied, singleton patched."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "schema_mcp.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    create_core_tables_raw(database.conn)
    apply_core_table_comments(database)
    db_module._database_instance = database  # type: ignore[attr-defined]
    try:
        yield database
    finally:
        db_module._database_instance = None  # type: ignore[attr-defined]
        database.close()


@pytest.mark.unit
def test_resource_schema_returns_valid_json(schema_mcp_db: Database) -> None:
    """Parsed JSON must have version, tables, conventions, and beyond_the_interface."""
    result = resource_schema()
    data = json.loads(result)
    assert data["version"] == 1
    assert isinstance(data["tables"], list)
    assert "conventions" in data
    assert "beyond_the_interface" in data


@pytest.mark.unit
def test_resource_schema_contains_core_interface_tables(
    schema_mcp_db: Database,
) -> None:
    """core.fct_transactions and core.dim_accounts must appear in tables[].name."""
    result = resource_schema()
    data = json.loads(result)
    names = {t["name"] for t in data["tables"]}
    assert "core.fct_transactions" in names
    assert "core.dim_accounts" in names


@pytest.mark.unit
def test_resource_schema_tables_have_required_fields(
    schema_mcp_db: Database,
) -> None:
    """Each table entry has the expected top-level keys."""
    data = json.loads(resource_schema())
    assert data["tables"], "fixture should have seeded core tables"
    for table in data["tables"]:
        assert "name" in table
        assert "purpose" in table
        assert "columns" in table
        assert "examples" in table


@pytest.mark.unit
def test_resource_schema_columns_have_name_and_type(
    schema_mcp_db: Database,
) -> None:
    """Each column entry has at least name and type."""
    data = json.loads(resource_schema())
    assert data["tables"], "fixture should have seeded core tables"
    for table in data["tables"]:
        for col in table["columns"]:
            assert "name" in col
            assert "type" in col
