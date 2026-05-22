"""Tests for the moneybin://schema MCP resource."""

from __future__ import annotations

import json

import pytest

from moneybin.database import Database
from moneybin.mcp.resources import resource_schema

pytestmark = pytest.mark.unit


def _insert_seed_connection(
    db: Database,
    *,
    connection_id: str,
    alias: str,
    status: str = "healthy",
) -> None:
    """Insert a seed-adapter connection row (bypasses audited repo for test speed)."""
    db.execute(
        """
        INSERT INTO app.gsheet_connections (
            connection_id, spreadsheet_id, sheet_gid, sheet_name, workbook_name,
            adapter, account_id, account_name, column_mapping, header_signature,
            skip_rows, alias, status
        ) VALUES (?, ?, 0, 'Sheet1', 'WB', 'seed', NULL, NULL,
                  ?, ?, 0, ?, ?)
        """,
        [
            connection_id,
            f"ss_{connection_id}",
            json.dumps({"col_a": "VARCHAR"}),
            json.dumps(["col_a"]),
            alias,
            status,
        ],
    )


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


# ── seed-view discovery ───────────────────────────────────────────────────────


def test_resource_schema_omits_seed_view_when_view_missing(
    schema_catalog_db: Database,
) -> None:
    """Connection exists but no view materialized — schema doc omits the entry."""
    _insert_seed_connection(schema_catalog_db, connection_id="c1", alias="missing_view")
    data = json.loads(resource_schema())
    names = {t["name"] for t in data["tables"]}
    assert "raw.gsheet_missing_view" not in names


def test_resource_schema_includes_seed_view_columns_after_load(
    schema_catalog_db: Database,
) -> None:
    """Materialized seed view appears in the schema doc with its columns."""
    _insert_seed_connection(schema_catalog_db, connection_id="c2", alias="my_seed")
    schema_catalog_db.execute(
        "CREATE OR REPLACE VIEW raw.gsheet_my_seed AS "
        "SELECT CAST(NULL AS VARCHAR) AS col_a, "
        "CAST(NULL AS BIGINT) AS row_number, "
        "CAST(NULL AS TIMESTAMP) AS deleted_from_source_at, "
        "CAST(NULL AS TIMESTAMP) AS loaded_at "
        "WHERE FALSE"
    )
    data = json.loads(resource_schema())
    entry = next((t for t in data["tables"] if t["name"] == "raw.gsheet_my_seed"), None)
    assert entry is not None, "seed view should appear once materialized"
    col_names = {c["name"] for c in entry["columns"]}
    assert {"col_a", "row_number", "deleted_from_source_at", "loaded_at"} <= col_names
    assert "alias=my_seed" in entry["purpose"]


def test_resource_schema_omits_disconnected_seed_view(
    schema_catalog_db: Database,
) -> None:
    """Disconnected connections do not surface even if their view still exists."""
    _insert_seed_connection(
        schema_catalog_db,
        connection_id="c3",
        alias="archived",
        status="disconnected",
    )
    schema_catalog_db.execute(
        "CREATE OR REPLACE VIEW raw.gsheet_archived AS SELECT 1 AS x WHERE FALSE"
    )
    data = json.loads(resource_schema())
    names = {t["name"] for t in data["tables"]}
    assert "raw.gsheet_archived" not in names
