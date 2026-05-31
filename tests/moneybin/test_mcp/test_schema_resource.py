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
    """Materialized seed view appears in the schema doc with its _-prefixed columns."""
    _insert_seed_connection(schema_catalog_db, connection_id="c2", alias="my_seed")
    # Simulate the shape generate_seed_view_sql now emits: system carry
    # columns surface with a leading underscore (boundary against user-
    # header collisions). Test must assert that contract.
    schema_catalog_db.execute(
        "CREATE OR REPLACE VIEW raw.gsheet_my_seed AS "
        "SELECT CAST(NULL AS VARCHAR) AS col_a, "
        'CAST(NULL AS BIGINT) AS "_row_number", '
        'CAST(NULL AS TIMESTAMP) AS "_deleted_from_source_at", '
        'CAST(NULL AS TIMESTAMP) AS "_loaded_at" '
        "WHERE FALSE"
    )
    data = json.loads(resource_schema())
    entry = next((t for t in data["tables"] if t["name"] == "raw.gsheet_my_seed"), None)
    assert entry is not None, "seed view should appear once materialized"
    col_names = {c["name"] for c in entry["columns"]}
    assert {
        "col_a",
        "_row_number",
        "_deleted_from_source_at",
        "_loaded_at",
    } <= col_names
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


# ── PDF seed-view discovery ───────────────────────────────────────────────────


def _insert_pdf_import(
    db: Database,
    *,
    import_id: str,
    alias: str,
    status: str = "complete",
) -> None:
    """Insert a PDF import_log row (bypasses ImportService for test speed)."""
    db.execute(
        """
        INSERT INTO raw.import_log (
            import_id, source_file, source_type, source_origin,
            account_names, status
        ) VALUES (?, ?, 'pdf', ?, '[]', ?)
        """,
        [import_id, f"/tmp/{alias}.pdf", alias, status],  # noqa: S108  # test fixture path, not a real file
    )


def test_resource_schema_omits_pdf_view_when_view_missing(
    schema_catalog_db: Database,
) -> None:
    """PDF import_log row exists but no view materialized — schema doc omits entry."""
    _insert_pdf_import(schema_catalog_db, import_id="p1", alias="no_view_yet")
    data = json.loads(resource_schema())
    names = {t["name"] for t in data["tables"]}
    assert "raw.pdf_no_view_yet" not in names


def test_resource_schema_includes_pdf_view_after_import(
    schema_catalog_db: Database,
) -> None:
    """Materialized pdf_* view appears in the schema doc with its columns."""
    _insert_pdf_import(schema_catalog_db, import_id="p2", alias="my_statement")
    schema_catalog_db.execute(
        "CREATE OR REPLACE VIEW raw.pdf_my_statement AS "
        "SELECT CAST(NULL AS DATE) AS date, "
        "CAST(NULL AS DECIMAL(18,2)) AS amount, "
        "CAST(NULL AS VARCHAR) AS description "
        "WHERE FALSE"
    )
    data = json.loads(resource_schema())
    entry = next(
        (t for t in data["tables"] if t["name"] == "raw.pdf_my_statement"), None
    )
    assert entry is not None, "pdf view should appear once materialized"
    col_names = {c["name"] for c in entry["columns"]}
    assert {"date", "amount", "description"} <= col_names
    assert "alias=my_statement" in entry["purpose"]


def test_resource_schema_omits_failed_pdf_import(
    schema_catalog_db: Database,
) -> None:
    """Failed PDF imports do not surface even if a view somehow exists."""
    _insert_pdf_import(
        schema_catalog_db, import_id="p3", alias="bad_pdf", status="failed"
    )
    schema_catalog_db.execute(
        "CREATE OR REPLACE VIEW raw.pdf_bad_pdf AS SELECT 1 AS x WHERE FALSE"
    )
    data = json.loads(resource_schema())
    names = {t["name"] for t in data["tables"]}
    assert "raw.pdf_bad_pdf" not in names
