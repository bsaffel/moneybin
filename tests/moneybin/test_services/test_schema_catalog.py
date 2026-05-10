"""Tests for the schema catalog service."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.services.schema_catalog import (
    CONVENTIONS,
    EXAMPLES,
    Example,
    build_schema_doc,
)
from moneybin.tables import INTERFACE_TABLES

pytestmark = pytest.mark.unit


def test_conventions_has_required_keys() -> None:
    """CONVENTIONS must define exactly the four canonical keys."""
    assert set(CONVENTIONS.keys()) == {
        "amount_sign",
        "currency",
        "dates",
        "ids",
    }


def test_example_dataclass_shape() -> None:
    """Example is a frozen dataclass with question and sql fields."""
    ex = Example(question="q?", sql="SELECT 1")
    assert ex.question == "q?"
    assert ex.sql == "SELECT 1"


def test_examples_only_reference_interface_tables() -> None:
    """Every key in EXAMPLES must be a known interface table."""
    interface_names = {t.full_name for t in INTERFACE_TABLES}
    for table_name in EXAMPLES.keys():
        assert table_name in interface_names, (
            f"EXAMPLES key {table_name!r} is not an interface table"
        )


def test_every_interface_table_has_at_least_one_example() -> None:
    """Every interface table must have at least one entry in EXAMPLES."""
    interface_names = {t.full_name for t in INTERFACE_TABLES}
    missing = interface_names - set(EXAMPLES.keys())
    assert not missing, f"Interface tables missing examples: {sorted(missing)}"


def _present_tables(db: Database) -> set[str]:
    """Return fully-qualified names of all tables and views in the test DB."""
    rows = db.execute(
        "SELECT schema_name || '.' || table_name FROM duckdb_tables() "
        "UNION ALL "
        "SELECT schema_name || '.' || view_name FROM duckdb_views() "
        "WHERE NOT internal"
    ).fetchall()
    return {r[0] for r in rows}


def test_build_schema_doc_top_level_keys(schema_catalog_db: Database) -> None:
    """The returned dict must have all expected top-level keys with correct types."""
    doc = build_schema_doc()
    assert doc["version"] == 1
    assert "generated_at" in doc
    assert doc["conventions"]["amount_sign"].startswith("negative")
    assert isinstance(doc["tables"], list)
    assert "beyond_the_interface" in doc
    assert "catalog_query" in doc["beyond_the_interface"]


def test_build_schema_doc_includes_present_interface_tables(
    schema_catalog_db: Database,
) -> None:
    """Core interface tables present in the DB must appear in the output."""
    doc = build_schema_doc()
    names = {t["name"] for t in doc["tables"]}
    # The test DB only creates core.* via create_core_tables_raw; app tables
    # are absent, so build_schema_doc should silently skip them rather than
    # error. Core interface tables must be present.
    assert "core.fct_transactions" in names
    assert "core.dim_accounts" in names


def test_build_schema_doc_includes_interface_views(
    schema_catalog_db: Database,
) -> None:
    """Interface objects that are views (not tables) must appear too.

    Regression test: `duckdb_tables()` excludes views, so the catalog
    query must union it with `duckdb_views()` to surface objects like
    `core.dim_categories` (a SQLMesh-managed view; stubbed in the fixture
    for tests).
    """
    doc = build_schema_doc()
    names = {t["name"] for t in doc["tables"]}
    assert "core.dim_categories" in names


def test_beyond_the_interface_query_executes(
    schema_catalog_db: Database,
) -> None:
    """The catalog query in the footer must run via the same connection.

    Regression test: previously used `table_schema` (an information_schema
    column), which fails on `duckdb_tables()`. The query must use
    `schema_name` so power users / LLMs copying it can run it directly.
    """
    doc = build_schema_doc()
    query = doc["beyond_the_interface"]["catalog_query"]
    schema_catalog_db.execute(query).fetchall()


def test_build_schema_doc_columns_carry_type_and_comment(
    schema_catalog_db: Database,
) -> None:
    """Each column entry must include data_type and the applied comment."""
    doc = build_schema_doc()
    fct = next(t for t in doc["tables"] if t["name"] == "core.fct_transactions")
    cols_by_name = {c["name"]: c for c in fct["columns"]}
    assert "amount" in cols_by_name
    assert "DECIMAL" in cols_by_name["amount"]["type"].upper()
    assert "negative" in cols_by_name["amount"]["comment"].lower()


def test_build_schema_doc_includes_examples_for_present_tables(
    schema_catalog_db: Database,
) -> None:
    """Each table entry must carry at least one example with question and sql."""
    doc = build_schema_doc()
    fct = next(t for t in doc["tables"] if t["name"] == "core.fct_transactions")
    assert len(fct["examples"]) >= 1
    first = fct["examples"][0]
    assert "question" in first
    assert "sql" in first


def test_interface_tables_present_in_catalog(schema_catalog_db: Database) -> None:
    """Stale-entry drift: every interface-tagged table must exist in the DB.

    Catches removals or renames of any INTERFACE_TABLES entry — including the
    six app.* interface tables (categories, budgets, merchants,
    categorization_rules, transaction_categories, transaction_notes), which
    were previously skipped because the fixture did not seed them.
    """
    present = _present_tables(schema_catalog_db)
    missing = [t.full_name for t in INTERFACE_TABLES if t.full_name not in present]
    assert not missing, f"INTERFACE_TABLES entries missing from test DB: {missing}"


def test_examples_parse_and_execute(schema_catalog_db: Database) -> None:
    """Examples must parse and execute against the live schema.

    Catches column-renamed-but-example-not-updated drift. Now exercises
    examples for app.* interface tables (previously skipped when the
    fixture did not seed them).
    """
    for examples in EXAMPLES.values():
        for ex in examples:
            schema_catalog_db.execute(ex.sql).fetchall()
