"""Tests for the sqlglot-based SQL inspection helpers.

The framework needs to enumerate every CREATE TABLE / CREATE VIEW target
in a package's SQL files (for capability validation) and check that table
references in tools follow the package's prefix (for prefix validation).
"""

from pathlib import Path

import pytest

from moneybin.packages._framework._sql_walk import (
    extract_create_targets,
    iter_table_refs,
)


def test_extract_create_targets_picks_up_tables_and_views(tmp_path: Path) -> None:
    """CREATE TABLE and CREATE VIEW both yield schema.name tuples."""
    sql = """
    CREATE TABLE IF NOT EXISTS app.test_synthetic_state (
        id TEXT PRIMARY KEY,
        value INTEGER NOT NULL
    );

    CREATE OR REPLACE VIEW reports.test_synthetic_summary AS
    SELECT id, value FROM app.test_synthetic_state;
    """
    sql_file = tmp_path / "test.sql"
    sql_file.write_text(sql)

    targets = extract_create_targets(sql_file)

    assert ("app", "test_synthetic_state") in targets
    assert ("reports", "test_synthetic_summary") in targets


def test_extract_create_targets_ignores_temp_tables(tmp_path: Path) -> None:
    """Temporary CREATE statements without schema qualifiers are skipped."""
    sql = "CREATE TEMP TABLE scratch AS SELECT 1 AS x;"
    sql_file = tmp_path / "temp.sql"
    sql_file.write_text(sql)

    targets = extract_create_targets(sql_file)
    assert targets == []


def test_iter_table_refs_returns_referenced_schemas(tmp_path: Path) -> None:
    """SELECT and JOIN targets surface for prefix validation."""
    sql = """
    CREATE OR REPLACE VIEW reports.test_synthetic_summary AS
    SELECT t.amount
    FROM core.fct_transactions t
    JOIN app.test_synthetic_state s ON s.id = t.id;
    """
    sql_file = tmp_path / "view.sql"
    sql_file.write_text(sql)

    refs = list(iter_table_refs(sql_file))

    assert ("core", "fct_transactions") in refs
    assert ("app", "test_synthetic_state") in refs


def test_extract_create_targets_raises_on_unparseable(tmp_path: Path) -> None:
    """Malformed SQL surfaces a precise error, not a sqlglot internal."""
    sql = "CREATE TABLE oops syntax error;;;"
    sql_file = tmp_path / "bad.sql"
    sql_file.write_text(sql)

    with pytest.raises(ValueError, match="failed to parse"):
        extract_create_targets(sql_file)
