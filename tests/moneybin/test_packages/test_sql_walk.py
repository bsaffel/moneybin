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
    """Temporary CREATE statements are skipped (ephemeral, never persist)."""
    sql = "CREATE TEMP TABLE scratch AS SELECT 1 AS x;"
    sql_file = tmp_path / "temp.sql"
    sql_file.write_text(sql)

    targets = extract_create_targets(sql_file)
    assert targets == []


def test_extract_create_targets_resolves_unqualified_to_main(tmp_path: Path) -> None:
    """An unqualified persistent CREATE resolves to main.<name>, not skipped.

    Returning ('main', name) lets the capability/prefix validators flag it — an
    unqualified write would land in DuckDB's default schema, escaping the
    package's declared globs.
    """
    sql = "CREATE TABLE scratch (x INT);"
    sql_file = tmp_path / "unqualified.sql"
    sql_file.write_text(sql)

    targets = extract_create_targets(sql_file)
    assert targets == [("main", "scratch")]


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


def test_iter_table_refs_ignores_unqualified_refs(tmp_path: Path) -> None:
    """Unqualified table references (no schema) are skipped."""
    sql = "SELECT * FROM scratch JOIN core.fct_transactions t ON t.id = scratch.id;"
    sql_file = tmp_path / "unq.sql"
    sql_file.write_text(sql)

    refs = list(iter_table_refs(sql_file))

    assert ("core", "fct_transactions") in refs
    assert not any(name == "scratch" for _, name in refs)


def test_iter_table_refs_with_cte(tmp_path: Path) -> None:
    """CTE aliases are not yielded as schema-qualified refs."""
    sql = """
    WITH summary AS (SELECT id, amount FROM core.fct_transactions)
    SELECT s.*
    FROM summary s
    JOIN app.test_state st ON st.id = s.id;
    """
    sql_file = tmp_path / "cte.sql"
    sql_file.write_text(sql)

    refs = list(iter_table_refs(sql_file))

    assert ("core", "fct_transactions") in refs
    assert ("app", "test_state") in refs
    assert not any(name == "summary" for _, name in refs)


def test_extract_create_targets_raises_on_unparseable(tmp_path: Path) -> None:
    """Malformed SQL surfaces a precise error, not a sqlglot internal."""
    sql = "CREATE TABLE oops syntax error;;;"
    sql_file = tmp_path / "bad.sql"
    sql_file.write_text(sql)

    # sqlglot falls back to a Command node (rather than raising) for this input;
    # _parse must detect the Command and raise ValueError itself.
    with pytest.raises(ValueError, match="failed to parse"):
        extract_create_targets(sql_file)


def test_extract_create_targets_normalizes_case(tmp_path: Path) -> None:
    """Unquoted identifiers are normalized to lowercase (DuckDB semantics)."""
    sql = "CREATE TABLE App.Assets_State (id TEXT);"
    sql_file = tmp_path / "case.sql"
    sql_file.write_text(sql)

    targets = extract_create_targets(sql_file)

    assert ("app", "assets_state") in targets


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE app.foo LIKE core.fct_transactions;",
        "CREATE TABLE app.foo (id TEXT REFERENCES core.fct_transactions(id));",
        "CREATE TABLE app.foo AS SELECT * FROM core.fct_transactions;",
    ],
)
def test_extract_create_targets_scopes_to_the_create_target(
    tmp_path: Path, sql: str
) -> None:
    """A referenced table (LIKE / FK / AS SELECT) is never mistaken for the target.

    extract_create_targets resolves the target from statement.this rather than
    the first Table in DFS order, so a referenced table cannot slip in and get
    capability/prefix-validated as if the package wrote it.
    """
    sql_file = tmp_path / "ref.sql"
    sql_file.write_text(sql)

    targets = extract_create_targets(sql_file)

    assert targets == [("app", "foo")]
    assert ("core", "fct_transactions") not in targets


def test_iter_table_refs_normalizes_case(tmp_path: Path) -> None:
    """Unquoted table references are normalized to lowercase (DuckDB semantics)."""
    sql = "SELECT * FROM Core.FCT_Transactions JOIN App.Test_State ON 1=1;"
    sql_file = tmp_path / "case_refs.sql"
    sql_file.write_text(sql)

    refs = list(iter_table_refs(sql_file))

    assert ("core", "fct_transactions") in refs
    assert ("app", "test_state") in refs


def test_iter_table_refs_excludes_create_target(tmp_path: Path) -> None:
    """The CREATE target itself is not yielded as a read dependency."""
    sql = """
    CREATE OR REPLACE VIEW reports.assets_summary AS
    SELECT * FROM core.fct_transactions;
    """
    sql_file = tmp_path / "view.sql"
    sql_file.write_text(sql)

    refs = list(iter_table_refs(sql_file))

    assert ("core", "fct_transactions") in refs
    assert ("reports", "assets_summary") not in refs
