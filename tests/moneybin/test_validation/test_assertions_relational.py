"""Tests for relational integrity assertion primitives."""

import duckdb

from moneybin.validation.assertions.relational import (
    assert_no_duplicates,
    assert_no_nulls,
    assert_no_orphans,
    assert_valid_foreign_keys,
)


def _conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE parent (id INT)")
    c.execute("INSERT INTO parent VALUES (1), (2), (3)")
    c.execute("CREATE TABLE child (id INT, parent_id INT)")
    return c


def test_valid_foreign_keys_passes_when_all_children_resolve() -> None:
    """All child rows reference existing parent rows."""
    c = _conn()
    c.execute("INSERT INTO child VALUES (10, 1), (11, 2)")
    r = assert_valid_foreign_keys(
        c, child="child", column="parent_id", parent="parent", parent_column="id"
    )
    assert r.passed
    assert r.details == {"checked_rows": 2, "violations": 0}


def test_valid_foreign_keys_fails_with_violation_count() -> None:
    """Child row with missing parent is counted as a violation."""
    c = _conn()
    c.execute("INSERT INTO child VALUES (10, 1), (11, 99)")
    r = assert_valid_foreign_keys(
        c, child="child", column="parent_id", parent="parent", parent_column="id"
    )
    assert not r.passed
    assert r.details["violations"] == 1


def test_no_duplicates_detects_repeats() -> None:
    """Duplicate rows in a column set are detected."""
    c = _conn()
    c.execute("INSERT INTO child VALUES (10, 1), (10, 1)")
    r = assert_no_duplicates(c, table="child", columns=["id"])
    assert not r.passed
    assert r.details["duplicate_groups"] == 1


def test_no_orphans_passes_when_every_parent_has_child() -> None:
    """Every parent row has at least one matching child row."""
    c = _conn()
    c.execute("INSERT INTO child VALUES (1, 1), (2, 2), (3, 3)")
    r = assert_no_orphans(
        c, parent="parent", parent_column="id", child="child", child_column="parent_id"
    )
    assert r.passed


def test_no_nulls_passes_when_all_columns_populated() -> None:
    """Populated table with no null values in checked columns passes."""
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE t (a INT, b INT)")
    c.execute("INSERT INTO t VALUES (1, 10), (2, 20)")
    r = assert_no_nulls(c, table="t", columns=["a", "b"])
    assert r.passed
    assert r.details == {"null_counts": {"a": 0, "b": 0}, "total": 0}


def test_no_nulls_fails_with_per_column_counts() -> None:
    """Each column's null count is reported when the assertion fails."""
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE t (a INT, b INT)")
    c.execute("INSERT INTO t VALUES (1, NULL), (NULL, 20), (NULL, NULL)")
    r = assert_no_nulls(c, table="t", columns=["a", "b"])
    assert not r.passed
    assert r.details == {"null_counts": {"a": 2, "b": 2}, "total": 4}


def test_no_nulls_passes_on_empty_table() -> None:
    """Empty table reports zero counts (exercises the SUM-returns-NULL path)."""
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE t (a INT, b INT)")
    r = assert_no_nulls(c, table="t", columns=["a", "b"])
    assert r.passed
    assert r.details == {"null_counts": {"a": 0, "b": 0}, "total": 0}
