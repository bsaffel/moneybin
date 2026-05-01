"""Tests for referential-integrity assertion primitives."""

import duckdb

from moneybin.validation.assertions.integrity import (
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


def test_no_orphans_passes_when_every_parent_has_child() -> None:
    """Every parent row has at least one matching child row."""
    c = _conn()
    c.execute("INSERT INTO child VALUES (1, 1), (2, 2), (3, 3)")
    r = assert_no_orphans(
        c, parent="parent", parent_column="id", child="child", child_column="parent_id"
    )
    assert r.passed
