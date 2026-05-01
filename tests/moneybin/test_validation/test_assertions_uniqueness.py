"""Tests for uniqueness assertion primitives."""

import duckdb

from moneybin.validation.assertions.uniqueness import assert_no_duplicates


def _conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE parent (id INT)")
    c.execute("INSERT INTO parent VALUES (1), (2), (3)")
    c.execute("CREATE TABLE child (id INT, parent_id INT)")
    return c


def test_no_duplicates_detects_repeats() -> None:
    """Duplicate rows in a column set are detected."""
    c = _conn()
    c.execute("INSERT INTO child VALUES (10, 1), (10, 1)")
    r = assert_no_duplicates(c, table="child", columns=["id"])
    assert not r.passed
    assert r.details["duplicate_groups"] == 1
