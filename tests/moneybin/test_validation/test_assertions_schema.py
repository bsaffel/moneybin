"""Tests for schema and row-count assertion primitives."""

import duckdb

from moneybin.validation.assertions.schema import (
    assert_column_types,
    assert_columns_exist,
    assert_row_count_delta,
    assert_row_count_exact,
)


def _conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
    c.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    return c


def test_columns_exist_passes() -> None:
    """All listed columns exist in the table."""
    r = assert_columns_exist(_conn(), table="t", columns=["id", "name"])
    assert r.passed


def test_columns_exist_fails_when_missing() -> None:
    """Missing column is reported in details."""
    r = assert_columns_exist(_conn(), table="t", columns=["id", "missing"])
    assert not r.passed
    assert "missing" in r.details["missing"]


def test_column_types_match() -> None:
    """Column types match the expected mapping."""
    r = assert_column_types(
        _conn(), table="t", types={"id": "INTEGER", "name": "VARCHAR"}
    )
    assert r.passed


def test_row_count_exact() -> None:
    """Exact row count passes for correct value and fails otherwise."""
    assert assert_row_count_exact(_conn(), table="t", expected=3).passed
    assert not assert_row_count_exact(_conn(), table="t", expected=2).passed


def test_row_count_delta_within_tolerance() -> None:
    """Delta check passes within tolerance and fails beyond it."""
    r = assert_row_count_delta(_conn(), table="t", expected=3, tolerance_pct=10)
    assert r.passed
    r2 = assert_row_count_delta(_conn(), table="t", expected=10, tolerance_pct=10)
    assert not r2.passed
    assert r2.details["delta_pct"] < -50
