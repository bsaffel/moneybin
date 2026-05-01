"""Tests for completeness assertion primitives."""

import duckdb

from moneybin.validation.assertions.completeness import assert_no_nulls


def _conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE txn (id INT, amount DECIMAL(18,2), note VARCHAR)")
    return c


def test_no_nulls_passes_when_no_nulls_present() -> None:
    """All rows populated — assertion passes."""
    c = _conn()
    c.execute("INSERT INTO txn VALUES (1, 10.00, 'a'), (2, 20.00, 'b')")
    r = assert_no_nulls(c, table="txn", columns=["amount"])
    assert r.passed
    assert r.details["total"] == 0


def test_no_nulls_fails_when_null_present() -> None:
    """Null value in checked column is counted as a violation."""
    c = _conn()
    c.execute("INSERT INTO txn VALUES (1, NULL, 'a'), (2, 20.00, 'b')")
    r = assert_no_nulls(c, table="txn", columns=["amount"])
    assert not r.passed
    assert r.details["null_counts"]["amount"] == 1
    assert r.details["total"] == 1


def test_no_nulls_checks_multiple_columns() -> None:
    """Null counts are accumulated across all checked columns."""
    c = _conn()
    c.execute("INSERT INTO txn VALUES (1, NULL, NULL), (2, 20.00, 'b')")
    r = assert_no_nulls(c, table="txn", columns=["amount", "note"])
    assert not r.passed
    assert r.details["null_counts"]["amount"] == 1
    assert r.details["null_counts"]["note"] == 1
    assert r.details["total"] == 2


def test_no_nulls_raises_on_empty_columns() -> None:
    """Empty column list raises ValueError."""
    c = _conn()
    try:
        assert_no_nulls(c, table="txn", columns=[])
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
