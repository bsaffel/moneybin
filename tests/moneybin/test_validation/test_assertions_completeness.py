"""Tests for completeness assertion primitives."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.completeness import assert_no_nulls


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """Provide a test Database with a txn table."""
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    database.execute("CREATE TABLE txn (id INT, amount DECIMAL(18,2), note VARCHAR)")
    return database


def test_no_nulls_passes_when_no_nulls_present(db: Database) -> None:
    """All rows populated — assertion passes."""
    db.execute("INSERT INTO txn VALUES (1, 10.00, 'a'), (2, 20.00, 'b')")
    r = assert_no_nulls(db, table="txn", columns=["amount"])
    assert r.passed
    assert r.details["total"] == 0


def test_no_nulls_fails_when_null_present(db: Database) -> None:
    """Null value in checked column is counted as a violation."""
    db.execute("INSERT INTO txn VALUES (1, NULL, 'a'), (2, 20.00, 'b')")
    r = assert_no_nulls(db, table="txn", columns=["amount"])
    assert not r.passed
    assert r.details["null_counts"]["amount"] == 1
    assert r.details["total"] == 1


def test_no_nulls_checks_multiple_columns(db: Database) -> None:
    """Null counts are accumulated across all checked columns."""
    db.execute("INSERT INTO txn VALUES (1, NULL, NULL), (2, 20.00, 'b')")
    r = assert_no_nulls(db, table="txn", columns=["amount", "note"])
    assert not r.passed
    assert r.details["null_counts"]["amount"] == 1
    assert r.details["null_counts"]["note"] == 1
    assert r.details["total"] == 2


def test_no_nulls_raises_on_empty_columns(db: Database) -> None:
    """Empty column list raises ValueError."""
    with pytest.raises(ValueError):
        assert_no_nulls(db, table="txn", columns=[])
