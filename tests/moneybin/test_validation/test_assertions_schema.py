"""Tests for schema and row-count assertion primitives."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.schema import (
    assert_column_types,
    assert_columns_exist,
    assert_row_count_delta,
    assert_row_count_exact,
)


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """Provide a test Database with a simple id/name table."""
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    database.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
    database.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    return database


def test_columns_exist_passes(db: Database) -> None:
    """All listed columns exist in the table."""
    r = assert_columns_exist(db, table="t", columns=["id", "name"])
    assert r.passed


def test_columns_exist_fails_when_missing(db: Database) -> None:
    """Missing column is reported in details."""
    r = assert_columns_exist(db, table="t", columns=["id", "missing"])
    assert not r.passed
    assert "missing" in r.details["missing"]


def test_column_types_match(db: Database) -> None:
    """Column types match the expected mapping."""
    r = assert_column_types(db, table="t", types={"id": "INTEGER", "name": "VARCHAR"})
    assert r.passed


def test_row_count_exact(db: Database) -> None:
    """Exact row count passes for correct value and fails otherwise."""
    assert assert_row_count_exact(db, table="t", expected=3).passed
    assert not assert_row_count_exact(db, table="t", expected=2).passed


def test_row_count_delta_within_tolerance(db: Database) -> None:
    """Delta check passes within tolerance and fails beyond it."""
    r = assert_row_count_delta(db, table="t", expected=3, tolerance_pct=10)
    assert r.passed
    r2 = assert_row_count_delta(db, table="t", expected=10, tolerance_pct=10)
    assert not r2.passed
    assert r2.details["delta_pct"] < -50
