"""Tests for distributional and cardinality assertion primitives."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.distribution import (
    assert_distribution_within_bounds,
    assert_unique_value_count,
)


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """Provide a test Database with an amount/category table."""
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    database.execute("CREATE TABLE t (amount DECIMAL(18,2), category VARCHAR)")
    database.execute(
        "INSERT INTO t VALUES (10, 'a'), (20, 'a'), (30, 'b'), (40, 'b'), (50, 'c')"
    )
    return database


def test_distribution_within_bounds_passes(db: Database) -> None:
    """Stats within all specified bounds returns passed=True."""
    r = assert_distribution_within_bounds(
        db,
        table="t",
        col="amount",
        min_value=10,
        max_value=50,
        mean_range=(25, 35),
    )
    assert r.passed


def test_distribution_out_of_range_fails(db: Database) -> None:
    """Observed max exceeding the specified ceiling returns passed=False."""
    r = assert_distribution_within_bounds(
        db,
        table="t",
        col="amount",
        min_value=10,
        max_value=49,
        mean_range=(25, 35),
    )
    assert not r.passed
    assert r.details["max_observed"] == 50


def test_unique_value_count_within_tolerance(db: Database) -> None:
    """Exact distinct count matching expected with zero tolerance passes."""
    r = assert_unique_value_count(
        db, table="t", col="category", expected=3, tolerance_pct=0
    )
    assert r.passed
