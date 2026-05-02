"""Tests for distributional and cardinality assertion primitives."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.distribution import (
    assert_distribution_within_bounds,
    assert_ground_truth_coverage,
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


@pytest.fixture()
def coverage_db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """Provide a test Database with the three tables ground_truth_coverage joins."""
    database = Database(
        tmp_path / "coverage.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    database.execute("CREATE SCHEMA IF NOT EXISTS core")
    database.execute("CREATE SCHEMA IF NOT EXISTS prep")
    database.execute("CREATE SCHEMA IF NOT EXISTS synthetic")
    database.execute("CREATE TABLE core.fct_transactions (transaction_id VARCHAR)")
    database.execute(
        "CREATE TABLE prep.int_transactions__matched "
        "(transaction_id VARCHAR, source_transaction_id VARCHAR)"
    )
    database.execute(
        "CREATE TABLE synthetic.ground_truth "
        "(source_transaction_id VARCHAR, expected_category VARCHAR)"
    )
    return database


def test_ground_truth_coverage_passes_when_threshold_met(
    coverage_db: Database,
) -> None:
    """Full coverage clears the 0.9 floor."""
    coverage_db.execute(
        "INSERT INTO core.fct_transactions VALUES ('T1'),('T2'),('T3'),('T4'),('T5')"  # noqa: S608  # test input, not executing SQL
    )
    coverage_db.execute(
        "INSERT INTO prep.int_transactions__matched VALUES "
        "('T1','S1'),('T2','S2'),('T3','S3'),('T4','S4'),('T5','S5')"  # noqa: S608  # test input, not executing SQL
    )
    coverage_db.execute(
        "INSERT INTO synthetic.ground_truth VALUES "
        "('S1','grocery'),('S2','grocery'),('S3','grocery'),"
        "('S4','grocery'),('S5','grocery')"  # noqa: S608  # test input, not executing SQL
    )
    r = assert_ground_truth_coverage(coverage_db, min_coverage=0.9)
    assert r.passed, r.details
    assert r.details["coverage"] == 1.0


def test_ground_truth_coverage_fails_below_threshold(coverage_db: Database) -> None:
    """Half-labeled rows should fail a 0.9 threshold and report coverage=0.5."""
    coverage_db.execute(
        "INSERT INTO core.fct_transactions VALUES ('T1'),('T2'),('T3'),('T4')"  # noqa: S608  # test input, not executing SQL
    )
    coverage_db.execute(
        "INSERT INTO prep.int_transactions__matched VALUES "
        "('T1','S1'),('T2','S2'),('T3','S3'),('T4','S4')"  # noqa: S608  # test input, not executing SQL
    )
    coverage_db.execute(
        "INSERT INTO synthetic.ground_truth VALUES ('S1','grocery'),('S2','grocery')"  # noqa: S608  # test input, not executing SQL
    )
    r = assert_ground_truth_coverage(coverage_db, min_coverage=0.9)
    assert not r.passed
    assert r.details["coverage"] == 0.5


def test_ground_truth_coverage_validates_min_coverage_range(
    coverage_db: Database,
) -> None:
    """Out-of-range min_coverage raises ValueError."""
    with pytest.raises(ValueError, match=r"min_coverage"):
        assert_ground_truth_coverage(coverage_db, min_coverage=1.5)
