"""Tests for business-rule assertion primitives."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from tests.validation.assertions.domain import (
    assert_amount_precision,
    assert_date_bounds,
    assert_date_continuity,
)


@pytest.fixture()
def continuity_db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """Provide a test Database with a simple date/account table."""
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    database.execute("CREATE TABLE txns (account_id VARCHAR, transaction_date DATE)")
    return database


def test_date_continuity_passes_with_no_gaps(continuity_db: Database) -> None:
    """All months populated for each account → passes."""
    continuity_db.execute(
        "INSERT INTO txns VALUES "
        "('a', '2024-01-15'), ('a', '2024-02-10'), ('a', '2024-03-05'),"
        "('b', '2024-06-01'), ('b', '2024-07-12')"
    )
    r = assert_date_continuity(
        continuity_db,
        table="txns",
        date_col="transaction_date",
        account_col="account_id",
    )
    assert r.passed, r.details


def test_date_continuity_flags_missing_month(continuity_db: Database) -> None:
    """A month-gap on one account fails with that account in the details."""
    continuity_db.execute(
        "INSERT INTO txns VALUES "
        "('a', '2024-01-15'), ('a', '2024-03-05')"  # February missing
    )
    r = assert_date_continuity(
        continuity_db,
        table="txns",
        date_col="transaction_date",
        account_col="account_id",
    )
    assert not r.passed
    assert r.details["gap_count"] == 1


def test_date_continuity_single_month_passes(continuity_db: Database) -> None:
    """Account with only one month observed has no gap to detect."""
    continuity_db.execute(
        "INSERT INTO txns VALUES ('a', '2024-01-15'), ('a', '2024-01-22')"
    )
    assert assert_date_continuity(
        continuity_db,
        table="txns",
        date_col="transaction_date",
        account_col="account_id",
    ).passed


def test_date_continuity_flags_account_with_all_null_dates(
    continuity_db: Database,
) -> None:
    """Account whose dates are all NULL produces NULL bounds — must not silently pass."""
    continuity_db.execute("INSERT INTO txns VALUES ('a', NULL), ('a', NULL)")
    r = assert_date_continuity(
        continuity_db,
        table="txns",
        date_col="transaction_date",
        account_col="account_id",
    )
    assert not r.passed
    assert r.details["gap_count"] == 1


def test_amount_precision_passes_for_decimal_18_2_column(db: Database) -> None:
    db.execute("CREATE TABLE t (amount DECIMAL(18,2))")
    db.execute("INSERT INTO t VALUES (47.99), (-1500.00), (0.01)")
    r = assert_amount_precision(db, table="t", column="amount", precision=18, scale=2)
    assert r.passed, r.details


def test_amount_precision_fails_when_column_is_double(db: Database) -> None:
    db.execute("CREATE TABLE t (amount DOUBLE)")
    db.execute("INSERT INTO t VALUES (47.99)")
    r = assert_amount_precision(db, table="t", column="amount", precision=18, scale=2)
    assert not r.passed
    assert "DOUBLE" in r.details["actual_type"]


def test_amount_precision_fails_when_scale_too_small(db: Database) -> None:
    db.execute("CREATE TABLE t (amount DECIMAL(18,1))")
    r = assert_amount_precision(db, table="t", column="amount", precision=18, scale=2)
    assert not r.passed


def test_date_bounds_passes_when_all_in_range(db: Database) -> None:
    db.execute("CREATE TABLE t (d DATE)")
    db.execute("INSERT INTO t VALUES ('2024-01-01'), ('2024-06-15'), ('2024-12-31')")
    r = assert_date_bounds(
        db,
        table="t",
        column="d",
        min_date=date(2024, 1, 1),
        max_date=date(2024, 12, 31),
    )
    assert r.passed, r.details


def test_date_bounds_fails_below_min(db: Database) -> None:
    db.execute("CREATE TABLE t (d DATE)")
    db.execute("INSERT INTO t VALUES ('2023-12-31'), ('2024-06-15')")
    r = assert_date_bounds(
        db,
        table="t",
        column="d",
        min_date=date(2024, 1, 1),
        max_date=date(2024, 12, 31),
    )
    assert not r.passed
    assert r.details["below_min_count"] == 1


def test_date_bounds_fails_above_max(db: Database) -> None:
    db.execute("CREATE TABLE t (d DATE)")
    db.execute("INSERT INTO t VALUES ('2024-06-15'), ('2025-01-01')")
    r = assert_date_bounds(
        db,
        table="t",
        column="d",
        min_date=date(2024, 1, 1),
        max_date=date(2024, 12, 31),
    )
    assert not r.passed
    assert r.details["above_max_count"] == 1


def test_date_bounds_fails_when_nulls_present(db: Database) -> None:
    """NULL values count as out-of-bounds — they cannot be verified in window."""
    db.execute("CREATE TABLE t (d DATE)")
    db.execute("INSERT INTO t VALUES ('2024-06-15'), (NULL)")
    r = assert_date_bounds(
        db,
        table="t",
        column="d",
        min_date=date(2024, 1, 1),
        max_date=date(2024, 12, 31),
    )
    assert not r.passed
    assert r.details["null_count"] == 1


def test_date_bounds_accepts_iso_string_bounds(db: Database) -> None:
    """YAML scenarios pass quoted dates as strings — coerce at function entry."""
    db.execute("CREATE TABLE t (d DATE)")
    db.execute("INSERT INTO t VALUES ('2024-06-15')")
    r = assert_date_bounds(
        db,
        table="t",
        column="d",
        min_date="2024-01-01",
        max_date="2024-12-31",
    )
    assert r.passed, r.details


def test_date_bounds_passes_for_empty_table(db: Database) -> None:
    db.execute("CREATE TABLE t (d DATE)")
    r = assert_date_bounds(
        db,
        table="t",
        column="d",
        min_date=date(2024, 1, 1),
        max_date=date(2024, 12, 31),
    )
    assert r.passed


def test_date_continuity_year_boundary_passes(continuity_db: Database) -> None:
    """Dec→Jan across years is a contiguous span, not a gap."""
    continuity_db.execute(
        "INSERT INTO txns VALUES ('a', '2023-12-15'), ('a', '2024-01-22')"
    )
    assert assert_date_continuity(
        continuity_db,
        table="txns",
        date_col="transaction_date",
        account_col="account_id",
    ).passed
