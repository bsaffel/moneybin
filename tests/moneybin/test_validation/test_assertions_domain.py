"""Tests for business-rule assertion primitives."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.validation.assertions.domain import (
    assert_balanced_transfers,
    assert_date_continuity,
    assert_sign_convention,
)


@pytest.fixture()
def continuity_db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """Provide a test Database with a simple date/account table."""
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    database.execute("CREATE TABLE txns (account_id VARCHAR, transaction_date DATE)")
    return database


@pytest.fixture()
def txn_db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """Provide a test Database with core.fct_transactions."""
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    database.execute("CREATE SCHEMA IF NOT EXISTS core")
    database.execute(
        "CREATE TABLE IF NOT EXISTS core.fct_transactions ("
        " transaction_id VARCHAR, amount DECIMAL(18,2),"
        " category VARCHAR, transfer_pair_id VARCHAR,"
        " is_transfer BOOLEAN DEFAULT FALSE)"
    )
    return database


def test_sign_convention_passes_when_categories_match_signs(txn_db: Database) -> None:
    """Expenses with negative amounts and income with positive amounts pass."""
    txn_db.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -50.00, 'Groceries', NULL, FALSE),"
        "('t2', 1000.00, 'Income', NULL, FALSE)"
    )
    assert assert_sign_convention(txn_db).passed


def test_sign_convention_flags_positive_expense(txn_db: Database) -> None:
    """A positive-amount expense category counts as a violation."""
    txn_db.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', 50.00, 'Groceries', NULL, FALSE)"
    )
    r = assert_sign_convention(txn_db)
    assert not r.passed
    assert r.details["violations"] == 1


def test_sign_convention_exempts_transfers_via_is_transfer_flag(
    txn_db: Database,
) -> None:
    """Rows with ``is_transfer = TRUE`` are exempt from sign rules even if positive."""
    txn_db.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', 100.00, NULL, 'p1', TRUE),"  # incoming leg, NULL category
        "('t2', -100.00, NULL, 'p1', TRUE)"
    )
    assert assert_sign_convention(txn_db).passed


def test_balanced_transfers_pairs_net_to_zero(txn_db: Database) -> None:
    """Transfer pair with equal and opposite amounts passes."""
    txn_db.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -100.00, 'Transfer', 'p1', TRUE),"
        "('t2', 100.00, 'Transfer', 'p1', TRUE)"
    )
    assert assert_balanced_transfers(txn_db).passed


def test_balanced_transfers_flags_imbalance(txn_db: Database) -> None:
    """Transfer pair that does not net to zero is flagged as unbalanced."""
    txn_db.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -100.00, 'Transfer', 'p1', TRUE),"
        "('t2', 90.00, 'Transfer', 'p1', TRUE)"
    )
    r = assert_balanced_transfers(txn_db)
    assert not r.passed


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
