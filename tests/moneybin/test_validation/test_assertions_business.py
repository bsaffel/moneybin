"""Tests for business-rule assertion primitives."""

import duckdb

from moneybin.validation.assertions.business import (
    assert_balanced_transfers,
    assert_date_continuity,
    assert_sign_convention,
)


def _continuity_conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE TABLE txns (account_id VARCHAR, transaction_date DATE)")
    return c


def _txn_conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE SCHEMA core")
    c.execute(
        "CREATE TABLE core.fct_transactions ("
        " transaction_id VARCHAR, amount DECIMAL(18,2),"
        " category VARCHAR, transfer_pair_id VARCHAR,"
        " is_transfer BOOLEAN DEFAULT FALSE)"
    )
    return c


def test_sign_convention_passes_when_categories_match_signs() -> None:
    """Expenses with negative amounts and income with positive amounts pass."""
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -50.00, 'Groceries', NULL, FALSE),"
        "('t2', 1000.00, 'Income', NULL, FALSE)"
    )
    assert assert_sign_convention(c).passed


def test_sign_convention_flags_positive_expense() -> None:
    """A positive-amount expense category counts as a violation."""
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', 50.00, 'Groceries', NULL, FALSE)"
    )
    r = assert_sign_convention(c)
    assert not r.passed
    assert r.details["violations"] == 1


def test_sign_convention_exempts_transfers_via_is_transfer_flag() -> None:
    """Rows with ``is_transfer = TRUE`` are exempt from sign rules even if positive."""
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', 100.00, NULL, 'p1', TRUE),"  # incoming leg, NULL category
        "('t2', -100.00, NULL, 'p1', TRUE)"
    )
    assert assert_sign_convention(c).passed


def test_balanced_transfers_pairs_net_to_zero() -> None:
    """Transfer pair with equal and opposite amounts passes."""
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -100.00, 'Transfer', 'p1', TRUE),"
        "('t2', 100.00, 'Transfer', 'p1', TRUE)"
    )
    assert assert_balanced_transfers(c).passed


def test_balanced_transfers_flags_imbalance() -> None:
    """Transfer pair that does not net to zero is flagged as unbalanced."""
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -100.00, 'Transfer', 'p1', TRUE),"
        "('t2', 90.00, 'Transfer', 'p1', TRUE)"
    )
    r = assert_balanced_transfers(c)
    assert not r.passed


def test_balanced_transfers_flags_null_sum_as_violation() -> None:
    """Transfer pair whose amounts are all NULL nets to NULL — must not silently pass."""
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', NULL, 'Transfer', 'p1', TRUE),"
        "('t2', NULL, 'Transfer', 'p1', TRUE)"
    )
    r = assert_balanced_transfers(c)
    assert not r.passed
    assert r.details["unbalanced_count"] == 1


def test_date_continuity_passes_with_no_gaps() -> None:
    """All months populated for each account → passes."""
    c = _continuity_conn()
    c.execute(
        "INSERT INTO txns VALUES "
        "('a', '2024-01-15'), ('a', '2024-02-10'), ('a', '2024-03-05'),"
        "('b', '2024-06-01'), ('b', '2024-07-12')"
    )
    r = assert_date_continuity(
        c, table="txns", date_col="transaction_date", account_col="account_id"
    )
    assert r.passed, r.details


def test_date_continuity_flags_missing_month() -> None:
    """A month-gap on one account fails with that account in the details."""
    c = _continuity_conn()
    c.execute(
        "INSERT INTO txns VALUES "
        "('a', '2024-01-15'), ('a', '2024-03-05')"  # February missing
    )
    r = assert_date_continuity(
        c, table="txns", date_col="transaction_date", account_col="account_id"
    )
    assert not r.passed
    assert r.details["gap_count"] == 1


def test_date_continuity_single_month_passes() -> None:
    """Account with only one month observed has no gap to detect."""
    c = _continuity_conn()
    c.execute("INSERT INTO txns VALUES ('a', '2024-01-15'), ('a', '2024-01-22')")
    assert assert_date_continuity(
        c, table="txns", date_col="transaction_date", account_col="account_id"
    ).passed


def test_date_continuity_year_boundary_passes() -> None:
    """Dec→Jan across years is a contiguous span, not a gap."""
    c = _continuity_conn()
    c.execute("INSERT INTO txns VALUES ('a', '2023-12-15'), ('a', '2024-01-22')")
    assert assert_date_continuity(
        c, table="txns", date_col="transaction_date", account_col="account_id"
    ).passed


def test_date_continuity_flags_account_with_all_null_dates() -> None:
    """Account whose dates are all NULL produces NULL bounds — must not silently pass."""
    c = _continuity_conn()
    c.execute("INSERT INTO txns VALUES ('a', NULL), ('a', NULL)")
    r = assert_date_continuity(
        c, table="txns", date_col="transaction_date", account_col="account_id"
    )
    assert not r.passed
    assert r.details["gap_count"] == 1
