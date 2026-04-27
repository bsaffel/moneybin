"""Tests for business-rule assertion primitives."""

import duckdb

from moneybin.validation.assertions.business import (
    assert_balanced_transfers,
    assert_sign_convention,
)


def _txn_conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    c.execute("CREATE SCHEMA core")
    c.execute(
        "CREATE TABLE core.fct_transactions ("
        " transaction_id VARCHAR, amount DECIMAL(18,2),"
        " category VARCHAR, transfer_pair_id VARCHAR)"
    )
    return c


def test_sign_convention_passes_when_categories_match_signs() -> None:
    """Expenses with negative amounts and income with positive amounts pass."""
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -50.00, 'Groceries', NULL),"
        "('t2', 1000.00, 'Income', NULL)"
    )
    assert assert_sign_convention(c).passed


def test_sign_convention_flags_positive_expense() -> None:
    """A positive-amount expense category counts as a violation."""
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES ('t1', 50.00, 'Groceries', NULL)"
    )
    r = assert_sign_convention(c)
    assert not r.passed
    assert r.details["violations"] == 1


def test_balanced_transfers_pairs_net_to_zero() -> None:
    """Transfer pair with equal and opposite amounts passes."""
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -100.00, 'Transfer', 'p1'),"
        "('t2', 100.00, 'Transfer', 'p1')"
    )
    assert assert_balanced_transfers(c).passed


def test_balanced_transfers_flags_imbalance() -> None:
    """Transfer pair that does not net to zero is flagged as unbalanced."""
    c = _txn_conn()
    c.execute(
        "INSERT INTO core.fct_transactions VALUES "
        "('t1', -100.00, 'Transfer', 'p1'),"
        "('t2', 90.00, 'Transfer', 'p1')"
    )
    r = assert_balanced_transfers(c)
    assert not r.passed
