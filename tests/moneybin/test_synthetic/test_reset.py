"""Tests for the shared synthetic-reset helper."""

import pytest

from moneybin.database import Database
from moneybin.synthetic.reset import RESET_DELETIONS, reset_synthetic_rows

_INSERT = (
    "INSERT INTO raw.tabular_transactions "
    "(transaction_id, account_id, transaction_date, amount, "
    "source_file, source_type, source_origin, import_id) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)


@pytest.mark.unit
def test_reset_synthetic_rows_deletes_only_synthetic(db: Database) -> None:
    # One generator-created row (synthetic:// source_file) and one real import.
    db.execute(
        _INSERT,
        [
            "t1",
            "acct",
            "2025-01-01",
            "10.00",
            "synthetic://basic/42/csv",
            "csv",
            "syn",
            "imp1",
        ],
    )
    db.execute(
        _INSERT,
        ["t2", "acct", "2025-01-01", "20.00", "user-upload.csv", "csv", "user", "imp2"],
    )

    reset_synthetic_rows(db)

    rows = db.execute(
        "SELECT transaction_id FROM raw.tabular_transactions ORDER BY transaction_id"
    ).fetchall()
    assert rows == [("t2",)]


@pytest.mark.unit
def test_reset_deletions_allowlist_is_synthetic_scoped() -> None:
    # Every non-ground_truth deletion is scoped to synthetic:// source files, so
    # the helper can never touch a real user import.
    for table, where in RESET_DELETIONS.items():
        if table.endswith("ground_truth"):
            continue
        assert "synthetic://" in where, f"{table} deletion is not synthetic-scoped"
