"""V047: add raw.ofx_transactions.fitid_repaired and backfill it.

The migration touches existing data (ADD COLUMN with a DEFAULT, then an UPDATE
backfill), so per ``.claude/rules/database.md`` it is exercised against a
populated table rather than an empty one — the backfill is the part that can go
wrong, and an empty table proves nothing about it.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V047__add_fitid_repaired_to_ofx_transactions import (
    migrate,
)
from tests.moneybin.migration_helpers import column_exists, run_migration

pytestmark = pytest.mark.fresh_db


def _insert_pre_migration_row(
    db: Database,
    *,
    fitid: str,
    source_file: str,
    amount: str = "-13.12",
    payee: str = "FOREIGN TRANSACTION FEE",
) -> None:
    """Seed a row shaped like one written before the column existed."""
    db.execute(
        """
        INSERT INTO raw.ofx_transactions (
            source_transaction_id, account_id, transaction_type, date_posted,
            amount, payee, memo, check_number, source_file, extracted_at,
            loaded_at, source_type, source_origin, currency_code
        ) VALUES (?, 'ACC1', 'DEBIT', TIMESTAMP '2026-01-15 00:00:00', ?, ?,
                  NULL, NULL, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'ofx',
                  'chase', 'USD')
        """,
        [fitid, Decimal(amount), payee, source_file],
    )


@pytest.fixture
def pre_v047_db(db: Database) -> Database:
    """A populated table without the column, so migrate() does real work."""
    db.execute("ALTER TABLE raw.ofx_transactions DROP COLUMN IF EXISTS fitid_repaired")
    _insert_pre_migration_row(db, fitid="PLAIN1", source_file="before.qfx")
    _insert_pre_migration_row(
        db, fitid="PLAIN2", source_file="before.qfx", amount="-1.00"
    )
    _insert_pre_migration_row(db, fitid="X#aaaa1111", source_file="after.qfx")
    _insert_pre_migration_row(
        db, fitid="X#bbbb2222", source_file="after.qfx", amount="-0.39"
    )
    return db


def _flag(db: Database, fitid: str) -> bool:
    row = db.execute(
        "SELECT fitid_repaired FROM raw.ofx_transactions "
        "WHERE source_transaction_id = ?",
        [fitid],
    ).fetchone()
    assert row is not None, f"{fitid} missing"
    return bool(row[0])


def test_v047_adds_the_column(pre_v047_db: Database) -> None:
    assert not column_exists(pre_v047_db, "raw", "ofx_transactions", "fitid_repaired")
    run_migration(pre_v047_db, migrate)
    assert column_exists(pre_v047_db, "raw", "ofx_transactions", "fitid_repaired")


def test_v047_backfills_marked_ids_to_preserve_existing_supersession(
    pre_v047_db: Database,
) -> None:
    """Rows already carrying the marker keep the behavior their install runs on.

    Nothing at this layer can recover the true provenance of an id written before
    the flag existed. Backfilling FALSE would strand the orphaned bare rows the
    supersession was built to retire, re-creating the double-count on every
    database that has already hit a collision.
    """
    run_migration(pre_v047_db, migrate)

    assert _flag(pre_v047_db, "X#aaaa1111") is True
    assert _flag(pre_v047_db, "X#bbbb2222") is True


def test_v047_leaves_unmarked_ids_alone(pre_v047_db: Database) -> None:
    """The flag licenses a delete, so an id the extractor never rewrote stays off."""
    run_migration(pre_v047_db, migrate)

    assert _flag(pre_v047_db, "PLAIN1") is False
    assert _flag(pre_v047_db, "PLAIN2") is False


def test_v047_is_idempotent(pre_v047_db: Database) -> None:
    run_migration(pre_v047_db, migrate)
    run_migration(pre_v047_db, migrate)

    row = pre_v047_db.execute("SELECT COUNT(*) FROM raw.ofx_transactions").fetchone()
    assert row is not None and row[0] == 4
    assert _flag(pre_v047_db, "PLAIN1") is False
    assert _flag(pre_v047_db, "X#aaaa1111") is True
