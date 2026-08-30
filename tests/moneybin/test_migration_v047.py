"""V047: add raw.ofx_transactions.fitid_repaired, FALSE for every existing row.

The migration touches existing data (ADD COLUMN with a DEFAULT), so per
``.claude/rules/database.md`` it is exercised against a populated table rather
than an empty one. The fixture seeds marked and unmarked ids side by side
because the load-bearing property is what the column says about rows that
predate it — an empty table proves nothing about that.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V047__add_fitid_repaired_to_ofx_transactions import (
    migrate,
)
from tests.moneybin.migration_helpers import column_exists, run_migration


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


def test_v047_does_not_infer_provenance_from_the_marker(
    pre_v047_db: Database,
) -> None:
    """A pre-existing '#' is not evidence, so it must not arrive as proof.

    Nothing at this layer can recover what wrote an id predating the column, and
    the marker is exactly the unsound inference the flag replaces: the OFX spec
    does not reserve '#', so `X#reference` may be a bank's own id for a distinct
    transaction. Backfilling from it would let the migration hand that inference
    a permanent licence to delete `X`.

    The accepted cost is the other direction: a database that already imported a
    collision keeps showing that double-count until a re-import writes the real
    value. Visible in a total and correctable, where the alternative is not.
    """
    run_migration(pre_v047_db, migrate)

    assert _flag(pre_v047_db, "X#aaaa1111") is False
    assert _flag(pre_v047_db, "X#bbbb2222") is False


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
    assert _flag(pre_v047_db, "X#aaaa1111") is False
