"""V026: add predicted ``transaction_id`` to ``raw.manual_transactions`` + backfill.

Data-touching migration (ADD COLUMN + Python backfill of pre-existing rows
via the same hash ``_predict_manual_gold_key`` uses), so per
``.claude/rules/database.md`` migration-realism rules the fixture seeds
≥3 realistic rows with ``transaction_id = NULL`` (the pre-V026 state) and
asserts the backfill writes the expected deterministic hash for each.

V026 is a FROZEN migration: its ``_predict`` uses the ORIGINAL
``manual|source_transaction_id|account_id`` hash. The ADR-015/RD-2 re-key (B4)
later moved the live transaction_id formula to the immutable source identity
(``source_type|source_origin|source_account_key|source_transaction_id``). There
is no in-place re-key migration: existing dogfood data is re-imported into a
clean database, and fresh manual transactions are written with the new formula
by ``transaction_service._predict_manual_gold_key``. So V026 stays historical —
its backfill is inert on fresh data (no pre-V026 rows) — and must NOT track the
service formula; pinning its output here guards a shipped migration against
accidental change.
"""

# pyright: reportPrivateUsage=false
# This test deliberately imports module-private hash helpers from both
# transaction_service and the V026 migration so it can assert they stay in
# lockstep (the whole point of the test).

from __future__ import annotations

import hashlib
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V026__add_transaction_id_to_manual_transactions import (
    _predict as _migration_predict,
)
from moneybin.sql.migrations.V026__add_transaction_id_to_manual_transactions import (
    migrate,
)
from tests.moneybin.migration_helpers import column_exists, run_migration


def _expected_hash(source_transaction_id: str, account_id: str) -> str:
    """V026's frozen backfill hash: ``manual|source_transaction_id|account_id``.

    This is the ORIGINAL pre-ADR-015 formula. The live service + SQL model moved
    on (B4); V026's own output stays this (frozen shipped migration).
    """
    raw = f"manual|{source_transaction_id}|{account_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@pytest.mark.parametrize(
    ("source_transaction_id", "account_id"),
    [
        ("manual_aaaa11112222", "acct-chase-001"),
        ("manual_bbbb33334444", "acct-amex-002"),
        ("manual_cccc55556666", "acct-fido-003"),
        ("manual_xxxxx", "weird-acct/with-symbols-001"),
    ],
)
def test_migration_predict_uses_frozen_original_formula(
    source_transaction_id: str, account_id: str
) -> None:
    """V026's ``_predict`` stays the ORIGINAL ``manual|stid|account_id`` hash.

    V026 is a frozen migration — its backfill formula is historical and must NOT
    be silently retracked to the live transaction_id formula, which the ADR-015 /
    RD-2 re-key (B4) changed to the immutable source identity. Pinning V026's own
    output here guards against an accidental change to a shipped migration.
    """
    assert _migration_predict(source_transaction_id, account_id) == (
        _expected_hash(source_transaction_id, account_id)
    )


@pytest.fixture()
def pre_v026_db(db: Database) -> Database:
    """Three realistic raw.manual_transactions rows in the pre-V026 state.

    Auto-migration on the test fixture already added the V026 column, so
    we simulate the upgrade path by NULL'ing it on rows we seed via raw
    SQL — that's the state V026's backfill must repair.
    """
    rows = [
        (
            "manual_aaaa11112222",
            "acct-chase-001",
            "2025-11-01",
            Decimal("-42.50"),
            "STARBUCKS",
        ),
        (
            "manual_bbbb33334444",
            "acct-amex-002",
            "2025-11-05",
            Decimal("-128.00"),
            "AMAZON",
        ),
        (
            "manual_cccc55556666",
            "acct-fido-003",
            "2025-11-10",
            Decimal("2500.00"),
            "PAYROLL",
        ),
    ]
    # DuckDB doesn't enforce the import_id FK by default — skip the
    # raw.import_log row and reference the id directly. (V019's fixture
    # follows the same pattern for tabular_transactions.)
    for sid, acct, txn_date, amt, desc in rows:
        db.execute(
            "INSERT INTO raw.manual_transactions "  # noqa: S608  # test input, not user SQL
            "(source_transaction_id, import_id, account_id, transaction_date, "
            " amount, description, created_by, transaction_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
            [sid, "imp-v026", acct, txn_date, amt, desc, "cli"],
        )
    return db


class TestV026AddTransactionIdToManualTransactions:
    """V026 adds raw.manual_transactions.transaction_id and backfills it."""

    def test_column_exists_after_migration(self, pre_v026_db: Database) -> None:
        run_migration(pre_v026_db, migrate)
        assert column_exists(
            pre_v026_db, "raw", "manual_transactions", "transaction_id"
        )

    def test_column_is_nullable_varchar(self, pre_v026_db: Database) -> None:
        run_migration(pre_v026_db, migrate)
        row = pre_v026_db.execute(
            "SELECT data_type, is_nullable FROM duckdb_columns() "
            "WHERE schema_name = 'raw' "
            "AND table_name = 'manual_transactions' "
            "AND column_name = 'transaction_id'"
        ).fetchone()
        assert row is not None
        data_type, is_nullable = row
        assert data_type == "VARCHAR"
        assert bool(is_nullable) is True

    def test_backfill_populates_predicted_hash_for_each_row(
        self, pre_v026_db: Database
    ) -> None:
        """Every pre-existing row gets the deterministic predicted hash."""
        run_migration(pre_v026_db, migrate)
        rows = pre_v026_db.execute(
            "SELECT source_transaction_id, account_id, transaction_id "
            "FROM raw.manual_transactions "
            "ORDER BY source_transaction_id"
        ).fetchall()
        assert len(rows) == 3
        for source_txn_id, account_id, txn_id in rows:
            assert txn_id == _expected_hash(source_txn_id, account_id), (
                f"backfill produced unexpected hash for "
                f"({source_txn_id}, {account_id}): got {txn_id!r}"
            )

    def test_idempotent(self, pre_v026_db: Database) -> None:
        """Re-running on an already-migrated DB is harmless + leaves data intact."""
        run_migration(pre_v026_db, migrate)
        run_migration(pre_v026_db, migrate)
        rows = pre_v026_db.execute(
            "SELECT COUNT(*) FROM raw.manual_transactions WHERE transaction_id IS NOT NULL"
        ).fetchone()
        assert rows is not None
        assert rows[0] == 3

    def test_backfill_skips_already_populated_rows(self, pre_v026_db: Database) -> None:
        """Rows with a non-NULL transaction_id are left alone (no UPDATE)."""
        sentinel = "preexisting_hash"
        pre_v026_db.execute(
            "UPDATE raw.manual_transactions SET transaction_id = ? "
            "WHERE source_transaction_id = 'manual_aaaa11112222'",
            [sentinel],
        )
        run_migration(pre_v026_db, migrate)
        row = pre_v026_db.execute(
            "SELECT transaction_id FROM raw.manual_transactions "
            "WHERE source_transaction_id = 'manual_aaaa11112222'"
        ).fetchone()
        assert row is not None
        assert row[0] == sentinel


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
