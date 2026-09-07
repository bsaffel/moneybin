"""Tests for V057: capture the Plaid balance fields the client was discarding.

``SyncBalance`` declared six of the nine keys moneybin-sync sends in
``balances[]``, and Pydantic's default ``extra='ignore'`` destroyed the other
three at validation. Fresh installs get the columns from the schema DDL;
existing installs get them here. Pure additive (``ADD COLUMN IF NOT EXISTS ...
NULL``), so it is idempotent and needs no backfill — a row loaded before this
migration keeps NULL until ``moneybin sync pull --force`` re-fetches it.
"""

from __future__ import annotations

from moneybin.database import Database

# Private on both sides deliberately: the byte-identity test derives the DDL and
# migration strings from the real sources, and restating either as a literal here
# would be the drift it exists to catch. Neither has a public alias.
from moneybin.schema import (
    _all_schema_files,  # pyright: ignore[reportPrivateUsage]
    _comment_plan,  # pyright: ignore[reportPrivateUsage]
)
from moneybin.sql.migrations.V057__add_plaid_balance_wire_fields import (
    _COLUMNS,  # pyright: ignore[reportPrivateUsage]
    migrate,
)

_NEW_COLUMNS = ["balance_limit", "margin_loan_amount", "last_updated_datetime"]

# The pre-V057 shape of raw.plaid_balances (none of the three wire fields).
_PRE_V057_DDL = """
    CREATE TABLE raw.plaid_balances (
        account_id VARCHAR NOT NULL,
        balance_date DATE NOT NULL,
        current_balance DECIMAL(18, 2),
        available_balance DECIMAL(18, 2),
        iso_currency_code VARCHAR,
        unofficial_currency_code VARCHAR,
        source_file VARCHAR NOT NULL,
        source_type VARCHAR NOT NULL DEFAULT 'plaid',
        source_origin VARCHAR NOT NULL,
        extracted_at TIMESTAMP,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (account_id, balance_date, source_origin)
    )
"""


def _plaid_balances_columns(db: Database) -> set[str]:
    rows = db.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'raw' AND table_name = 'plaid_balances'"
    ).fetchall()
    return {row[0] for row in rows}


def _recreate_pre_v057_table(db: Database) -> None:
    """Reverse the V057 end-state: rebuild raw.plaid_balances without the new columns."""
    db.execute("DROP TABLE IF EXISTS raw.plaid_balances")
    db.execute(_PRE_V057_DDL)
    db.execute(
        "INSERT INTO raw.plaid_balances "
        "(account_id, balance_date, current_balance, available_balance, "
        "source_file, source_type, source_origin) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            "acc_pre_001",
            "2026-06-01",
            "1000.00",
            "950.00",
            "sync_old",
            "plaid",
            "item_abc",
        ],
    )


def test_v057_adds_the_dropped_wire_columns(db: Database) -> None:
    _recreate_pre_v057_table(db)
    before = _plaid_balances_columns(db)
    assert not (before & set(_NEW_COLUMNS)), "setup failed: new columns already present"

    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

    after = _plaid_balances_columns(db)
    for col in _NEW_COLUMNS:
        assert col in after, f"V057 did not add {col}"

    # A row loaded before the migration gets NULL for all three — no backfill.
    row = db.execute(
        "SELECT balance_limit, margin_loan_amount, last_updated_datetime "
        "FROM raw.plaid_balances WHERE account_id = 'acc_pre_001'"
    ).fetchone()
    assert row == (None, None, None)


def test_v057_comments_match_the_schema_ddl_byte_for_byte() -> None:
    """A comment that diverges from the DDL loses to it on the very next open.

    ``_apply_comments`` re-runs ``raw_plaid_balances.sql``'s comments on every
    startup while this migration runs once, so the catalog description would
    read the migration's text in the upgrade session and the DDL's in every
    session after — differing by which ran last. V050 and V052 record the same
    requirement as a comment only; this asserts it. Both sides are derived, so
    the check cannot be satisfied by restating either literal.
    """
    ddl = next(p for p in _all_schema_files() if p.name == "raw_plaid_balances.sql")
    ddl_comments = {
        column: comment
        for table_plan in _comment_plan(ddl, ddl.read_text())
        for column, comment in table_plan.column_comments
    }

    assert {name: comment for name, _, comment in _COLUMNS} == {
        name: ddl_comments[name] for name, _, _ in _COLUMNS
    }


def test_v057_is_idempotent(db: Database) -> None:
    """ADD COLUMN IF NOT EXISTS — a second run (columns already present) is a no-op."""
    _recreate_pre_v057_table(db)

    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]
    migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

    after = _plaid_balances_columns(db)
    for col in _NEW_COLUMNS:
        assert col in after
