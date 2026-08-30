"""V050: add persistent_account_id and name to raw.plaid_accounts.

Pure additive DDL (``ADD COLUMN ... NULL``, no DEFAULT), so per
``.claude/rules/database.md`` populated fixtures are not required. They are here
anyway because the load-bearing claim is about rows that predate the columns:
the values are unrecoverable for them, and a migration that appeared to
reconstruct one would license a cross-connection merge on invented evidence.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V050__add_plaid_account_identity_fields import migrate
from tests.moneybin.migration_helpers import column_exists, insert_rows, run_migration

_PRE_MIGRATION_COLUMNS = (
    "account_id",
    "account_type",
    "account_subtype",
    "institution_name",
    "official_name",
    "mask",
    "source_file",
    "source_type",
    "source_origin",
)


_PRE_V050_DDL = """
CREATE TABLE raw.plaid_accounts (
    account_id VARCHAR NOT NULL,
    account_type VARCHAR,
    account_subtype VARCHAR,
    institution_name VARCHAR,
    official_name VARCHAR,
    mask VARCHAR,
    source_file VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL DEFAULT 'plaid',
    source_origin VARCHAR NOT NULL,
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id, source_origin)
)
"""


@pytest.fixture
def pre_v050_db(db: Database) -> Database:
    """A populated table without the columns, so migrate() does real work.

    Rebuilt from the pre-V050 DDL rather than ALTER-dropped: the primary key
    indexes ``source_origin``, and DuckDB refuses to drop any column ahead of an
    indexed one. Recreating is also the truer fixture — this is the table shape
    an existing database actually has.
    """
    db.execute("DROP TABLE raw.plaid_accounts")
    db.execute(_PRE_V050_DDL)
    insert_rows(
        db,
        "raw",
        "plaid_accounts",
        _PRE_MIGRATION_COLUMNS,
        [
            (
                "acc_old_check",
                "depository",
                "checking",
                "Chase",
                "Total Checking",
                "1234",
                "sync_job_a",
                "plaid",
                "item_chase_abc",
            ),
            (
                "acc_old_card_a",
                "credit",
                "credit card",
                "Chase",
                "Ultimate Rewards®",
                "1111",
                "sync_job_a",
                "plaid",
                "item_chase_abc",
            ),
            (
                "acc_old_card_b",
                "credit",
                "credit card",
                "Chase",
                "Ultimate Rewards®",
                "2222",
                "sync_job_a",
                "plaid",
                "item_chase_abc",
            ),
        ],
    )
    return db


def test_v050_adds_both_columns(pre_v050_db: Database) -> None:
    assert not column_exists(
        pre_v050_db, "raw", "plaid_accounts", "persistent_account_id"
    )
    assert not column_exists(pre_v050_db, "raw", "plaid_accounts", "name")

    run_migration(pre_v050_db, migrate)

    assert column_exists(pre_v050_db, "raw", "plaid_accounts", "persistent_account_id")
    assert column_exists(pre_v050_db, "raw", "plaid_accounts", "name")


def test_v050_leaves_pre_existing_rows_null(pre_v050_db: Database) -> None:
    """No backfill is possible, and a guessed one would be worse than none.

    Neither value is derivable from anything already stored: ``official_name``
    is the shared product label on the two cards above, and ``account_id`` is
    the very token a relink reissues. A ``persistent_account_id`` invented here
    would be an ``AccountResolver`` strong ref — it auto-adopts without review
    — so a wrong guess silently merges two ledgers. NULL means "unknown", the
    resolver skips the ref, and the next sync writes the real value.
    """
    run_migration(pre_v050_db, migrate)

    rows = pre_v050_db.execute(
        "SELECT persistent_account_id, name FROM raw.plaid_accounts ORDER BY account_id"
    ).fetchall()
    assert rows == [(None, None), (None, None), (None, None)]


def test_v050_is_idempotent(pre_v050_db: Database) -> None:
    run_migration(pre_v050_db, migrate)
    run_migration(pre_v050_db, migrate)

    row = pre_v050_db.execute("SELECT COUNT(*) FROM raw.plaid_accounts").fetchone()
    assert row is not None and row[0] == 3
