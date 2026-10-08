"""V069: create raw.exchange_rate_currencies.

Pure additive DDL (a new table), so the migration-realism rule does not require
populated fixtures — the coverage that matters is the key and the dual path.
"""

from __future__ import annotations

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V069__create_raw_exchange_rate_currencies import migrate
from tests.moneybin.migration_helpers import run_migration


def _insert(db: Database, source_type: str = "frankfurter", code: str = "USD") -> None:
    db.execute(
        "INSERT INTO raw.exchange_rate_currencies (source_type, currency_code) "
        "VALUES (?, ?)",
        [source_type, code],
    )


@pytest.fixture
def pre_v069_db(db: Database) -> Database:
    """A database without the table, so migrate() does real work."""
    db.execute("DROP TABLE IF EXISTS raw.exchange_rate_currencies")
    return db


def test_v069_creates_the_table(pre_v069_db: Database) -> None:
    run_migration(pre_v069_db, migrate)
    _insert(pre_v069_db)
    row = pre_v069_db.execute(
        "SELECT source_type, currency_code, loaded_at IS NOT NULL "
        "FROM raw.exchange_rate_currencies"
    ).fetchone()
    assert row == ("frankfurter", "USD", True)


def test_v069_is_idempotent(pre_v069_db: Database) -> None:
    run_migration(pre_v069_db, migrate)
    _insert(pre_v069_db)
    run_migration(pre_v069_db, migrate)
    row = pre_v069_db.execute(
        "SELECT COUNT(*) FROM raw.exchange_rate_currencies"
    ).fetchone()
    assert row is not None and row[0] == 1, "a second run must not drop the list"


def test_v069_one_row_per_provider_and_currency(pre_v069_db: Database) -> None:
    """Two providers can both publish a currency; one provider lists it once."""
    run_migration(pre_v069_db, migrate)
    _insert(pre_v069_db, "frankfurter", "USD")
    _insert(pre_v069_db, "exchangerate_host", "USD")
    with pytest.raises(duckdb.ConstraintException):
        _insert(pre_v069_db, "frankfurter", "USD")


def test_fresh_schema_has_the_table(db: Database) -> None:
    """Dual-path: a fresh install gets the table from raw_exchange_rate_currencies.sql."""
    _insert(db)
    row = db.execute("SELECT COUNT(*) FROM raw.exchange_rate_currencies").fetchone()
    assert row is not None and row[0] == 1
