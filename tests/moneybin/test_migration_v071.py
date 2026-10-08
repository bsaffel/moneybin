"""V071: create raw.exchange_rate_coverage.

Pure additive DDL (a new table), so the migration-realism rule does not require
populated fixtures — the coverage that matters is the key, the span CHECK, and
the dual path.
"""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V071__create_raw_exchange_rate_coverage import migrate
from tests.moneybin.migration_helpers import run_migration


def _insert(db: Database, start: date, end: date) -> None:
    db.execute(
        "INSERT INTO raw.exchange_rate_coverage "
        "(from_currency, to_currency, start_date, end_date, source_type) "
        "VALUES ('EUR', 'USD', ?, ?, 'frankfurter')",
        [start, end],
    )


@pytest.fixture
def pre_v071_db(db: Database) -> Database:
    """A database without the table, so migrate() does real work."""
    db.execute("DROP TABLE IF EXISTS raw.exchange_rate_coverage")
    return db


def test_v071_creates_the_table_and_is_idempotent(pre_v071_db: Database) -> None:
    run_migration(pre_v071_db, migrate)
    _insert(pre_v071_db, date(2025, 12, 24), date(2025, 12, 29))
    run_migration(pre_v071_db, migrate)
    row = pre_v071_db.execute(
        "SELECT COUNT(*) FROM raw.exchange_rate_coverage"
    ).fetchone()
    assert row == (1,)


def test_v071_refuses_a_backwards_span(pre_v071_db: Database) -> None:
    run_migration(pre_v071_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        _insert(pre_v071_db, date(2025, 12, 29), date(2025, 12, 24))


def test_fresh_schema_has_the_table(db: Database) -> None:
    """Dual-path: a fresh install gets the table from raw_exchange_rate_coverage.sql."""
    _insert(db, date(2025, 12, 24), date(2025, 12, 29))
    row = db.execute("SELECT COUNT(*) FROM raw.exchange_rate_coverage").fetchone()
    assert row == (1,)
