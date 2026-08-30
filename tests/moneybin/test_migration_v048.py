"""V048: create raw.exchange_rates.

The provider half of ``multi-currency.md``'s rate resolution: every reference
rate a feed publishes, cached locally so a conversion never depends on the
network being up twice. User corrections live in
``app.exchange_rate_overrides`` and outrank every row here.

Pure additive DDL (a new table), so the migration-realism rule does not require
populated fixtures — the coverage that matters is the key, the CHECK, and the
scale. ``source_type`` is part of the primary key for the same reason it is in
``raw.security_prices``: two providers can both publish a rate for one pair and
date, and omitting the column silently loses one.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V048__create_raw_exchange_rates import migrate
from tests.moneybin.migration_helpers import run_migration


def _insert_rate(
    db: Database,
    *,
    from_currency: str = "USD",
    to_currency: str = "EUR",
    rate_date: date = date(2026, 3, 13),
    rate: str = "0.87138",
    source_type: str = "frankfurter",
) -> None:
    db.execute(
        """
        INSERT INTO raw.exchange_rates
            (from_currency, to_currency, rate_date, rate, source_type)
        VALUES (?, ?, ?, ?, ?)
        """,
        [from_currency, to_currency, rate_date, Decimal(rate), source_type],
    )


@pytest.fixture
def pre_v048_db(db: Database) -> Database:
    """A database without the table, so migrate() does real work."""
    db.execute("DROP TABLE IF EXISTS raw.exchange_rates")
    return db


def test_v048_creates_the_table(pre_v048_db: Database) -> None:
    run_migration(pre_v048_db, migrate)
    _insert_rate(pre_v048_db)
    row = pre_v048_db.execute("SELECT rate FROM raw.exchange_rates").fetchone()
    assert row is not None and row[0] == Decimal("0.87138")


def test_v048_is_idempotent(pre_v048_db: Database) -> None:
    run_migration(pre_v048_db, migrate)
    _insert_rate(pre_v048_db)
    run_migration(pre_v048_db, migrate)
    row = pre_v048_db.execute("SELECT COUNT(*) FROM raw.exchange_rates").fetchone()
    assert row is not None and row[0] == 1, "a second run must not drop the cache"


def test_v048_one_rate_per_pair_date_and_source(pre_v048_db: Database) -> None:
    """Append-only: a refetch keeps the row first written rather than rewriting it."""
    run_migration(pre_v048_db, migrate)
    _insert_rate(pre_v048_db)
    with pytest.raises(duckdb.ConstraintException):
        _insert_rate(pre_v048_db, rate="0.99999")


def test_v048_source_type_is_part_of_the_key(pre_v048_db: Database) -> None:
    """Two providers can both publish one pair and date; neither displaces the other."""
    run_migration(pre_v048_db, migrate)
    _insert_rate(pre_v048_db, source_type="frankfurter", rate="0.87138")
    _insert_rate(pre_v048_db, source_type="exchangerate_host", rate="0.87140")
    rows = pre_v048_db.execute(
        "SELECT source_type, rate FROM raw.exchange_rates ORDER BY source_type"
    ).fetchall()
    assert rows == [
        ("exchangerate_host", Decimal("0.87140")),
        ("frankfurter", Decimal("0.87138")),
    ]


def test_v048_rejects_a_non_positive_rate(pre_v048_db: Database) -> None:
    """A zero rate converts every balance to nothing, and this table never edits.

    A row here can only be superseded by a user override, so a bad rate that
    lands is a permanent wrong answer for that date — the CHECK is the only
    place it can be refused.
    """
    run_migration(pre_v048_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        _insert_rate(pre_v048_db, rate="0")
    with pytest.raises(duckdb.ConstraintException):
        _insert_rate(pre_v048_db, rate="-0.87138")


def test_v048_rate_keeps_eight_decimal_places(pre_v048_db: Database) -> None:
    """DECIMAL(18,8) at both ends of the range published for real pairs.

    A weak scale is invisible on EUR/USD and ruinous elsewhere: at (18,2) the
    IDR rate below stores as 0.00 and converts every balance to zero, while JPY
    loses the places that decide the cents.
    """
    run_migration(pre_v048_db, migrate)
    _insert_rate(pre_v048_db, to_currency="IDR", rate="0.00006134")
    _insert_rate(pre_v048_db, to_currency="JPY", rate="155.12345678")
    rows = pre_v048_db.execute(
        "SELECT to_currency, rate FROM raw.exchange_rates ORDER BY to_currency"
    ).fetchall()
    assert rows == [
        ("IDR", Decimal("0.00006134")),
        ("JPY", Decimal("155.12345678")),
    ]


def test_fresh_schema_has_the_table(db: Database) -> None:
    """Dual-path: a fresh install gets the table from raw_exchange_rates.sql."""
    _insert_rate(db)
    row = db.execute("SELECT rate FROM raw.exchange_rates").fetchone()
    assert row is not None and row[0] == Decimal("0.87138")
