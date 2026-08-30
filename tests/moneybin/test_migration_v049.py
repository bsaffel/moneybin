"""V049: create app.exchange_rate_overrides.

The user half of ``multi-currency.md``'s rate resolution (Requirement 14): a
correction outranks the cached provider rate for its own ``(pair, date)``, and a
later refetch never silently overwrites it. Its grain is one daily reference
rate — a per-transaction bank spread is explicitly out of scope.

Pure additive DDL (a new table), so the migration-realism rule does not require
populated fixtures — the coverage that matters is the key, the CHECK, and the
scale. ``source_type`` is deliberately absent, unlike ``raw.exchange_rates``:
every row here has one author, and "the user" is what outranks the providers.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V049__create_app_exchange_rate_overrides import migrate
from tests.moneybin.migration_helpers import run_migration


def _insert_override(
    db: Database,
    *,
    from_currency: str = "USD",
    to_currency: str = "EUR",
    rate_date: date = date(2026, 3, 16),
    rate: str = "0.93000000",
    note: str | None = "bank rate, not ECB mid",
) -> None:
    db.execute(
        """
        INSERT INTO app.exchange_rate_overrides
            (from_currency, to_currency, rate_date, rate, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        [from_currency, to_currency, rate_date, Decimal(rate), note],
    )


@pytest.fixture
def pre_v049_db(db: Database) -> Database:
    """A database without the table, so migrate() does real work."""
    db.execute("DROP TABLE IF EXISTS app.exchange_rate_overrides")
    return db


def test_v049_creates_the_table(pre_v049_db: Database) -> None:
    run_migration(pre_v049_db, migrate)
    _insert_override(pre_v049_db)
    row = pre_v049_db.execute("SELECT rate FROM app.exchange_rate_overrides").fetchone()
    assert row is not None and row[0] == Decimal("0.93000000")


def test_v049_is_idempotent(pre_v049_db: Database) -> None:
    run_migration(pre_v049_db, migrate)
    _insert_override(pre_v049_db)
    run_migration(pre_v049_db, migrate)
    row = pre_v049_db.execute(
        "SELECT COUNT(*) FROM app.exchange_rate_overrides"
    ).fetchone()
    assert row is not None and row[0] == 1, "a second run must not drop a correction"


def test_v049_one_override_per_pair_and_date(pre_v049_db: Database) -> None:
    """One daily reference rate per pair — the repo corrects in place, never appends."""
    run_migration(pre_v049_db, migrate)
    _insert_override(pre_v049_db)
    with pytest.raises(duckdb.ConstraintException):
        _insert_override(pre_v049_db, rate="0.94000000")


def test_v049_to_currency_is_part_of_the_key(pre_v049_db: Database) -> None:
    """One from-currency priced into two others keeps both corrections."""
    run_migration(pre_v049_db, migrate)
    _insert_override(pre_v049_db, to_currency="EUR", rate="0.93000000")
    _insert_override(pre_v049_db, to_currency="GBP", rate="0.79000000")
    rows = pre_v049_db.execute(
        "SELECT to_currency, rate FROM app.exchange_rate_overrides ORDER BY to_currency"
    ).fetchall()
    assert rows == [
        ("EUR", Decimal("0.93000000")),
        ("GBP", Decimal("0.79000000")),
    ]


def test_v049_rejects_a_non_positive_rate(pre_v049_db: Database) -> None:
    """An override outranks the provider, so a zero here zeroes every conversion.

    ``raw.exchange_rates`` already carries ``CHECK (rate > 0)``. This table is
    the only other row the conversion layer will read, and it wins — without the
    same floor the guarantee has a hole on exactly the path that beats the cache.
    """
    run_migration(pre_v049_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        _insert_override(pre_v049_db, rate="0")
    with pytest.raises(duckdb.ConstraintException):
        _insert_override(pre_v049_db, rate="-0.93000000")


def test_v049_rate_keeps_eight_decimal_places(pre_v049_db: Database) -> None:
    """DECIMAL(18,8) at both ends of the range published for real pairs.

    Mirrors the provider cache's scale rather than trusting it: an override is
    read *instead* of ``raw.exchange_rates``, so a weak scale here truncates the
    rate the user typed precisely because they wanted a different one. At (18,2)
    the IDR rate below stores as 0.00 and converts every balance to zero.
    """
    run_migration(pre_v049_db, migrate)
    _insert_override(pre_v049_db, to_currency="IDR", rate="0.00006134")
    _insert_override(pre_v049_db, to_currency="JPY", rate="155.12345678")
    rows = pre_v049_db.execute(
        "SELECT to_currency, rate FROM app.exchange_rate_overrides ORDER BY to_currency"
    ).fetchall()
    assert rows == [
        ("IDR", Decimal("0.00006134")),
        ("JPY", Decimal("155.12345678")),
    ]


def test_v049_note_is_optional(pre_v049_db: Database) -> None:
    run_migration(pre_v049_db, migrate)
    _insert_override(pre_v049_db, note=None)
    row = pre_v049_db.execute("SELECT note FROM app.exchange_rate_overrides").fetchone()
    assert row is not None and row[0] is None


def test_fresh_schema_has_the_table(db: Database) -> None:
    """Dual-path: a fresh install gets the table from app_exchange_rate_overrides.sql."""
    _insert_override(db)
    row = db.execute("SELECT rate FROM app.exchange_rate_overrides").fetchone()
    assert row is not None and row[0] == Decimal("0.93000000")
