"""V043: create app.security_price_overrides.

Phase C.2 of ``investments-price-feeds.md`` lets a user set a price by hand for
any security and date, including securities no feed covers, and a later provider
fetch never overwrites that mark. The table is the user-mark half of
``core.fct_security_prices``' three-way union.

Pure additive DDL (a new table), so the migration-realism rule does not require
populated fixtures — the coverage that matters here is the key and the CHECK.
``quote_currency`` is part of the primary key for the same reason it is in
``raw.security_prices``: a security quoted in two currencies has two legitimate
prices for one date, and omitting the column silently loses one.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V043__create_app_security_price_overrides import migrate
from tests.moneybin.migration_helpers import run_migration

pytestmark = pytest.mark.fresh_db


def _insert_override(
    db: Database,
    *,
    security_id: str = "abc123def456",
    price_date: date = date(2026, 7, 12),
    quote_currency: str = "USD",
    close: str = "214.55",
    note: str | None = "broker statement",
) -> None:
    db.execute(
        """
        INSERT INTO app.security_price_overrides
            (security_id, price_date, quote_currency, close, note)
        VALUES (?, ?, ?, ?, ?)
        """,
        [security_id, price_date, quote_currency, Decimal(close), note],
    )


@pytest.fixture
def pre_v043_db(db: Database) -> Database:
    """A database without the table, so migrate() does real work."""
    db.execute("DROP TABLE IF EXISTS app.security_price_overrides")
    return db


def test_v043_creates_the_table(pre_v043_db: Database) -> None:
    run_migration(pre_v043_db, migrate)
    _insert_override(pre_v043_db)
    row = pre_v043_db.execute(
        "SELECT close FROM app.security_price_overrides"
    ).fetchone()
    assert row is not None and row[0] == Decimal("214.55")


def test_v043_is_idempotent(pre_v043_db: Database) -> None:
    run_migration(pre_v043_db, migrate)
    _insert_override(pre_v043_db)
    run_migration(pre_v043_db, migrate)
    row = pre_v043_db.execute(
        "SELECT COUNT(*) FROM app.security_price_overrides"
    ).fetchone()
    assert row is not None and row[0] == 1, "a second run must not drop the mark"


def test_v043_one_mark_per_security_date_currency(pre_v043_db: Database) -> None:
    run_migration(pre_v043_db, migrate)
    _insert_override(pre_v043_db)
    with pytest.raises(duckdb.ConstraintException):
        _insert_override(pre_v043_db, close="999.99")


def test_v043_quote_currency_is_part_of_the_key(pre_v043_db: Database) -> None:
    """A dual-quoted security keeps both marks rather than one overwriting the other."""
    run_migration(pre_v043_db, migrate)
    _insert_override(pre_v043_db, quote_currency="USD", close="214.55")
    _insert_override(pre_v043_db, quote_currency="GBP", close="169.20")
    rows = pre_v043_db.execute(
        "SELECT quote_currency, close FROM app.security_price_overrides "
        "ORDER BY quote_currency"
    ).fetchall()
    assert rows == [("GBP", Decimal("169.20")), ("USD", Decimal("214.55"))]


def test_v043_rejects_a_non_positive_close(pre_v043_db: Database) -> None:
    """Zero is the value the whole spec refuses to publish; a mark cannot assert it.

    ``raw.security_prices`` already carries ``CHECK (close > 0)``; a user mark is
    the one other row that can reach ``core.fct_security_prices``, so it needs
    the same floor or the guarantee "an unpriced holding is NULL, never zero"
    has a hole on the override path.
    """
    run_migration(pre_v043_db, migrate)
    with pytest.raises(duckdb.ConstraintException):
        _insert_override(pre_v043_db, close="0")
    with pytest.raises(duckdb.ConstraintException):
        _insert_override(pre_v043_db, close="-5.00")


def test_v043_note_is_optional(pre_v043_db: Database) -> None:
    run_migration(pre_v043_db, migrate)
    _insert_override(pre_v043_db, note=None)
    row = pre_v043_db.execute(
        "SELECT note FROM app.security_price_overrides"
    ).fetchone()
    assert row is not None and row[0] is None


def test_fresh_schema_has_the_table(db: Database) -> None:
    """Dual-path: a fresh install gets the table from app_security_price_overrides.sql."""
    _insert_override(db)
    row = db.execute("SELECT COUNT(*) FROM app.security_price_overrides").fetchone()
    assert row is not None and row[0] == 1
