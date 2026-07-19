"""raw.security_prices DDL: registered, keyed, basis-constrained, append-only."""

from __future__ import annotations

import pytest

from moneybin.database import Database


def _insert(
    db: Database,
    *,
    close: str,
    basis: str = "raw",
    key: str = "plaid_sec_1",
    price_date: str = "2026-07-15",
    source: str = "plaid",
    origin: str = "item_1",
    currency: str = "USD",
) -> None:
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES (?, ?::DATE, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [key, price_date, currency, source, origin, close, basis],
    )


def test_table_is_registered_and_empty(db: Database) -> None:
    row = db.execute("SELECT COUNT(*) FROM raw.security_prices").fetchone()
    assert row is not None and row[0] == 0


def test_price_basis_check_rejects_undeclared_basis(db: Database) -> None:
    """An adapter that cannot state a basis fails at ingest, per Requirement 2."""
    with pytest.raises(Exception, match="(?i)constraint|check"):
        _insert(db, close="10.00", basis="probably_adjusted")


def test_quote_currency_is_part_of_the_key(db: Database) -> None:
    """A dual-quoted security keeps both prices instead of one overwriting the other."""
    _insert(db, close="100.00", currency="USD")
    _insert(db, close="79.00", currency="GBP")
    row = db.execute("SELECT COUNT(*) FROM raw.security_prices").fetchone()
    assert row is not None and row[0] == 2


def test_source_origin_keeps_two_connections_distinct(db: Database) -> None:
    """Two Plaid items reporting the same security-date are two observations."""
    _insert(db, close="100.00", origin="item_1")
    _insert(db, close="100.25", origin="item_2")
    row = db.execute("SELECT COUNT(*) FROM raw.security_prices").fetchone()
    assert row is not None and row[0] == 2
