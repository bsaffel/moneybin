"""core.dim_holdings valuation: market value, staleness, and honest NULLs."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


def _seed_position(db: Database, *, security_id: str = "canonvti0000001") -> None:
    """10 units at 100.00, cost basis 1000.00, in account acc_1."""
    db.execute(
        """
        INSERT INTO app.securities (security_id, name, security_type, ticker)
        VALUES (?, 'Vanguard Total Stock Market ETF', 'etf', 'VTI')
        """,  # noqa: S608  # test fixture, not executing user SQL
        [security_id],
    )
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees, created_by,
            investment_transaction_id
        ) VALUES ('buy_1', 'imp_1', 'acc_1', ?, 'VTI', 'buy',
                  DATE '2026-01-05', 10, 100.00, -1000.00, 0.00, 'test', 'buy_1')
        """,  # noqa: S608  # test fixture, not executing user SQL
        [security_id],
    )


def _seed_price(
    db: Database, *, price_date: date, close: str, security_id: str = "canonvti0000001"
) -> None:
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES ('sec_vti', ?, 'USD', 'plaid', 'item_1', ?, 'raw',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [price_date, close],
    )
    db.execute(
        """
        INSERT INTO app.security_links
            (link_id, security_id, ref_kind, ref_value, source_type,
             status, decided_by, decided_at)
        VALUES ('link_vti', ?, 'plaid_security_id', 'sec_vti', 'plaid',
                'accepted', 'auto', CURRENT_TIMESTAMP)
        ON CONFLICT DO NOTHING
        """,  # noqa: S608  # test fixture, not executing user SQL
        [security_id],
    )


def _holding(db: Database) -> tuple[object, ...]:
    """Fetch the one dim_holdings row for acc_1's position and assert it IS one.

    ``fetchall()`` (not ``fetchone()``) plus an explicit count check: a bug that
    fans a position out to two rows (e.g. a price join missing the currency
    predicate, matching every quote currency instead of the position's own) would
    otherwise pass or fail depending on DuckDB's arbitrary row-return order rather
    than deterministically failing — grain (account_id, security_id) uniqueness is
    this model's own contract, not an incidental assumption of the test.
    """
    rows = db.execute(
        """
        SELECT market_value, unrealized_gain, price_date, price_source,
               days_since_observed, valuation_status
        FROM core.dim_holdings
        WHERE account_id = 'acc_1'
        """
    ).fetchall()
    assert len(rows) == 1, (
        f"expected exactly one dim_holdings row for acc_1 (grain violation): {rows}"
    )
    return rows[0]


@pytest.mark.slow
def test_same_day_price_values_the_position(db: Database) -> None:
    _seed_position(db)
    _seed_price(db, price_date=date.today(), close="120.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    market_value, gain, _pd, source, days, status = _holding(db)
    assert market_value == Decimal("1200.00")
    assert gain == Decimal("200.00"), "market value less cost basis"
    assert source == "plaid"
    assert days == 0
    assert status == "valued"


@pytest.mark.slow
def test_older_price_carries_forward_with_rising_staleness(db: Database) -> None:
    """Markets close ~114 days a year; as-of resolution is what makes a series possible."""
    _seed_position(db)
    _seed_price(db, price_date=date.today() - timedelta(days=3), close="120.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    market_value, _gain, _pd, _source, days, status = _holding(db)
    assert market_value == Decimal("1200.00")
    assert days == 3
    assert status == "carried_forward"


@pytest.mark.slow
def test_most_recent_of_two_past_prices_wins(db: Database) -> None:
    """The as-of pick is 'most recent on or before today', not merely 'any eligible row'.

    None of the other fixtures in this module ever insert two same-security,
    same-currency observations, so `QUALIFY ROW_NUMBER() ... ORDER BY price_date
    DESC` is otherwise never exercised — a model that picked ANY eligible row
    (e.g. DuckDB's scan order, or `ORDER BY price_date ASC`) would pass every
    other test here unnoticed. The older, wrong-answer row is inserted FIRST so
    a table-scan-order bug produces the stale close (50.00) instead of the
    correct one (120.00) — inserting the winner first would let that exact bug
    pass by coincidence.
    """
    _seed_position(db)
    _seed_price(db, price_date=date.today() - timedelta(days=10), close="50.00")
    _seed_price(db, price_date=date.today() - timedelta(days=2), close="120.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    market_value, _gain, price_date, _source, days, status = _holding(db)
    assert market_value == Decimal("1200.00"), "the newer close (120.00) must win"
    assert price_date == date.today() - timedelta(days=2)
    assert days == 2
    assert status == "carried_forward"


@pytest.mark.slow
def test_future_price_never_values_an_earlier_date(db: Database) -> None:
    _seed_position(db)
    _seed_price(db, price_date=date.today() + timedelta(days=5), close="500.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    market_value, _gain, _pd, _source, _days, status = _holding(db)
    assert market_value is None
    assert status == "unpriced"


@pytest.mark.slow
def test_unpriced_holding_is_null_never_zero(db: Database) -> None:
    """Zero is indistinguishable from a worthless position and understates every total."""
    _seed_position(db)

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    market_value, gain, price_date, source, days, status = _holding(db)
    assert market_value is None
    assert gain is None
    assert price_date is None
    assert source is None
    assert days is None
    assert status == "unpriced"


@pytest.mark.slow
def test_price_in_another_currency_does_not_value_the_position(db: Database) -> None:
    """Valuing a USD position at a GBP close would be silently wrong; M1K.2 converts."""
    _seed_position(db)
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES ('sec_vti', CURRENT_DATE, 'GBP', 'plaid', 'item_1', 95.00, 'raw',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
    )
    _seed_price(db, price_date=date.today() - timedelta(days=400), close="1.00")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    _mv, _gain, price_date, _source, _days, _status = _holding(db)
    assert price_date == date.today() - timedelta(days=400), (
        "the GBP close must not win over an older USD one"
    )
