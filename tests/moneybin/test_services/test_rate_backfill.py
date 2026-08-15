"""Which rates a profile actually needs, and over which dates.

Display conversion prices every row at its own date, so a report covering three
years needs a rate for every date it touches — not one. Fetching those lazily on
the read path would put a network call and the exclusive writer lock behind a
command that looks read-only, so the rates are gathered during refresh instead,
where the lock is already held. This module plans that gather.

The plan is deliberately a *window per pair* rather than a list of dates: the
provider prices a whole date range in one call, and a reference-rate series has
holes by construction (weekends, holidays) that must never be mistaken for
missing coverage and re-fetched forever.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from moneybin.connectors.rates.errors import RateFeedUnreachableError
from moneybin.connectors.rates.protocol import RateObservation
from moneybin.database import Database
from moneybin.services.rate_backfill import (
    RateWindow,
    plan_rate_backfill,
    run_rate_backfill,
)
from tests.moneybin.db_helpers import (
    CORE_FCT_INVESTMENT_TRANSACTIONS_DDL,
    create_core_tables,
)

_TODAY = date(2026, 8, 15)

# db_helpers ships core.dim_holdings as an empty `WHERE FALSE` stub view, which
# cannot take fixture rows. Same column shape, declared as a table so a position
# can be inserted; the planner only reads currency_code, so the divergence from
# the production view is invisible to what these tests assert.
_DIM_HOLDINGS_TABLE_DDL = """\
CREATE TABLE IF NOT EXISTS core.dim_holdings (
    account_id VARCHAR,
    security_id VARCHAR,
    quantity DECIMAL(28, 10),
    cost_basis DECIMAL(18, 2),
    average_cost DECIMAL(28, 10),
    currency_code VARCHAR,
    market_value DECIMAL(18, 2),
    unrealized_gain DECIMAL(18, 2),
    price_date DATE,
    price_source VARCHAR,
    days_since_observed INTEGER,
    valuation_status VARCHAR
);
"""


@pytest.fixture(autouse=True)
def _core_tables(db: Database) -> None:  # pyright: ignore[reportUnusedFunction]
    """Every test here reads the core relations a profile's currencies live in."""
    create_core_tables(db)
    db.execute(CORE_FCT_INVESTMENT_TRANSACTIONS_DDL)
    db.execute(_DIM_HOLDINGS_TABLE_DDL)


def _add_transaction(db: Database, *, on: date, currency: str | None) -> None:
    """Insert one core transaction carrying a currency and a date."""
    db.execute(
        """
        INSERT INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, amount, currency_code)
        VALUES (?, 'acct-1', ?, -10.00, ?)
        """,
        [f"txn-{on.isoformat()}-{currency}", on, currency],
    )


def _add_daily_balance(db: Database, *, on: date, currency: str | None) -> None:
    """Insert one daily balance carrying a currency and a date."""
    db.execute(
        """
        INSERT INTO core.fct_balances_daily
            (account_id, balance_date, balance, currency_code)
        VALUES ('acct-1', ?, 100.00, ?)
        """,
        [on, currency],
    )


def _add_stored_rate(db: Database, *, on: date, rate: str = "0.9") -> None:
    """Insert one cached EUR->USD provider rate."""
    db.execute(
        """
        INSERT INTO raw.exchange_rates
            (from_currency, to_currency, rate_date, rate, source_type)
        VALUES ('EUR', 'USD', ?, ?, 'frankfurter')
        """,
        [on, rate],
    )


def test_a_foreign_currency_yields_one_window_from_its_earliest_row(
    db: Database,
) -> None:
    """The window starts at the earliest row in that currency, not at its latest."""
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    _add_transaction(db, on=date(2026, 6, 20), currency="EUR")

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="EUR",
            to_currency="USD",
            start=date(2026, 3, 10),
            end=_TODAY,
        ),
    )


def test_stored_coverage_moves_the_window_past_what_is_already_cached(
    db: Database,
) -> None:
    """Refresh extends coverage forward; it never re-requests a cached span.

    Without this the backfill would re-fetch every date it already holds on
    every refresh — years of provider calls for rows that are already on disk,
    and the append-only cache would ignore every one of them.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    _add_stored_rate(db, on=date(2026, 3, 10))
    _add_stored_rate(db, on=date(2026, 6, 1))

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="EUR",
            to_currency="USD",
            start=date(2026, 6, 2),
            end=_TODAY,
        ),
    )


def test_a_balance_needs_a_rate_even_with_no_transaction_in_that_currency(
    db: Database,
) -> None:
    """Net worth converts balances, so a balance's currency must be covered.

    An account can hold a foreign balance for years without a single transaction
    reaching core in that currency — a dormant account, or one whose history was
    never imported. Planning from transactions alone leaves the net-worth report
    with no rate on exactly those rows.
    """
    _add_daily_balance(db, on=date(2026, 5, 4), currency="GBP")

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="GBP",
            to_currency="USD",
            start=date(2026, 5, 4),
            end=_TODAY,
        ),
    )


def test_the_window_starts_at_the_earliest_row_across_every_source(
    db: Database,
) -> None:
    """One currency reached by two relations yields one window, not two.

    The start is the earliest date any source carries, because a report may
    convert a balance older than the oldest transaction in the same currency.
    """
    _add_transaction(db, on=date(2026, 6, 20), currency="EUR")
    _add_daily_balance(db, on=date(2026, 2, 2), currency="EUR")

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="EUR",
            to_currency="USD",
            start=date(2026, 2, 2),
            end=_TODAY,
        ),
    )


def _add_investment_transaction(db: Database, *, on: date, currency: str) -> None:
    """Insert one core investment transaction carrying a currency and a trade date."""
    db.execute(
        """
        INSERT INTO core.fct_investment_transactions
            (investment_transaction_id, account_id, security_id, trade_date,
             type, amount, currency_code)
        VALUES (?, 'acct-2', 'sec-1', ?, 'buy', -500.00, ?)
        """,
        [f"inv-{on.isoformat()}-{currency}", on, currency],
    )


def _add_holding(db: Database, *, currency: str) -> None:
    """Insert one open position. A snapshot — it carries no historical date."""
    db.execute(
        """
        INSERT INTO core.dim_holdings
            (account_id, security_id, quantity, cost_basis, currency_code,
             market_value, valuation_status)
        VALUES ('acct-2', 'sec-1', 10, 500.00, ?, 750.00, 'valued')
        """,
        [currency],
    )


def test_an_investment_trade_is_covered_from_its_trade_date(db: Database) -> None:
    """An investment ledger denominated abroad needs rates over its own history."""
    _add_investment_transaction(db, on=date(2026, 1, 15), currency="CHF")

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="CHF",
            to_currency="USD",
            start=date(2026, 1, 15),
            end=_TODAY,
        ),
    )


def test_a_holding_is_covered_for_today_even_with_no_dated_row(db: Database) -> None:
    """A position carries no date, so it implies exactly one day: the one being valued.

    ``core.dim_holdings`` is a rebuilt snapshot whose final projection has no
    date column at all — the acquisition date it computes stays inside a CTE. A
    profile that synced positions but no investment ledger would otherwise have
    a market value with no rate to convert it, so net worth would silently omit
    the position. Today alone is the honest implication: nothing in the snapshot
    says when it was acquired.
    """
    _add_holding(db, currency="JPY")

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="JPY",
            to_currency="USD",
            start=_TODAY,
            end=_TODAY,
        ),
    )


def test_the_home_currency_is_never_fetched(db: Database) -> None:
    """A pair of one currency with itself is not a rate the provider will price.

    Frankfurter answers ``base=USD&symbols=USD`` with a 422, so letting the home
    currency through would turn every ordinary refresh into a failed call.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="USD")
    _add_transaction(db, on=date(2026, 3, 10), currency="usd")

    assert plan_rate_backfill(db, home_currency="USD", through=_TODAY) == ()


def test_a_row_with_no_currency_contributes_nothing(db: Database) -> None:
    """An unknown currency is its own segment; it is never converted, so never priced."""
    _add_transaction(db, on=date(2026, 3, 10), currency=None)
    _add_daily_balance(db, on=date(2026, 3, 10), currency=None)

    assert plan_rate_backfill(db, home_currency="USD", through=_TODAY) == ()


def test_coverage_already_complete_yields_no_window_at_all(db: Database) -> None:
    """A pair needing nothing is dropped, not sent as a backwards range.

    Once the newest stored rate reaches ``through``, ``newest + 1`` runs past the
    end of the window. Emitting it would send the provider a reversed range,
    which Frankfurter answers 404 — indistinguishable from "no such series", so
    a fully-cached pair would report as an unsupported one on every refresh.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    _add_stored_rate(db, on=_TODAY)

    assert plan_rate_backfill(db, home_currency="USD", through=_TODAY) == ()


def test_a_gap_inside_the_cached_span_is_not_refetched(db: Database) -> None:
    """Holidays leave real holes in a reference series; they are not missing coverage.

    A date-set model would see every non-trading day as absent and re-request it
    forever. Coverage is therefore a span bounded by what is stored, not the set
    of dates within it.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    _add_stored_rate(db, on=date(2026, 3, 10))
    # 2026-03-11 and 03-12 deliberately absent — a closed market.
    _add_stored_rate(db, on=date(2026, 3, 13))

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="EUR",
            to_currency="USD",
            start=date(2026, 3, 14),
            end=_TODAY,
        ),
    )


# ------------------------------ running the plan ------------------------------


class _SpanAdapter:
    """Prices every window it is given, one rate per requested start date."""

    source_type = "frankfurter"

    def __init__(self) -> None:
        self.ranges: list[tuple[str, str, date, date]] = []

    def fetch(
        self, from_currency: str, to_currency: str, on: date
    ) -> RateObservation | None:
        raise AssertionError("the backfill must never fetch one date at a time")

    def fetch_range(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> tuple[RateObservation, ...]:
        self.ranges.append((from_currency, to_currency, start, end))
        return (
            RateObservation(
                from_currency, to_currency, start, Decimal("1.10"), self.source_type
            ),
        )

    def supported_currencies(self) -> frozenset[str]:
        return frozenset({"USD", "EUR", "GBP"})


class _SilentAdapter(_SpanAdapter):
    """Answers every window with nothing, the way an unpublished pair does."""

    def fetch_range(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> tuple[RateObservation, ...]:
        self.ranges.append((from_currency, to_currency, start, end))
        return ()


class _FlakyAdapter(_SpanAdapter):
    """Unreachable for EUR, fine for everything else."""

    def fetch_range(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> tuple[RateObservation, ...]:
        self.ranges.append((from_currency, to_currency, start, end))
        if from_currency == "EUR":
            raise RateFeedUnreachableError("rate feed unreachable: ConnectError")
        return super().fetch_range(from_currency, to_currency, start, end)


def test_a_planned_window_is_fetched_once_and_stored(db: Database) -> None:
    """One call per pair, over the whole span — never one call per day."""
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _SpanAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert adapter.ranges == [("EUR", "USD", date(2026, 3, 10), _TODAY)]
    assert result.rates_written == 1
    assert result.pairs_failed == ()


def test_a_pair_the_provider_does_not_publish_is_not_a_failure(db: Database) -> None:
    """An unpublished pair is an absence to route around, not an error to raise."""
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _SilentAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.rates_written == 0
    assert result.pairs_failed == ()


def test_one_unreachable_pair_does_not_abandon_the_others(db: Database) -> None:
    """Refresh has already done its expensive work by the time rates run.

    Letting a transient network fault propagate would fail the whole command
    after gsheet, match, transform, categorize and identity had all succeeded —
    and the rates step is the one whose input is outside the machine.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    _add_transaction(db, on=date(2026, 4, 1), currency="GBP")
    adapter = _FlakyAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.pairs_failed == ("EUR/USD",)
    assert result.rates_written == 1, "GBP still got its rate"


def test_nothing_to_backfill_never_touches_the_provider(db: Database) -> None:
    """A single-currency profile must not make a network call on every refresh."""
    _add_transaction(db, on=date(2026, 3, 10), currency="USD")
    adapter = _SpanAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert adapter.ranges == []
    assert result.rates_written == 0
