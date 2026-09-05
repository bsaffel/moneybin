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

import logging
from datetime import date, timedelta
from decimal import Decimal

import pytest

from moneybin.connectors.rates.errors import RateFeedUnreachableError
from moneybin.connectors.rates.protocol import RateObservation
from moneybin.database import Database
from moneybin.services.currency_service import MAX_BACKWARD_RESOLUTION_DAYS
from moneybin.services.rate_backfill import (
    RateBackfillNotReadyError,
    RateWindow,
    plan_rate_backfill,
    run_rate_backfill,
)
from tests.moneybin.db_helpers import (
    CORE_BRIDGE_CURRENCY_CONVERSIONS_DDL,
    CORE_FCT_INVESTMENT_TRANSACTIONS_DDL,
    create_core_tables,
)

_TODAY = date(2026, 8, 15)

# db_helpers ships core.dim_holdings as an empty `WHERE FALSE` stub view, which
# cannot take fixture rows. Same column shape, declared as a table so a position
# can be inserted; the planner reads only currency_code and price_date, so the
# divergence from the production view is invisible to what these tests assert.
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
    db.execute(CORE_BRIDGE_CURRENCY_CONVERSIONS_DDL)
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


def _add_received_currency_conversion(db: Database, *, on: date, currency: str) -> None:
    """Insert one conversion whose received Currency appears nowhere else."""
    db.execute(
        """
        INSERT INTO core.bridge_currency_conversions
            (conversion_id, source_shape, from_currency, to_currency,
             home_currency, from_amount, to_amount, from_date, to_date)
        VALUES ('fxc-received-only', 'source_single_row', 'GBP', ?, 'USD',
                100.00, 150.00, ?, ?)
        """,
        [currency, on, on],
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


def test_a_received_only_conversion_currency_yields_a_rate_window(
    db: Database,
) -> None:
    """A single-row received leg needs its own rate even without another row."""
    _add_received_currency_conversion(db, on=date(2026, 4, 12), currency="JPY")

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="JPY",
            to_currency="USD",
            start=date(2026, 4, 12),
            end=_TODAY,
        ),
    )


def test_two_sparse_cached_rows_do_not_hide_the_span_between_them(
    db: Database,
) -> None:
    """Stored rows are not evidence that the span between them was ever fetched.

    This is the whole reason the window is re-requested rather than resumed.
    ``moneybin fx rate`` caches exactly one date per call, so two ordinary
    lookups years apart leave two isolated rows. A planner that read the stored
    ``MIN``/``MAX`` as coverage would treat every date between them as held and
    resume from the newest — and because the window only ever moves forward,
    that interval would never be fetched again. The dates in between are exactly
    the ones a report spanning them has to convert.
    """
    _add_transaction(db, on=date(2020, 1, 1), currency="EUR")
    _add_stored_rate(db, on=date(2020, 1, 1))
    _add_stored_rate(db, on=date(2026, 6, 1))

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="EUR",
            to_currency="USD",
            start=date(2020, 1, 1),
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


def _add_holding(db: Database, *, currency: str, priced_on: date | None = None) -> None:
    """Insert one open position, priced at the close ``priced_on`` names."""
    db.execute(
        """
        INSERT INTO core.dim_holdings
            (account_id, security_id, quantity, cost_basis, currency_code,
             market_value, price_date, valuation_status)
        VALUES ('acct-2', 'sec-1', 10, 500.00, ?, 750.00, ?, 'valued')
        """,
        [currency, priced_on],
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


def test_a_holding_is_covered_from_the_close_it_was_priced_at(db: Database) -> None:
    """A position implies the day its market value was struck, not today.

    ``InvestmentService`` converts each position at its own ``price_date``, the
    same rule every other converting read follows — a transaction at its
    transaction date, a balance at its balance date. Planning today's rate
    instead would fetch a rate the read never asks for, so a carried-forward
    foreign position whose close is older than today would report a null
    combined market value immediately after a successful refresh.
    """
    _add_holding(db, currency="JPY", priced_on=date(2026, 7, 31))

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="JPY",
            to_currency="USD",
            start=date(2026, 7, 31),
            end=_TODAY,
        ),
    )


def test_an_unpriced_holding_implies_no_window(db: Database) -> None:
    """A position with no close has no value to convert, so it needs no rate.

    ``InvestmentService`` skips a row whose ``price_date`` is null, so a window
    planned for one would buy a provider call per refresh that nothing reads.
    """
    _add_holding(db, currency="JPY", priced_on=None)

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == ()


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


def test_a_currency_seen_only_in_the_future_is_not_sent_as_a_reversed_range(
    db: Database,
) -> None:
    """A window can only run forwards, so a pair implying none is dropped.

    A row dated after ``through`` — a scheduled transaction, a clock-skewed
    import — is the one case where the earliest needed date lands past the end
    of the window. Emitting it would send the provider a reversed range, which
    Frankfurter answers 404: the same status it uses for a currency it does not
    publish, so the pair would be reported unsupported on every refresh forever.
    """
    _add_transaction(db, on=_TODAY + timedelta(days=30), currency="EUR")

    assert plan_rate_backfill(db, home_currency="USD", through=_TODAY) == ()


def test_a_future_dated_currency_is_counted_rather_than_skipped_in_silence(
    db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """The skip above leaves the trace its sibling three lines up leaves.

    Both drop a currency before it ever becomes a pair, so a profile whose only
    foreign rows sit past ``through`` plans nothing and reports exactly what a
    profile needing no rates at all reports. A count is all that can be said:
    the code is untrusted source data and the date it carries is ``TXN_DATE``.
    """
    _add_transaction(db, on=_TODAY + timedelta(days=30), currency="EUR")

    with caplog.at_level(logging.WARNING, logger="moneybin.services.rate_backfill"):
        plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert "skipped 1 currency code(s) dated after the window" in caplog.text


def test_a_fully_cached_pair_is_still_requested(db: Database) -> None:
    """The accepted cost of not trusting stored rows as coverage.

    Nothing distinguishes a span that was genuinely fetched from one bracketed
    by two stray rows, so the planner does not try: it re-requests the whole
    implied span every refresh and lets the append-only cache discard what it
    already holds. That is one provider call per pair per refresh, deliberately
    spent to make the sparse-row case above impossible rather than merely
    unlikely. Asserted explicitly so the cost cannot be reintroduced as a
    surprise, or optimized away without re-reading why it is here.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    _add_stored_rate(db, on=date(2026, 3, 10))
    _add_stored_rate(db, on=_TODAY)

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="EUR",
            to_currency="USD",
            start=date(2026, 3, 10),
            end=_TODAY,
        ),
    )


def test_a_newly_implied_earlier_date_reopens_the_window(db: Database) -> None:
    """History can arrive long after the backfill that would have covered it.

    Importing a years-old statement moves the earliest *needed* date backwards
    after rates have already been gathered — a different route to the same
    stranded history as the sparse rows above, and the one a user hits by
    importing rather than by looking a rate up. The window is planned from what
    the profile now needs, so the newly implied years are fetched on the next
    refresh rather than being permanently behind the stored rows.
    """
    _add_transaction(db, on=date(2019, 4, 2), currency="EUR")
    _add_stored_rate(db, on=_TODAY)

    windows = plan_rate_backfill(db, home_currency="USD", through=_TODAY)

    assert windows == (
        RateWindow(
            from_currency="EUR",
            to_currency="USD",
            start=date(2019, 4, 2),
            end=_TODAY,
        ),
    )


def test_a_malformed_currency_code_yields_no_window(db: Database) -> None:
    """`currency_code` is untrusted source data, so it cannot address a provider.

    A CSV whose columns shifted by one puts whatever that cell held into
    `currency_code`, and the planner reads those values straight out of
    `core.*`. Left unchecked the value becomes the `base` parameter of an
    outbound request to the rate provider — a value the user never typed,
    leaving the machine — and nothing the provider can answer will ever cover
    the pair, so the same request repeats on every refresh forever.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="Chase Checking 1098")

    assert plan_rate_backfill(db, home_currency="USD", through=_TODAY) == ()


def test_a_malformed_home_currency_plans_nothing(db: Database) -> None:
    """The home currency is the other half of every pair the planner emits.

    `ProfileSettingsRepo.set_home_currency` validates on the way in, so this is
    reachable only by writing `app.profile_settings` through the documented
    operator bypass. It is still cheaper to refuse the pair than to spend one
    outbound request per foreign currency on a quote nothing can answer.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")

    assert plan_rate_backfill(db, home_currency="dollars", through=_TODAY) == ()


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


class _DatedAdapter(_SpanAdapter):
    """Answers every window with rates on exactly the dates it was built with."""

    def __init__(self, *days: date) -> None:
        super().__init__()
        self._days = days

    def fetch_range(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> tuple[RateObservation, ...]:
        self.ranges.append((from_currency, to_currency, start, end))
        return tuple(
            RateObservation(
                from_currency, to_currency, day, Decimal("1.10"), self.source_type
            )
            for day in self._days
        )


class _FlakyAdapter(_SpanAdapter):
    """Unreachable for EUR, fine for everything else."""

    def fetch_range(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> tuple[RateObservation, ...]:
        self.ranges.append((from_currency, to_currency, start, end))
        if from_currency == "EUR":
            raise RateFeedUnreachableError("rate feed unreachable: ConnectError")
        return (
            RateObservation(
                from_currency, to_currency, start, Decimal("1.10"), self.source_type
            ),
        )


class _UnstorableAdapter(_SpanAdapter):
    """Prices EUR at a rate the column cannot hold; fine for everything else.

    ``0.000000001`` is chosen over a zero or a negative because it is what a
    real feed produces — a hyperinflated currency quoted against a strong one
    genuinely rounds to nothing at ``DECIMAL(18,8)``. It is numeric, positive,
    and finite, so only the *quantized* check rejects it.
    """

    def fetch_range(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> tuple[RateObservation, ...]:
        self.ranges.append((from_currency, to_currency, start, end))
        rate = Decimal("0.000000001") if from_currency == "EUR" else Decimal("1.10")
        return (
            RateObservation(from_currency, to_currency, start, rate, self.source_type),
        )


class _UnknowingAdapter(_SilentAdapter):
    """Publishes nothing and cannot say which currencies it supports."""

    def supported_currencies(self) -> frozenset[str]:
        raise RateFeedUnreachableError("rate feed unreachable: ConnectError")


def test_a_planned_window_is_fetched_once_and_stored(db: Database) -> None:
    """One call per pair, over the whole span — never one call per day.

    The span opens ``MAX_BACKWARD_RESOLUTION_DAYS`` before the window it covers;
    ``test_a_window_opening_on_a_closed_market_is_covered_from_before_it`` is
    where that reach is explained.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _SpanAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert adapter.ranges == [
        (
            "EUR",
            "USD",
            date(2026, 3, 10) - timedelta(days=MAX_BACKWARD_RESOLUTION_DAYS),
            _TODAY,
        )
    ]
    assert result.rates_written == 1
    assert result.pairs_failed == ()


def test_planning_against_an_unbuilt_core_raises_the_named_precondition(
    db: Database,
) -> None:
    """A fresh profile's missing `core.*` is raised by name, not as a DuckDB error.

    Refresh runs before the first transform, so this is the normal state of a new
    install. The store raises these same DuckDB types on a late write failure, so
    the distinction has to be drawn here — where planning is the only thing that
    has run — rather than by matching the exception type at the call site, which
    would report a genuine write crash as a step that declined to run.

    Every relation the planner reads is dropped, not just one. A fresh install
    has none of them, and dropping a single table instead would be the drifted
    schema the test below covers — a fixture that reaches the same catch for the
    opposite reason, so it could not tell the two apart.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    # `dim_holdings` and the Currency bridge are tables in this module, so all
    # five drop the same way here.
    for relation in (
        "core.bridge_currency_conversions",
        "core.fct_transactions",
        "core.fct_balances_daily",
        "core.fct_investment_transactions",
        "core.dim_holdings",
    ):
        db.execute(f"DROP TABLE IF EXISTS {relation}")  # noqa: S608  # fixed literals

    with pytest.raises(RateBackfillNotReadyError):
        run_rate_backfill(
            db, home_currency="USD", through=_TODAY, adapter=_SpanAdapter()
        )


def test_a_drifted_core_schema_is_reported_rather_than_silently_skipped(
    db: Database,
) -> None:
    """A built-but-drifted `core.*` is a real failure, not a first-load precondition.

    The two states raise the same DuckDB types from the same call, and the quiet
    branch answers `rates_written=null` with no error and no recovery action —
    indistinguishable from a profile that had nothing to fetch. On a mature
    database that is a renamed column or a dropped model going unreported
    forever, so the suppression has to prove the models are wholly unbuilt
    before claiming the first-load excuse.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    db.execute("ALTER TABLE core.fct_transactions RENAME currency_code TO ccy")

    with pytest.raises(Exception) as caught:  # noqa: B017, PT011  # asserted below
        run_rate_backfill(
            db, home_currency="USD", through=_TODAY, adapter=_SpanAdapter()
        )
    assert not isinstance(caught.value, RateBackfillNotReadyError), (
        "the other three relations are built, so this is drift the user must see"
    )


def test_a_supported_pair_with_nothing_published_is_neither_failed_nor_unsupported(
    db: Database,
) -> None:
    """An empty answer for a pair the provider *does* carry names no remedy.

    Neither of those lists fits: nothing raised, and the currency list says the
    series exists, so pointing at a retry or at `moneybin fx set` would send the
    user to fix something that is not broken. It is still the total case of the
    leading gap below — zero coverage over a window that needs it — so it is
    reported as discarded, which claims a shortfall without naming a cure.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _SilentAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.rates_written == 0
    assert result.pairs_failed == ()
    assert result.pairs_unsupported == ()
    assert result.pairs_discarded == ("EUR/USD",)


def test_a_pair_the_provider_does_not_publish_is_reported_as_unsupported(
    db: Database,
) -> None:
    """An empty answer means two different things, and only one of them waits.

    A 404 for a currency the provider has never published is permanent: every
    later refresh sends the same doomed request and reports the same silence.
    Its remedy is a manual `moneybin fx set`, which the user can only reach by
    being told the pair needs one — so it is carried out separately from
    ``pairs_failed``, whose members resolve themselves on the next run.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="JPY")
    adapter = _SilentAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.pairs_unsupported == ("JPY/USD",)
    assert result.pairs_failed == ()
    assert result.rates_written == 0


def test_a_home_currency_the_provider_does_not_publish_is_reported_as_unsupported(
    db: Database,
) -> None:
    """Either side of the pair can be the one the provider has never carried.

    An AED home profile holding EUR rows asks for EUR/AED: the base is
    published and the quote is not, so a check that reads only the base sees a
    supported currency and calls the empty answer an absence. Every refresh
    then re-sends the same doomed request while the user is told nothing —
    the exact failure ``pairs_unsupported`` exists to prevent, reached from the
    other end. ``CurrencyService._unsupported`` already reads both.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _SilentAdapter()

    result = run_rate_backfill(db, home_currency="AED", through=_TODAY, adapter=adapter)

    assert result.pairs_unsupported == ("EUR/AED",)
    assert result.pairs_failed == ()


def test_an_unreadable_currency_list_leaves_the_pair_retryable(
    db: Database,
) -> None:
    """Claiming "unsupported" on a dropped connection sends the user to fix nothing.

    ``supported_currencies`` is what separates the permanent absence from the
    transient one, so when it cannot be read the separation is unavailable and
    the pair must not be called unsupported.

    Claiming *neither* does not follow from that, though, and is what this
    asserts against: an empty answer that reaches the checks below is short at
    the start of its window, so falling through lands the pair in
    ``pairs_discarded`` — which the CLI warning and
    ``_step_crash_recovery_actions`` both read as "the provider answered, and a
    retry returns the same unusable value." Neither holds here. The provider
    answered nothing and the question of whether it ever could was itself
    unanswered, so this is the one outcome a later run can still resolve.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="JPY")
    adapter = _UnknowingAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.pairs_unsupported == ()
    assert result.pairs_failed == ("JPY/USD",)
    assert result.pairs_discarded == ()


def test_a_rate_the_column_cannot_hold_is_dropped_rather_than_raised(
    db: Database,
) -> None:
    """A malformed provider value must not reach DuckDB from the bulk path.

    ``CurrencyService._fetch`` guards the single-date path with
    ``is_storable_after_rounding`` because an unstorable rate is not a typed
    failure downstream: one that quantizes to zero trips ``CHECK (rate > 0)``,
    an oversized one overflows ``_as_stored``, and a NaN panics the frame
    builder in native code. The bulk path reaches the same writer, so it needs
    the same gate.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _UnstorableAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.rates_written == 0
    assert result.pairs_failed == ()


def test_a_pair_whose_rates_were_all_discarded_says_so(db: Database) -> None:
    """Dropping a provider answer silently recreates the ambiguity above.

    ``rates_written == 0`` with three empty lists is what a profile that needed
    nothing reports, so a pair whose every rate was thrown away is
    indistinguishable from one that was never in the plan. Conversion stays
    impossible on those dates either way, and only one of the two is worth
    telling the user about.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _UnstorableAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.pairs_discarded == ("EUR/USD",)
    assert result.pairs_failed == ()
    assert result.pairs_unsupported == ()


def test_a_rate_dated_outside_the_window_is_discarded_visibly_too(
    db: Database,
) -> None:
    """The window bound drops rates for a different reason and hides it the same way.

    Both filters stand between a provider answer and the store, and neither
    leaves a trace a caller can read. Reporting only the unstorable one would
    fix half the blind spot and leave the half that fires when a range endpoint
    echoes dates nobody asked for.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _DatedAdapter(_TODAY + timedelta(days=1))

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.pairs_discarded == ("EUR/USD",)
    assert result.rates_written == 0


def test_a_series_that_begins_after_the_window_reports_its_leading_gap(
    db: Database,
) -> None:
    """A provider that started publishing mid-history leaves the years before it.

    The window opens at the profile's earliest row, so a currency the provider
    only began carrying later answers every date it has and none of the ones
    before. Every returned rate passes the window bound, nothing is dropped, and
    the pair would otherwise be named in no list at all — reported exactly like
    a pair stored in full. The window never widens on its own either: it is
    derived from rows that already exist, so waiting for a later refresh cannot
    fill a prefix the provider does not publish.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _DatedAdapter(
        date(2026, 3, 10) + timedelta(days=MAX_BACKWARD_RESOLUTION_DAYS + 1)
    )

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.pairs_discarded == ("EUR/USD",)
    assert result.rates_written == 1, "the dates it did answer are still cached"
    assert result.pairs_failed == ()
    assert result.pairs_unsupported == ()


def test_a_series_that_starts_days_into_the_window_is_a_leading_gap_too(
    db: Database,
) -> None:
    """The slack the lower window bound allows does not run in this direction.

    Nothing resolves a date forward. ``CurrencyService.resolve_rate`` refuses an
    observation dated after the day asked about outright, and its cache path
    tries only that exact day and the last publication day *before* it. A series
    whose first rate lands even a few days into the window therefore leaves
    every transaction ahead of it unpriceable offline, however short the gap —
    so the check asks for a rate on or before the day the window opens, not one
    within reach of it.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _DatedAdapter(date(2026, 3, 10) + timedelta(days=3))

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.pairs_discarded == ("EUR/USD",)
    assert result.rates_written == 1, "the dates it did answer are still cached"


def test_a_window_opening_on_a_closed_market_is_covered_from_before_it(
    db: Database,
) -> None:
    """The negative control on the bound above, and why the request reaches back.

    A profile whose earliest row falls on a weekend opens its window on a day
    the provider never publishes. A request starting exactly there could not
    come back covering it, so the strict bound above would warn on every refresh
    forever with nothing the user could do. The request opens
    ``MAX_BACKWARD_RESOLUTION_DAYS`` earlier instead — exactly the span
    ``_within_window`` already accepts — so the last publication day before the
    window is fetched and stored, which is the row ``resolve_rate``'s hop back
    to the last publication day then reads.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _DatedAdapter(date(2026, 3, 10) - timedelta(days=2), _TODAY)

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert adapter.ranges == [
        (
            "EUR",
            "USD",
            date(2026, 3, 10) - timedelta(days=MAX_BACKWARD_RESOLUTION_DAYS),
            _TODAY,
        )
    ]
    assert result.pairs_discarded == ()


def test_a_series_that_stops_short_of_the_window_end_reports_its_trailing_gap(
    db: Database,
) -> None:
    """A series that ceased publication leaves the recent dates uncached.

    The mirror of the leading gap, and invisible for the same reason: every rate
    the provider sent is in range and storable, so nothing is dropped and no
    length comparison notices. A currency that stopped being published — or a
    stale proxy replaying an old copy of the range — therefore reports exactly
    like a pair stored in full, while the newest dates a report touches have no
    rate at all.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _DatedAdapter(date(2026, 3, 10) - timedelta(days=1), date(2026, 3, 15))

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.pairs_discarded == ("EUR/USD",)
    assert result.rates_written == 2, "the dates it did answer are still cached"


def test_a_series_answered_up_to_the_publication_lag_is_not_a_trailing_gap(
    db: Database,
) -> None:
    """The negative control on the bound above: an unpublished today is routine.

    Unlike the opening edge, the closing one is deliberately not exact. The
    provider has no rate for a weekend and often none for today until the
    afternoon, so a window ending on the day it is run is normally answered a
    little short — and the next refresh collects the rest, because the window's
    end moves forward on its own. Firing there would warn on every healthy
    profile, so the bound allows the same span a backward resolution may cross
    and catches only a series that has genuinely stopped.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _DatedAdapter(
        date(2026, 3, 10) - timedelta(days=1),
        _TODAY - timedelta(days=MAX_BACKWARD_RESOLUTION_DAYS),
    )

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.pairs_discarded == ()
    assert result.rates_written == 2


def test_a_pair_stored_in_full_is_not_reported_as_discarded(db: Database) -> None:
    """The negative control: the ordinary path must stay quiet.

    A list that named every pair would carry no information, and the CLI
    warning it drives would print on every healthy refresh. Both edges are
    answered here, so neither coverage bound can be what keeps it quiet.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _DatedAdapter(date(2026, 3, 10), _TODAY)

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.pairs_discarded == ()
    assert result.rates_written == 2


def test_one_unstorable_rate_does_not_cost_the_pairs_after_it(db: Database) -> None:
    """The isolation `FeedError` already gets, for the same reason.

    A rate that raises out of the store aborts the loop over windows, so every
    currency planned after the offending one is never attempted and the step
    reports as though it never ran. One malformed value in one provider
    response must cost exactly its own pair.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    _add_transaction(db, on=date(2026, 4, 1), currency="GBP")
    adapter = _UnstorableAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert [pair[0] for pair in adapter.ranges] == ["EUR", "GBP"]
    assert result.rates_written == 1, "GBP still got its rate"


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


def test_a_malformed_code_does_not_cost_the_currencies_beside_it(
    db: Database,
) -> None:
    """One unusable cell is not a reason to skip the profile's real currencies."""
    _add_transaction(db, on=date(2026, 3, 10), currency="Chase Checking 1098")
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _SpanAdapter()

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert adapter.ranges == [
        (
            "EUR",
            "USD",
            date(2026, 3, 10) - timedelta(days=MAX_BACKWARD_RESOLUTION_DAYS),
            _TODAY,
        )
    ]
    assert result.rates_written == 1


def test_a_rate_dated_after_the_window_is_not_stored(db: Database) -> None:
    """A rate dated past `end` is a claim about a day nobody asked about.

    `raw.exchange_rates` records what a provider published on a date, and it is
    append-only — so a row filed under the wrong day is wrong for the life of
    the profile and cannot be corrected in place. Every later conversion on that
    date reads it.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _DatedAdapter(_TODAY + timedelta(days=1))

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.rates_written == 0


def test_a_rate_dated_far_before_the_window_is_not_stored(db: Database) -> None:
    """The same bound, the other way, past the slack a real answer can use.

    Separate from the upper bound because the lower one is not symmetric: a
    provider legitimately answers a closed day with an earlier one, so the
    window opens with `MAX_BACKWARD_RESOLUTION_DAYS` of slack. This date is one
    day beyond it — far enough that no publication-day resolution explains it,
    which is what makes it a wrong date rather than a resolved one.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _DatedAdapter(
        date(2026, 3, 10) - timedelta(days=MAX_BACKWARD_RESOLUTION_DAYS + 1)
    )

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.rates_written == 0


def test_a_rate_resolved_back_to_the_last_publication_day_is_kept(
    db: Database,
) -> None:
    """The bound must not reject what the provider legitimately does.

    A window opening on a Sunday is answered with Friday's rate — the same
    bounded backward resolution `CurrencyService._fetch` already accepts for a
    single date. This is the negative control on the two bounds above: it
    passes before them and must keep passing after.
    """
    _add_transaction(db, on=date(2026, 3, 10), currency="EUR")
    adapter = _DatedAdapter(date(2026, 3, 10) - timedelta(days=2))

    result = run_rate_backfill(db, home_currency="USD", through=_TODAY, adapter=adapter)

    assert result.rates_written == 1
