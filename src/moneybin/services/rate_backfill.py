"""Plan and fill the exchange-rate coverage a profile's own data implies.

Display conversion prices each row at its own date, so coverage is a span, not a
point. Gathering it belongs to refresh rather than to a report read: refresh
already holds the exclusive per-profile writer lock and is already slow, while a
report that fetched would take that lock behind a read-only-looking command and
fail whenever a sync held it.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta

import duckdb

from moneybin.connectors.feed_errors import FeedError
from moneybin.connectors.rates.protocol import RateAdapter, RateObservation
from moneybin.database import Database
from moneybin.metrics.registry import FX_RATE_BACKFILL_PAIRS_TOTAL
from moneybin.services._validators import validate_currency_code
from moneybin.services.currency_service import (
    MAX_BACKWARD_RESOLUTION_DAYS,
    CurrencyService,
    canonical_currency,
    is_storable_after_rounding,
    unsupported_currencies,
)
from moneybin.tables import (
    DIM_HOLDINGS,
    FCT_BALANCES_DAILY,
    FCT_INVESTMENT_TRANSACTIONS,
    FCT_TRANSACTIONS,
)

logger = logging.getLogger(__name__)


class RateBackfillNotReadyError(Exception):
    """Planning could not read ``core.*`` — a first-load precondition, not a crash.

    Named rather than left as the underlying DuckDB error so a caller can tell
    this apart from the identical exception types the store raises later in the
    same call. Carries no message: the refusal is about the database's state,
    not about any currency, and the step reports it by staying quiet.
    """


@dataclass(frozen=True, slots=True)
class RateBackfillResult:
    """What one backfill pass managed to gather.

    ``pairs_failed`` names pairs whose provider call raised — an outage or a
    malformed body — and never a pair the provider simply does not publish.
    Those two need different remedies: the first resolves itself on the next
    refresh, the second needs a manual override, and collapsing them would send
    a user to the override table over a dropped connection.

    ``pairs_unsupported`` is that second kind: a currency the provider has never
    published. It is separate rather than absent because an empty result is
    otherwise indistinguishable from "nothing new to fetch", so the pair would
    be re-requested on every refresh forever while the user was never told that
    only a manual ``moneybin fx set`` can fill it.

    ``pairs_discarded`` names pairs the provider *answered* where the answer did
    not cover the window — a rate thrown away before the store, dated outside
    the window or too small for the column to hold, or a series that simply
    begins after the window does. That last kind is a currency the provider
    started publishing partway through the profile's history: it drops nothing,
    so only comparing the answer against the requested start finds it. It exists
    for the reason above read once more: a pair whose every rate was dropped
    reports zero written and empty lists, which is exactly what a profile
    needing nothing reports. Membership is not exclusive with the other two and
    does not mean the pair is empty — some of its dates may have stored fine, so
    it says coverage *may* have holes.
    """

    rates_written: int
    pairs_failed: tuple[str, ...]
    pairs_unsupported: tuple[str, ...] = ()
    pairs_discarded: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RateWindow:
    """One contiguous span of dates to price for one currency pair.

    A span rather than a set of dates because the provider prices a whole range
    in a single call, and because a reference-rate series is full of legitimate
    holes — weekends, holidays — that a date-set model would re-request forever.
    """

    from_currency: str
    to_currency: str
    start: date
    end: date


def run_rate_backfill(
    db: Database,
    *,
    home_currency: str,
    through: date,
    adapter: RateAdapter,
) -> RateBackfillResult:
    """Gather every rate this profile's own rows imply, one call per pair.

    A pair whose *provider call* fails is recorded and the next pair still runs.
    By the time this step executes, refresh has already done its expensive work;
    the rates step is the only one whose input lives outside the machine, so
    letting a transient network fault propagate would discard a successful run.

    The store stays outside that isolation, and the two gates above it are what
    earn that. Its failures are meant to be non-per-pair — refresh holds the
    exclusive writer lock, so a write that raises here means the database itself
    is unusable, and attributing that to the currency being stored would report
    a lie. That only holds if nothing pair-specific can reach it: an out-of-window
    date or a rate the column cannot hold is the *provider's* fault for one pair,
    and letting either through would abort the whole loop and report every
    currency after it as never attempted. Both are filtered first, so a raise
    from the store means what the sentence above says it means. Filtering is not
    the same as hiding: whichever gate drops a rate, the pair is named in
    ``pairs_discarded``, because a pair that lost every rate to a filter would
    otherwise be reported exactly as a profile that needed no rates at all.
    """
    try:
        windows = plan_rate_backfill(db, home_currency=home_currency, through=through)
    except (duckdb.CatalogException, duckdb.BinderException) as exc:
        # Raised here, rather than read off the exception type at the call site,
        # so that only *planning* can mean "core.* is not built yet". The store
        # below raises the same two types on a drifted cache or a bind failure
        # on write, and a caller matching on the type alone would report that
        # crash as a step that quietly declined to run.
        raise RateBackfillNotReadyError from exc
    service = CurrencyService(db, adapter=adapter)
    FX_RATE_BACKFILL_PAIRS_TOTAL.labels(outcome="planned").inc(len(windows))

    written = 0
    failed: list[str] = []
    unsupported: list[str] = []
    discarded: list[str] = []
    for window in windows:
        pair = f"{window.from_currency}/{window.to_currency}"
        try:
            observations = adapter.fetch_range(
                window.from_currency, window.to_currency, window.start, window.end
            )
        except FeedError:
            # Only the pair is named. The dates are deliberately absent: this
            # line reaches the durable cli_YYYY-MM-DD.log, and an FX date is
            # classified TXN_DATE. A currency pair discloses no amount.
            logger.warning(f"Exchange rate backfill failed for {pair}")
            failed.append(pair)
            FX_RATE_BACKFILL_PAIRS_TOTAL.labels(outcome="failed").inc()
            continue
        if not observations and unsupported_currencies(
            adapter, window.from_currency, window.to_currency
        ):
            logger.warning(f"No exchange rate series is published for {pair}")
            unsupported.append(pair)
            FX_RATE_BACKFILL_PAIRS_TOTAL.labels(outcome="unsupported").inc()
            continue
        answered = _within_window(observations, window)
        if len(answered) != len(observations):
            logger.warning(
                f"Discarded {len(observations) - len(answered)} out-of-window "
                f"rate(s) for {pair}"
            )
        storable = tuple(o for o in answered if is_storable_after_rounding(o.rate))
        if len(storable) != len(answered):
            # Counted, never quoted: the rejected value is the provider's
            # answer for a dated pair, and the reason it was rejected is a
            # property of the column, not information the user can act on.
            logger.warning(
                f"Discarded {len(answered) - len(storable)} unstorable "
                f"rate(s) for {pair}"
            )
        short_at_the_start = not _covers_window_start(storable, window)
        if short_at_the_start:
            # Only the pair is named, for the reason the FeedError branch above
            # gives: this line reaches the durable log and an FX date is
            # classified TXN_DATE.
            logger.warning(f"Exchange rates for {pair} begin after the window does")
        if short_at_the_start or len(storable) != len(observations):
            discarded.append(pair)
            FX_RATE_BACKFILL_PAIRS_TOTAL.labels(outcome="discarded").inc()
        written += service.store_observations(storable)

    logger.info(
        f"Rate backfill: {len(windows)} pair(s) planned, {written} rate(s) written, "
        f"{len(failed)} pair(s) failed, {len(unsupported)} pair(s) unsupported, "
        f"{len(discarded)} pair(s) with discarded rate(s)"
    )
    return RateBackfillResult(
        rates_written=written,
        pairs_failed=tuple(failed),
        pairs_unsupported=tuple(unsupported),
        pairs_discarded=tuple(discarded),
    )


def plan_rate_backfill(
    db: Database, *, home_currency: str, through: date
) -> tuple[RateWindow, ...]:
    """The rate windows this profile's own rows imply, newest bound at ``through``."""
    home = _usable_currency(home_currency)
    if home is None:
        # The value never rides the log line, here or below: `currency_code` is
        # untrusted source data, so whatever a mis-mapped cell held is what a
        # malformed code contains.
        logger.warning("Rate backfill skipped: the home currency is not a valid code")
        return ()
    # The window is what the profile needs, never what the cache appears to
    # hold. Stored rows cannot answer the question: `raw.exchange_rates` records
    # the dates a provider published, not the ranges MoneyBin asked for, so a
    # span fetched in full and a span bracketed by two rows from separate
    # `moneybin fx rate` lookups are byte-for-byte identical in it. Reading
    # `MIN`/`MAX` as coverage would resume from the newest row and — since the
    # window only ever moves forward — strand every date between those two
    # lookups permanently, which is the exact gap this module exists to close.
    # A date-set model fails the other way: a reference series has legitimate
    # holes on every weekend and holiday, so it would re-request them forever.
    #
    # So the whole implied span is re-requested each refresh and the append-only
    # cache discards what it already holds. That costs one provider call per
    # foreign currency per refresh, spent to make the stranded-span case
    # impossible rather than merely unlikely.
    rows = db.execute(
        f"""
        WITH dated AS (
            SELECT currency_code, transaction_date AS on_date
              FROM {FCT_TRANSACTIONS.full_name}
            UNION ALL
            SELECT currency_code, balance_date
              FROM {FCT_BALANCES_DAILY.full_name}
            UNION ALL
            SELECT currency_code, trade_date
              FROM {FCT_INVESTMENT_TRANSACTIONS.full_name}
            UNION ALL
            -- A position is valued as of the day being reported, so that is the
            -- day it implies. `dim_holdings` does carry a `price_date` — the
            -- close its market value came from, which for a `carried_forward`
            -- position can be older than today — but conversion keys off the
            -- date being reported, not the date the price was struck, so the
            -- rate this needs is today's. Narrowing to nothing would leave a
            -- foreign holding unconvertible in a profile that synced positions
            -- but no investment ledger. Whether display conversion should
            -- instead price each holding at its own `price_date` is open:
            -- followups.md, "Rate coverage for a stale holding price_date".
            SELECT currency_code, ?
              FROM {DIM_HOLDINGS.full_name}
        ),
        needed AS (
            -- TRIM as well as UPPER so a padded code groups with its bare
            -- spelling: `canonical_currency` strips before it uppercases, and
            -- two spellings of one currency would otherwise plan two windows
            -- and fetch the same span twice.
            SELECT UPPER(TRIM(currency_code)) AS from_currency,
                   MIN(on_date) AS earliest
              FROM dated
             WHERE currency_code IS NOT NULL AND UPPER(TRIM(currency_code)) <> ?
             GROUP BY 1
        )
        SELECT from_currency, earliest
          FROM needed
         ORDER BY from_currency
        """,  # noqa: S608  # TableRef + parameterized values
        [through, home],
    ).fetchall()
    windows: list[RateWindow] = []
    unusable = 0
    for row in rows:
        from_currency = _usable_currency(str(row[0]))
        if from_currency is None:
            # `currency_code` is whatever the source file put there, and this is
            # the last point before it becomes the `base` parameter of an
            # outbound request. Skipping the pair rather than raising keeps one
            # bad cell from costing the currencies beside it; nothing the
            # provider could answer would ever cover the pair anyway, so the
            # request would repeat on every refresh.
            unusable += 1
            continue
        # Every row this currency has is dated after the window ends — a
        # scheduled transaction, or a clock-skewed import. Dropping the pair
        # rather than letting it out as a backwards range matters because the
        # provider answers a reversed range 404, the same status it uses for a
        # currency it does not publish, so the pair would be reported as
        # unsupported on every refresh forever.
        if row[1] > through:
            continue
        windows.append(
            RateWindow(
                from_currency=from_currency,
                to_currency=home,
                start=row[1],
                end=through,
            )
        )
    if unusable:
        logger.warning(f"Rate backfill skipped {unusable} unusable currency code(s)")
        FX_RATE_BACKFILL_PAIRS_TOTAL.labels(outcome="unusable").inc(unusable)
    return tuple(windows)


def _usable_currency(value: str) -> str | None:
    """The canonical form of ``value``, or ``None`` if it cannot name a currency.

    Shares ``CurrencyService``'s canonicalization and shape gate rather than
    re-deriving one: a code the service would refuse must not reach a provider
    by a different route, and a code it would accept under one spelling must not
    be planned under another.
    """
    candidate = canonical_currency(value)
    try:
        validate_currency_code(candidate)
    except ValueError:
        return None
    return candidate


def _within_window(
    observations: Sequence[RateObservation], window: RateWindow
) -> tuple[RateObservation, ...]:
    """The observations that answer ``window``, dropping the ones that do not.

    Bounded here rather than in an adapter for the reason
    ``CurrencyService._fetch`` gives for its own bound: the window belongs to
    the Protocol, not to one provider's response shape, so one guard covers
    every adapter instead of each having to remember its own.

    What it protects is specific. ``raw.exchange_rates`` is append-only and its
    rows are read as a record of what the provider published on a date, so a row
    landing under a date nobody asked about is wrong for as long as the profile
    exists — and being append-only, it cannot be corrected in place. Every later
    conversion on that date reads it. A range endpoint that echoed a wider span
    than requested, or resolved a bound outward instead of inward, would write
    exactly that.

    The lower bound allows the same publication-day slack a single fetch does —
    a window opening on a closed market is legitimately answered with the last
    trading day before it.
    """
    earliest = window.start - timedelta(days=MAX_BACKWARD_RESOLUTION_DAYS)
    return tuple(
        observation
        for observation in observations
        if earliest <= observation.rate_date <= window.end
    )


def _covers_window_start(stored: Sequence[RateObservation], window: RateWindow) -> bool:
    """Whether the rates being kept reach back to the day ``window`` opens on.

    The bound above rejects an answer that reaches *too far*; this one asks
    whether it reached far enough. Nothing else does: a provider that began
    publishing a currency partway through the profile's history answers every
    date it has, so each rate passes the window bound, none is dropped, and the
    uncovered prefix leaves no trace. The window cannot grow into it later
    either — it is derived from rows that already exist, so it only ever opens
    earlier when *earlier data* is imported, never when time passes.

    The same publication slack applies, read forward instead of back: a window
    opening on a closed market is legitimately first answered days later, so a
    series starting within ``MAX_BACKWARD_RESOLUTION_DAYS`` of the request is
    covered. An empty set never is — zero coverage is the total case of the
    same shortfall, not a separate one.
    """
    if not stored:
        return False
    latest_covered_start = window.start + timedelta(days=MAX_BACKWARD_RESOLUTION_DAYS)
    return min(observation.rate_date for observation in stored) <= latest_covered_start
