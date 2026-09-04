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
    BRIDGE_CURRENCY_CONVERSIONS,
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
    the window or too small for the column to hold, or a series that begins
    after the window does or stops before it does. Those last two are a currency
    the provider only started carrying partway through the profile's history,
    and one that stopped being carried: both drop nothing, so only comparing the
    answer's own span against the requested one finds them. It exists for the
    reason above read once more: a pair whose every rate was dropped reports
    zero written and empty lists, which is exactly what a profile needing
    nothing reports. Membership is not exclusive with the other two and does not
    mean the pair is empty — some of its dates may have stored fine, so it says
    coverage *may* have holes.
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
        #
        # The phase alone is not enough, though. A mature database with a
        # renamed column or a dropped model raises the same two types from the
        # same call, and the quiet branch answers `rates_written=null` with no
        # error and no recovery action — the same thing a profile with nothing
        # to fetch answers. Only a core that is *wholly* unbuilt earns the
        # first-load excuse; anything else is drift the user has to be told
        # about, so it propagates to the step's error field.
        if _core_is_built(db):
            raise
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
                window.from_currency,
                window.to_currency,
                # Asked for earlier than the window opens, by exactly the span
                # `_within_window` already accepts. A profile whose earliest row
                # falls on a closed market opens its window on a day no provider
                # publishes, and nothing resolves that day forward: a request
                # starting on it could not come back covering it, so the check
                # below would name the pair on every refresh with nothing the
                # user could do. Reaching back fetches the last publication day
                # before the window instead — the row `resolve_rate`'s hop then
                # reads — so the opening date converts offline rather than
                # merely being reported as short.
                window.start - timedelta(days=MAX_BACKWARD_RESOLUTION_DAYS),
                window.end,
            )
        except FeedError:
            # Only the pair is named. The dates are deliberately absent: this
            # line reaches the durable cli_YYYY-MM-DD.log, and an FX date is
            # classified TXN_DATE. A currency pair discloses no amount.
            logger.warning(f"Exchange rate backfill failed for {pair}")
            failed.append(pair)
            FX_RATE_BACKFILL_PAIRS_TOTAL.labels(outcome="failed").inc()
            continue
        if not observations:
            never_published = unsupported_currencies(
                adapter, window.from_currency, window.to_currency
            )
            if never_published is None:
                # The list that separates a permanent absence from a transient
                # one was itself unreadable, so falling through is not neutral:
                # an empty answer is short at the start of its window, which
                # lands the pair in `discarded` — and both the CLI warning and
                # `_step_crash_recovery_actions` read discarded as "the provider
                # answered, so a retry returns the same unusable value." Neither
                # half of that holds here. `failed` is the outcome whose remedy
                # is a later run, which is the only one that can still resolve.
                logger.warning(f"Exchange rate support is unknown for {pair}")
                failed.append(pair)
                FX_RATE_BACKFILL_PAIRS_TOTAL.labels(outcome="failed").inc()
                continue
            if never_published:
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
        # Only the start bound reports an empty answer, so the two lines below
        # cannot both fire on one: a pair the provider published nothing for is
        # short at one end, not at both.
        short_at_the_end = bool(storable) and not _covers_window_end(storable, window)
        if short_at_the_start:
            # Only the pair is named, for the reason the FeedError branch above
            # gives: this line reaches the durable log and an FX date is
            # classified TXN_DATE.
            logger.warning(f"Exchange rates for {pair} begin after the window does")
        if short_at_the_end:
            logger.warning(f"Exchange rates for {pair} stop before the window does")
        if short_at_the_start or short_at_the_end or len(storable) != len(observations):
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


def _core_is_built(db: Database) -> bool:
    """Whether any relation the planner reads exists.

    "Any", not "all": a partially-applied transform is already past first load,
    and treating it as unbuilt would re-hide the drift this separates out. The
    catalog is asked rather than the failed exception parsed, because the
    message differs by DuckDB version and by which relation was missing.
    """
    relations = (
        BRIDGE_CURRENCY_CONVERSIONS,
        FCT_TRANSACTIONS,
        FCT_BALANCES_DAILY,
        FCT_INVESTMENT_TRANSACTIONS,
        DIM_HOLDINGS,
    )
    placeholders = " OR ".join(
        ["(table_schema = ? AND table_name = ?)"] * len(relations)
    )
    params = [part for ref in relations for part in (ref.schema, ref.name)]
    row = db.execute(
        # Fixed predicate count, values bound; no interpolation of caller input.
        f"SELECT COUNT(*) FROM information_schema.tables WHERE {placeholders}",  # noqa: S608
        params,
    ).fetchone()
    return bool(row and row[0] > 0)


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
            SELECT to_currency, to_date
              FROM {BRIDGE_CURRENCY_CONVERSIONS.full_name}
            UNION ALL
            SELECT currency_code, balance_date
              FROM {FCT_BALANCES_DAILY.full_name}
            UNION ALL
            SELECT currency_code, trade_date
              FROM {FCT_INVESTMENT_TRANSACTIONS.full_name}
            UNION ALL
            -- A position is priced at the close its market value came from, so
            -- that close is the day it implies. `InvestmentService` converts
            -- each holding at its own `price_date` — the same rule every other
            -- converting read follows, a transaction at its transaction date
            -- and a balance at its balance date. Planning today's rate instead
            -- would fetch one the read never asks for, so a carried-forward
            -- foreign position whose close is older than today would report no
            -- combined market value immediately after a successful refresh.
            -- A null `price_date` is skipped by that same reader, so it implies
            -- no window rather than a call nothing consumes.
            --
            -- This replaces the earlier `through` contribution rather than
            -- sitting beside it. Filtering the nulls out here is what makes
            -- that safe: they were the reason to keep `through`, since a null
            -- reaching `MIN` would aggregate the currency to a null window
            -- start. Nothing is narrowed by dropping it — the window's upper
            -- bound is `through` either way, so any currency that still plans
            -- one spans today, and the only currency that now plans none is
            -- one whose every position is unpriced, which no rate can convert.
            SELECT currency_code, price_date
              FROM {DIM_HOLDINGS.full_name}
             WHERE price_date IS NOT NULL
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
        [home],
    ).fetchall()
    windows: list[RateWindow] = []
    unusable = 0
    future_dated = 0
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
            future_dated += 1
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
    if future_dated:
        # Counted for the same reason as the skip above: both drop a currency
        # before it becomes a pair, so a profile whose only foreign rows sit
        # past `through` plans nothing and reports exactly what a profile
        # needing no rates at all reports. Only the count rides the line — the
        # code is untrusted source data and its date is classified TXN_DATE.
        logger.warning(
            f"Rate backfill skipped {future_dated} currency code(s) "
            "dated after the window"
        )
        FX_RATE_BACKFILL_PAIRS_TOTAL.labels(outcome="future_dated").inc(future_dated)
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
    trading day before it — and the request deliberately reaches that far back
    rather than waiting to be offered it, so this bound is the span asked for
    rather than a tolerance around a narrower one.
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

    The slack the lower bound allows does not run in this direction, and reading
    it forward would be wrong: nothing prices a date from a later observation.
    ``CurrencyService.resolve_rate`` refuses one dated after the day asked about
    outright, and its cache path tries only that exact day and the last
    publication day *before* it. A series whose first rate lands even a few days
    into the window therefore leaves every date ahead of it needing a live fetch,
    and offline it fails outright — which is the shortfall this reports, so the
    bound is the window's own opening day. The request reaching back over the
    publication slack is what keeps that strictness from firing on a window that
    opens on a closed market; ``run_rate_backfill`` explains why it does.

    An empty set never covers anything — zero coverage is the total case of the
    same shortfall, not a separate one.
    """
    if not stored:
        return False
    return min(observation.rate_date for observation in stored) <= window.start


def _covers_window_end(stored: Sequence[RateObservation], window: RateWindow) -> bool:
    """Whether the rates being kept reach forward to near the day ``window`` closes.

    The mirror of the bound above, and needed for the same reason: a series that
    stopped being published — or a stale proxy replaying an old copy of the
    range — sends only rates that are in range and storable, so nothing is
    dropped and no length comparison notices the missing tail.

    Not exact, though, and the asymmetry is deliberate rather than an oversight
    to tidy up later. A missing *leading* date is permanent: the window opens at
    the profile's earliest row, so it reopens only when older data is imported.
    A missing *trailing* date is routine and self-healing: the provider has no
    rate for a weekend and often none for today until the afternoon, while the
    window's end moves forward on its own, so the next refresh collects it. An
    exact bound here would warn on every healthy profile. Allowing the span a
    backward resolution may cross catches a series that has genuinely stopped
    and stays quiet through an ordinary publication lag.

    An empty set is left to the bound above rather than reported twice.
    """
    if not stored:
        return False
    earliest_covered_end = window.end - timedelta(days=MAX_BACKWARD_RESOLUTION_DAYS)
    return max(observation.rate_date for observation in stored) >= earliest_covered_end
