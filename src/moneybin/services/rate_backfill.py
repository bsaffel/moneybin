"""Plan and fill the exchange-rate coverage a profile's own data implies.

Display conversion prices each row at its own date, so coverage is a span, not a
point. Gathering it belongs to refresh rather than to a report read: refresh
already holds the exclusive per-profile writer lock and is already slow, while a
report that fetched would take that lock behind a read-only-looking command and
fail whenever a sync held it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from moneybin.connectors.feed_errors import FeedError
from moneybin.connectors.rates.protocol import RateAdapter
from moneybin.database import Database
from moneybin.metrics.registry import FX_RATE_BACKFILL_PAIRS_TOTAL
from moneybin.services.currency_service import CurrencyService, canonical_currency
from moneybin.tables import (
    DIM_HOLDINGS,
    EXCHANGE_RATES,
    FCT_BALANCES_DAILY,
    FCT_INVESTMENT_TRANSACTIONS,
    FCT_TRANSACTIONS,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RateBackfillResult:
    """What one backfill pass managed to gather.

    ``pairs_failed`` names pairs whose provider call raised — an outage or a
    malformed body — and never a pair the provider simply does not publish.
    Those two need different remedies: the first resolves itself on the next
    refresh, the second needs a manual override, and collapsing them would send
    a user to the override table over a dropped connection.
    """

    rates_written: int
    pairs_failed: tuple[str, ...]


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

    The store is deliberately outside that isolation. Its failures are not
    per-pair — refresh holds the exclusive writer lock, so a write that raises
    here means the database itself is unusable, and attributing that to the
    currency being stored would report a lie. Such a failure aborts the step and
    surfaces through ``_run_rates_step`` as "the step did not run", which is what
    actually happened.
    """
    windows = plan_rate_backfill(db, home_currency=home_currency, through=through)
    service = CurrencyService(db, adapter=adapter)
    FX_RATE_BACKFILL_PAIRS_TOTAL.labels(outcome="planned").inc(len(windows))

    written = 0
    failed: list[str] = []
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
        written += service.store_observations(observations)

    logger.info(
        f"Rate backfill: {len(windows)} pair(s) planned, {written} rate(s) written, "
        f"{len(failed)} pair(s) failed"
    )
    return RateBackfillResult(rates_written=written, pairs_failed=tuple(failed))


def plan_rate_backfill(
    db: Database, *, home_currency: str, through: date
) -> tuple[RateWindow, ...]:
    """The rate windows this profile's own rows imply, newest bound at ``through``."""
    home = canonical_currency(home_currency)
    # Coverage is the span `[covered.oldest, covered.newest]`, and both bounds
    # are load-bearing. `newest + 1` is what keeps a refresh from re-requesting
    # a span it already holds; `oldest` is what keeps a *newly implied* earlier
    # date from being silently skipped — a single row cached by `moneybin fx
    # rate`, or a statement imported years after the first backfill, leaves the
    # newest bound at today while the earliest needed date moves backwards, and
    # a newest-only model would plan a window starting tomorrow and drop it.
    # Neither bound reads the set of dates *within* the span: a reference series
    # has legitimate holes on every weekend and holiday, so a date-set model
    # would treat each one as missing coverage and re-request it forever.
    #
    # An earlier need therefore re-requests the whole span in one call. The
    # append-only cache ignores the rows it already holds, and the next refresh
    # is back on the `newest + 1` path, so this costs one extra call per pair
    # per backwards extension.
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
            -- A position is a rebuilt snapshot with no date column, so the only
            -- day it implies is the one being valued. Widening it to the whole
            -- history would be a guess; narrowing it to nothing would leave a
            -- foreign holding unconvertible in a profile that synced positions
            -- but no investment ledger.
            SELECT currency_code, ?
              FROM {DIM_HOLDINGS.full_name}
        ),
        needed AS (
            -- TRIM as well as UPPER: `canonical_currency` strips before it
            -- uppercases, so a padded code would otherwise reach `covered`
            -- under a spelling no stored rate carries and re-request its whole
            -- span every refresh.
            SELECT UPPER(TRIM(currency_code)) AS from_currency,
                   MIN(on_date) AS earliest
              FROM dated
             WHERE currency_code IS NOT NULL AND UPPER(TRIM(currency_code)) <> ?
             GROUP BY 1
        ),
        covered AS (
            SELECT from_currency,
                   MIN(rate_date) AS oldest,
                   MAX(rate_date) AS newest
              FROM {EXCHANGE_RATES.full_name}
             WHERE to_currency = ?
             GROUP BY 1
        )
        SELECT n.from_currency,
               CASE
                   WHEN c.newest IS NULL OR n.earliest < c.oldest THEN n.earliest
                   ELSE GREATEST(n.earliest, c.newest + 1)
               END
          FROM needed AS n
          LEFT JOIN covered AS c USING (from_currency)
         ORDER BY n.from_currency
        """,  # noqa: S608  # TableRef + parameterized values
        [through, home, home],
    ).fetchall()
    return tuple(
        RateWindow(
            from_currency=canonical_currency(str(row[0])),
            to_currency=home,
            start=row[1],
            end=through,
        )
        for row in rows
        # A pair cached through `through` needs nothing. Dropping it here rather
        # than letting it out as a backwards range matters because the provider
        # answers a reversed range 404 — the same status it uses for a currency
        # it does not publish — so a fully-covered pair would be reported as an
        # unsupported one, every refresh, forever.
        if row[1] <= through
    )
