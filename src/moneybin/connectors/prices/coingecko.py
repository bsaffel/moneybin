"""CoinGecko spot prices for crypto, keyed by the coingecko_id on app.securities.

Spot crypto has no splits or dividends, so its prices are raw by construction.

Two response facts below were verified against the live keyless API on
2026-07-24 because the documentation states neither. Both shape this adapter,
and re-deriving them from the docs alone is not possible:

- ``/coins/{id}/market_chart/range`` answers **401** without a key, while
  ``/coins/{id}/market_chart?days=N`` answers 200. So this adapter uses the
  ``days`` form even though ``from``/``to`` would express the window directly.
  ``/coins/{id}/ohlc/range`` — which would give a true daily close and remove
  the convention question below — is Analyst-plan-and-above.
- **Granularity varies with window width.** ``days=200`` returned points spaced
  86_400_000 ms at exact midnight UTC; ``days=5`` returned points spaced
  3_600_000 ms. The documented auto-granularity rule (>90 days daily, 2-90 days
  hourly) matches. This adapter therefore keeps only the exact-midnight point
  per date and drops the rest, so a stored price is a pure function of its date
  rather than of the window that happened to be requested. Without that filter a
  30-day refresh and a 365-day backfill would write *different* prices for the
  same date into an append-only table whose on_conflict="ignore" makes the first
  writer win permanently and silently.

https://docs.coingecko.com/reference/coins-id-market-chart
"""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal

import httpx

from moneybin.connectors.prices._http import DEFAULT_TIMEOUT, fetch_json
from moneybin.connectors.prices.errors import (
    PriceFeedNotFoundError,
    PriceFeedWindowUnsupportedError,
)
from moneybin.connectors.prices.protocol import (
    PriceFetchFailure,
    PriceFetchResult,
    PriceObservation,
    SecurityRef,
)

logger = logging.getLogger(__name__)

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
# The keyless tier serves at most a year of history. Asking for more returns the
# same year, so the adapter refuses rather than letting the caller believe a
# deeper backfill happened.
COINGECKO_MAX_HISTORY_DAYS = 365

_EPOCH = date(1970, 1, 1)
_MS_PER_DAY = 86_400_000


class CoinGeckoPriceAdapter:
    """Pulls daily crypto spot prices from CoinGecko, one request per coin."""

    source_type = "coingecko"
    price_basis = "raw"

    # Test hook — overridable so a backoff assertion costs no wall clock.
    _sleep = staticmethod(time.sleep)

    def __init__(self, client: httpx.Client | None = None) -> None:
        """Initialize with an optional injected HTTP client. No credential."""
        self._client = client or httpx.Client(timeout=DEFAULT_TIMEOUT)

    def fetch(
        self, securities: Sequence[SecurityRef], start: date, end: date
    ) -> PriceFetchResult:
        """Return one close per date per coin over the inclusive window."""
        observations: list[PriceObservation] = []
        failures: list[PriceFetchFailure] = []
        for ref in securities:
            params = {
                "vs_currency": ref.quote_currency.lower(),
                "days": str(_days_to_request(start)),
            }
            try:
                body = fetch_json(
                    self._client,
                    f"{COINGECKO_BASE_URL}/coins/{ref.provider_security_key}/market_chart",
                    params=params,
                    sleep=self._sleep,
                )
            except PriceFeedNotFoundError as exc:
                # Matches the Tiingo adapter: a slug CoinGecko does not know is
                # the one condition about this coin alone. Everything else — an
                # exhausted keyless quota, an unreachable host, a 5xx, or a
                # 401/403 if CoinGecko ever reuses those codes for a paid tier —
                # is wrong for every coin in the batch, and walking the rest only
                # burns the rate limit before failing identically. Whole-batch
                # conditions propagate to PriceService's per-source containment.
                failures.append(PriceFetchFailure(ref.provider_security_key, str(exc)))
                continue
            priced = self._observations(ref, body, start, end)
            if not priced:
                failures.append(
                    PriceFetchFailure(
                        ref.provider_security_key,
                        "CoinGecko returned no daily close in the requested window",
                    )
                )
                continue
            observations.extend(priced)
        return PriceFetchResult(tuple(observations), tuple(failures))

    def _observations(
        self, ref: SecurityRef, body: object, start: date, end: date
    ) -> list[PriceObservation]:
        if not isinstance(body, dict):
            logger.warning("CoinGecko returned a non-object body for one coin")
            return []
        fields: dict[str, object] = body
        points = fields.get("prices")
        if not isinstance(points, list):
            logger.warning("CoinGecko response carried no 'prices' array for one coin")
            return []
        series: list[object] = points
        rows: list[PriceObservation] = []
        for point in series:
            parsed = _parse_point(point)
            if parsed is None:
                continue
            price_date, close = parsed
            if not start <= price_date <= end:
                # `days` is anchored to now, not to `end`, so a response
                # routinely overshoots the window the caller asked for. Storing
                # the overshoot would write rows no report reads.
                continue
            rows.append(
                PriceObservation(
                    provider_security_key=ref.provider_security_key,
                    price_date=price_date,
                    quote_currency=ref.quote_currency,
                    close=close,
                    source_type=self.source_type,
                    price_basis=self.price_basis,
                )
            )
        return rows


def _days_to_request(start: date) -> int:
    """How many days back to ask for, given the earliest date wanted.

    The `days` parameter counts back from now rather than from the window's end,
    so the span is measured against today and the response is filtered after.

    Refuses rather than clamps a span the keyless tier cannot serve. Clamping
    silently substituted 365 days for whatever was asked, wrote those rows, and
    reported an ordinary success — so a deeper `--since` came back looking
    complete, and the only place the difference was visible was CoinGecko's
    documentation. Since the whole batch shares one window, this leaves as a
    whole-batch error and `pull` contains it per source.
    """
    span = (date.today() - start).days + 1
    if span > COINGECKO_MAX_HISTORY_DAYS:
        earliest = date.today() - timedelta(days=COINGECKO_MAX_HISTORY_DAYS - 1)
        raise PriceFeedWindowUnsupportedError(
            f"CoinGecko's keyless endpoint serves at most "
            f"{COINGECKO_MAX_HISTORY_DAYS} days of history, and {start.isoformat()} "
            f"is further back than that. Re-run with a start date of "
            f"{earliest.isoformat()} or later for the deepest crypto history "
            f"available; other price sources are unaffected."
        )
    return max(1, span)


def _parse_point(point: object) -> tuple[date, Decimal] | None:
    """Read one [timestamp_ms, price] pair, or None if unusable.

    A point at exactly 00:00:00 UTC on date D is the value at the *end* of D-1,
    so it is stored against D-1. That keeps price_date meaning "the value at the
    end of this date" for crypto exactly as it does for every equity close in
    the table. Labelling it D instead would blend a Friday equity close with
    Thursday-end crypto inside one portfolio total, and report that row's
    staleness as zero days when it is really a day old.

    The cost, accepted deliberately: today's crypto is never priced (that needs
    tomorrow's midnight point), and a stored row disagrees by one day with
    CoinGecko's own /history?date=D page, which labels the same snapshot D.
    """
    # json.loads renders a JSON array as a list, never a tuple.
    if not isinstance(point, list):
        return None
    pair: list[object] = point
    if len(pair) != 2:
        return None
    raw_ts, raw_price = pair
    if not isinstance(raw_ts, int) or isinstance(raw_ts, bool):
        return None
    if not isinstance(raw_price, Decimal | int) or isinstance(raw_price, bool):
        return None
    # Only the exact midnight boundary is a day's close. An hourly window carries
    # 24 points per date, and picking any other one makes the stored value depend
    # on how wide a window was requested.
    if raw_ts % _MS_PER_DAY != 0:
        return None
    price = Decimal(raw_price)
    # raw.security_prices CHECKs close > 0 and is append-only, so a zero row
    # would squat its primary key and block every later correction for that date.
    if price <= 0:
        return None
    boundary = _EPOCH + timedelta(days=raw_ts // _MS_PER_DAY)
    return boundary - timedelta(days=1), price
