"""Frankfurter ECB reference-rate adapter.

Keyless, like CoinGecko — no SecretStore, no token command. Only a currency pair
and a date leave the machine; no amount is ever sent, so the request carries no
information about what the user holds.

Response shapes recorded from the live API on 2026-08-14:

- ``/v1/2026-03-13?base=USD&symbols=EUR`` →
  ``{"amount":1.0,"base":"USD","date":"2026-03-13","rates":{"EUR":0.87138}}``
- the Sunday ``/v1/2026-03-15`` → the same body, ``"date":"2026-03-13"``
- ``symbols=XXX`` → 404; ``/v1/1990-06-01`` → 404 (the series starts 1999-01-04)
- ``base=USD&symbols=USD`` → 422, which is why an identity pair short-circuits
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date
from decimal import Decimal

import httpx

from moneybin.connectors._http import DEFAULT_TIMEOUT, fetch_json
from moneybin.connectors.rates.errors import (
    RATE_FEED_ERRORS,
    RateFeedAPIError,
    RateFeedNotFoundError,
)
from moneybin.connectors.rates.protocol import RateObservation

logger = logging.getLogger(__name__)

FRANKFURTER_BASE_URL = "https://api.frankfurter.dev/v1"
FRANKFURTER_SOURCE = "frankfurter"
_IS_CURRENCY_CODE = re.compile(r"[A-Z]{3}")


class FrankfurterRateAdapter:
    """Daily ECB reference rates, one pair and one date per call."""

    source_type = FRANKFURTER_SOURCE

    _sleep = staticmethod(time.sleep)

    def __init__(self, client: httpx.Client | None = None) -> None:
        """Initialize with an optional injected HTTP client. No credential."""
        self._client = client or httpx.Client(timeout=DEFAULT_TIMEOUT)
        self._supported: frozenset[str] | None = None

    def supported_currencies(self) -> frozenset[str]:
        """The codes this provider publishes at all, on any date.

        Lets a caller tell a permanent condition from a daily one. `fetch`
        answers None for both an unsupported currency and a date the provider
        has no row for, and the remedies differ: the first needs a manual
        override, the second needs a different date. Consulting this is how a
        caller says which one happened.

        Raises rather than answering an empty set when it cannot read the list —
        an empty set would mean "no currency is supported anywhere", which sends
        every pair to the override table over one dropped connection.
        """
        if self._supported is None:
            self._supported = self._read_supported()
        return self._supported

    def _read_supported(self) -> frozenset[str]:
        payload = fetch_json(
            self._client,
            f"{FRANKFURTER_BASE_URL}/currencies",
            params={},
            sleep=self._sleep,
            errors=RATE_FEED_ERRORS,
        )
        if not isinstance(payload, dict):
            raise RateFeedAPIError(
                "Exchange rate feed did not answer with a currency list"
            )
        codes: dict[str, object] = payload
        published = frozenset(code.upper() for code in codes)
        # A dict is not yet a catalog. `{}` is the empty set this method exists
        # to never return, and `{"message": "..."}` is a status payload whose one
        # key would become the whole published set — both arrive with a 200, so
        # the transport check above cannot see either. Every ISO 4217 alphabetic
        # code is exactly three letters, which is what separates a catalog from
        # prose; a real one carries ~30 entries, so requiring all keys to match
        # costs nothing and refuses a body that is only partly a catalog.
        if not published or not all(_IS_CURRENCY_CODE.fullmatch(c) for c in published):
            raise RateFeedAPIError(
                "Exchange rate feed did not answer with a currency list"
            )
        return published

    def fetch(
        self, from_currency: str, to_currency: str, on: date
    ) -> RateObservation | None:
        """Return one rate, or None when the provider publishes no such series."""
        base = from_currency.upper()
        quote = to_currency.upper()
        if base == quote:
            return RateObservation(
                from_currency=base,
                to_currency=quote,
                rate_date=on,
                rate=Decimal(1),
                source_type=FRANKFURTER_SOURCE,
            )

        try:
            payload = fetch_json(
                self._client,
                f"{FRANKFURTER_BASE_URL}/{on.isoformat()}",
                params={"base": base, "symbols": quote},
                sleep=self._sleep,
                errors=RATE_FEED_ERRORS,
            )
        except RateFeedNotFoundError:
            # The provider answers 404 for both an unsupported currency and a
            # date before its series begins. Either way it has no row, which is
            # the caller's None, not a failure.
            #
            # The date is deliberately absent: this line reaches the durable
            # cli_YYYY-MM-DD.log, and `FxRatePayload` classifies an FX date
            # TXN_DATE. The pair stays — a currency code is CURRENCY, which
            # discloses no amount — and it is what makes the line worth keeping.
            logger.info(f"No {base}/{quote} series published for the requested date")
            return None

        # A 200 the caller cannot read is the provider's fault, and the Protocol
        # reserves None for "an absence the caller can route around". Returning
        # None for any of the four shapes below would send `_absence` looking for
        # an unsupported currency, find both supported, and tell the user no rate
        # was published that day — so the offered remedy (a nearby date, or your
        # own override) cannot work and the corrupt response never surfaces.
        if not isinstance(payload, dict):
            raise RateFeedAPIError("Exchange rate feed returned a non-object body")
        fields: dict[str, object] = payload

        # A body that parses cleanly can still be about a different question.
        # Both `base` and `amount` are request parameters the provider echoes
        # back, so a proxy, a redirect, or a cached answer can return a
        # well-formed rate for a pair or a unit count MoneyBin never asked for.
        # Nothing downstream can notice: the rate is labelled with the requested
        # pair, `raw.exchange_rates` is append-only, and every later conversion
        # for that date reads the cache. Absent is trusted and present-but-wrong
        # is not, the same split `_as_date` makes — a provider that omits the
        # field cannot be checked, and refusing it would reject a shape the
        # Protocol allows.
        echoed = fields.get("base")
        if echoed is not None and (
            not isinstance(echoed, str) or echoed.upper() != base
        ):
            raise RateFeedAPIError(
                "Exchange rate feed answered for a different base currency"
            )
        amount = fields.get("amount")
        if amount is not None and _as_exact_decimal(amount) != 1:
            raise RateFeedAPIError(
                "Exchange rate feed priced something other than one unit"
            )

        rates = fields.get("rates")
        if not isinstance(rates, dict):
            raise RateFeedAPIError(
                "Exchange rate feed response carried no 'rates' object"
            )
        quoted: dict[str, object] = rates

        # Absent key is the one genuine absence in this block: the provider
        # answered, and simply prices no such series.
        if quote not in quoted:
            return None
        rate = _as_exact_decimal(quoted[quote])
        if rate is None:
            raise RateFeedAPIError(
                "Exchange rate feed answered with a rate it could not read"
            )
        rate_date = _as_date(fields.get("date"), default=on)
        if rate_date is None:
            raise RateFeedAPIError(
                "Exchange rate feed answered with an unreadable date"
            )

        return RateObservation(
            from_currency=base,
            to_currency=quote,
            rate_date=rate_date,
            rate=rate,
            source_type=FRANKFURTER_SOURCE,
        )

    def fetch_range(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> tuple[RateObservation, ...]:
        """Every rate the provider publishes for the pair between two dates.

        One call covers the whole span — a request for the full series since
        1999 answered with 7,071 days in a single body, so a first backfill
        never needs chunking.

        Dates come from the response's own keys, never from the requested range.
        The provider omits days it did not publish rather than sending a null,
        and resolves a closed day backward, so a single Saturday comes back
        under the preceding Friday's key. Reading the keys is what keeps
        `raw.exchange_rates` an honest record of what was published.
        """
        base = from_currency.upper()
        quote = to_currency.upper()
        if base == quote:
            # 422 on an identity pair, so it must not reach the network. One row
            # for the start date is enough: `resolve_rate` short-circuits an
            # identity pair itself and never consults the cache for one.
            return (
                RateObservation(
                    from_currency=base,
                    to_currency=quote,
                    rate_date=start,
                    rate=Decimal(1),
                    source_type=FRANKFURTER_SOURCE,
                ),
            )

        try:
            payload = fetch_json(
                self._client,
                f"{FRANKFURTER_BASE_URL}/{start.isoformat()}..{end.isoformat()}",
                params={"base": base, "symbols": quote},
                sleep=self._sleep,
                errors=RATE_FEED_ERRORS,
            )
        except RateFeedNotFoundError:
            # 404 answers three different absences here — an unsupported
            # currency, a range entirely before the series began, and a range
            # whose start is after its end. All three mean the provider has no
            # rows to give, which is the caller's empty result, not a failure.
            logger.info(f"No {base}/{quote} series published for the requested range")
            return ()

        if not isinstance(payload, dict):
            raise RateFeedAPIError("Exchange rate feed returned a non-object body")
        fields: dict[str, object] = payload

        # `base` and `amount` are echoed request parameters, so a proxy, a
        # redirect, or a cached answer can return well-formed rates for a pair
        # MoneyBin never asked for — and nothing downstream could notice, since
        # the rows would be labelled with the requested pair in an append-only
        # table. Same check, and same reasoning, as `fetch`.
        echoed = fields.get("base")
        if echoed is not None and (
            not isinstance(echoed, str) or echoed.upper() != base
        ):
            raise RateFeedAPIError(
                "Exchange rate feed answered for a different base currency"
            )
        amount = fields.get("amount")
        if amount is not None and _as_exact_decimal(amount) != 1:
            raise RateFeedAPIError(
                "Exchange rate feed priced something other than one unit"
            )

        rates = fields.get("rates")
        if not isinstance(rates, dict):
            raise RateFeedAPIError(
                "Exchange rate feed response carried no 'rates' object"
            )
        by_date: dict[str, object] = rates

        observations: list[RateObservation] = []
        for raw_day in sorted(by_date):
            rate_date = _as_date(raw_day, default=start)
            if rate_date is None:
                raise RateFeedAPIError(
                    "Exchange rate feed answered with an unreadable date"
                )
            rate = _quoted_rate(by_date[raw_day], quote)
            if rate is None:
                continue
            observations.append(
                RateObservation(
                    from_currency=base,
                    to_currency=quote,
                    rate_date=rate_date,
                    rate=rate,
                    source_type=FRANKFURTER_SOURCE,
                )
            )
        return tuple(observations)


def _quoted_rate(quoted: object, quote: str) -> Decimal | None:
    """Read one currency's rate out of a `rates` entry, or None if absent.

    Raises on a present-but-unreadable number for `fetch_range`'s reasons: the
    cache is append-only, so a rate stored wrong cannot be corrected in place.
    """
    if not isinstance(quoted, dict):
        raise RateFeedAPIError("Exchange rate feed returned an unreadable rates entry")
    per_currency: dict[str, object] = quoted
    if quote not in per_currency:
        return None
    rate = _as_exact_decimal(per_currency[quote])
    if rate is None:
        raise RateFeedAPIError(
            "Exchange rate feed answered with a rate it could not read"
        )
    return rate


def _as_exact_decimal(raw: object) -> Decimal | None:
    """Convert a parsed JSON number to Decimal without ever going via float.

    The helper's `parse_float=Decimal` covers `0.87138` but not `109`, because
    a JSON integer never reaches parse_float. Accepting int as well keeps a
    whole-number rate — plausible on JPY or KRW — from reading as a broken feed.
    bool is excluded explicitly: it is an int in Python, so `true` would
    otherwise become a rate of exactly 1.

    None means "present but unreadable", never "absent" — the caller checks for
    the key first and raises on the None this returns.
    """
    if isinstance(raw, Decimal):
        return raw
    if isinstance(raw, int) and not isinstance(raw, bool):
        return Decimal(raw)
    return None


def _as_date(raw: object, *, default: date) -> date | None:
    """Read the provider's resolved business day, or None if it is unreadable.

    An absent field falls back to the day asked for — that is the shape a
    provider without weekend resolution would send. A *present but unreadable*
    one does not: filing the rate under the requested day would record a rate on
    a day the provider never published, which is the failure storing the
    resolved day exists to prevent.
    """
    if raw is None:
        return default
    if not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None
