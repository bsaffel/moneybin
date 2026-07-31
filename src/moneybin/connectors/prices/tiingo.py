"""Tiingo end-of-day prices for equities, ETFs, and mutual funds.

Tiingo was chosen over exchange/SIP-derived providers on one axis: it publishes
open-end mutual fund NAVs, which dominate retirement accounts and have no
exchange print to derive a price from. Its documentation states that for a fund,
"the fields open, high, low, close will contain the NAV value for the given
day" — so the same field this adapter already reads carries funds too, with no
special case.

It also publishes `close` and `adjClose` as separate documented fields, which is
what lets the adapter *declare* price_basis = 'raw' instead of inferring it.

https://www.tiingo.com/documentation/end-of-day
"""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol

import httpx

from moneybin.connectors.prices._http import DEFAULT_TIMEOUT, fetch_json
from moneybin.connectors.prices.errors import (
    PriceFeedAuthError,
    PriceFeedNotFoundError,
)
from moneybin.connectors.prices.protocol import (
    PriceFetchFailure,
    PriceFetchResult,
    PriceObservation,
    SecurityRef,
)
from moneybin.secrets import TIINGO_API_TOKEN_KEY, SecretNotFoundError

logger = logging.getLogger(__name__)

TIINGO_BASE_URL = "https://api.tiingo.com"


@dataclass(frozen=True, slots=True)
class TickerMetadata:
    """What Tiingo says a symbol is, used to verify a feed key rather than assume it.

    Only the two fields a binding decision needs. Tiingo's meta response also
    carries `description`, `startDate`, and `endDate`; none of them discriminates
    between two listings sharing a symbol, so none is read.
    """

    name: str
    exchange_code: str | None


class SecretReader(Protocol):
    """The one method this adapter needs from SecretStore.

    Mirrors gsheet's OAuthCredentialsProvider: a thin Protocol keeps the
    adapter's tests free of the keychain without a production seam.
    """

    def get_key(self, name: str) -> str:
        """Return the stored secret, raising SecretNotFoundError if absent."""
        ...


class TiingoPriceAdapter:
    """Pulls end-of-day closes from Tiingo, one request per security."""

    source_type = "tiingo"
    # Tiingo documents `close` as the as-traded close beside a separate
    # `adjClose` (CRSP methodology, split *and* dividend). Reading `close` is
    # therefore raw by the provider's own definition, not by inference.
    price_basis = "raw"

    # Test hook — overridable so a backoff assertion costs no wall clock.
    _sleep = staticmethod(time.sleep)

    def __init__(
        self,
        secrets: SecretReader,
        client: httpx.Client | None = None,
    ) -> None:
        """Initialize with a secret reader and an optional injected HTTP client."""
        self._secrets = secrets
        self._client = client or httpx.Client(timeout=DEFAULT_TIMEOUT)

    def fetch(
        self, securities: Sequence[SecurityRef], start: date, end: date
    ) -> PriceFetchResult:
        """Return closes for `securities` over the inclusive window."""
        headers = {"Authorization": f"Token {self._token()}"}
        params = {
            "startDate": start.isoformat(),
            "endDate": end.isoformat(),
            "format": "json",
        }
        observations: list[PriceObservation] = []
        failures: list[PriceFetchFailure] = []
        for ref in securities:
            try:
                bars = fetch_json(
                    self._client,
                    f"{TIINGO_BASE_URL}/tiingo/daily/{ref.provider_security_key}/prices",
                    params=params,
                    headers=headers,
                    sleep=self._sleep,
                )
            except PriceFeedNotFoundError as exc:
                # The only condition that describes this security rather than the
                # run. Everything else — a rejected token, an exhausted quota, an
                # unreachable host, a 5xx — is wrong for every security in the
                # batch, so retrying the remaining 39 only burns the rate limit
                # before failing identically. Whole-batch conditions propagate to
                # PriceService, which contains them per source.
                failures.append(PriceFetchFailure(ref.provider_security_key, str(exc)))
                continue
            priced = self._observations(ref, bars)
            if not priced:
                # An empty series for a security the caller believes it holds is
                # the signal that drives the held-but-unpriced doctor check —
                # silently skipping it is how a position stays unpriced unnoticed.
                failures.append(
                    PriceFetchFailure(
                        ref.provider_security_key,
                        "Tiingo returned no priced observation in the requested window",
                    )
                )
                continue
            observations.extend(priced)
        return PriceFetchResult(tuple(observations), tuple(failures))

    def fetch_metadata(self, ticker: str) -> TickerMetadata | None:
        """What Tiingo says this symbol is, or ``None`` if it does not know it.

        A ticker is not an identifier: the same symbol names different securities
        across exchanges, share classes collide, and symbols are recycled after a
        delisting. This is the independent signal that lets a feed key bind on
        evidence instead of on the symbol string alone.

        ``None`` is not an error — it means no Tiingo series covers this security,
        which is a valuation gap for the held-but-unpriced check to surface, not
        a failed request. Only a 404 earns it: a quota exhaustion or a 5xx says
        nothing about coverage, and answering ``None`` there would report every
        held security as unsupported while hiding the one condition the user can
        actually act on.
        """
        try:
            body = fetch_json(
                self._client,
                f"{TIINGO_BASE_URL}/tiingo/daily/{ticker}",
                params={},
                headers={"Authorization": f"Token {self._token()}"},
                sleep=self._sleep,
            )
        except PriceFeedNotFoundError:
            return None
        if not isinstance(body, dict):
            return None
        fields: dict[str, object] = body
        name = fields.get("name")
        exchange = fields.get("exchangeCode")
        if not isinstance(name, str) or not name.strip():
            return None
        return TickerMetadata(
            name=name.strip(),
            exchange_code=exchange.strip() if isinstance(exchange, str) else None,
        )

    def _token(self) -> str:
        try:
            return self._secrets.get_key(TIINGO_API_TOKEN_KEY)
        except SecretNotFoundError as exc:
            raise PriceFeedAuthError(
                "No Tiingo API token is stored. Tiingo's free tier covers 1,000 "
                "requests/day; create a token at tiingo.com and store it with "
                "'moneybin investments prices token'."
            ) from exc

    def _observations(self, ref: SecurityRef, bars: object) -> list[PriceObservation]:
        """Turn a recorded end-of-day series into stored-shape rows."""
        if not isinstance(bars, list):
            logger.warning(
                f"Tiingo returned {type(bars).__name__}, not a series, for one security"
            )
            return []
        series: list[object] = bars
        rows: list[PriceObservation] = []
        for bar in series:
            parsed = _parse_bar(bar)
            if parsed is None:
                continue
            price_date, close = parsed
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


def _parse_bar(bar: object) -> tuple[date, Decimal] | None:
    """Read one bar's date and as-traded close, or None if unusable.

    Never reads `adjClose`: an adjusted series stops being correctly adjusted
    after the next corporate action, and core.fct_security_prices values
    holdings from price_basis = 'raw' exclusively.
    """
    if not isinstance(bar, dict):
        return None
    fields: dict[str, object] = bar
    raw_date = fields.get("date")
    raw_close = fields.get("close")
    if not isinstance(raw_date, str) or not isinstance(raw_close, Decimal | int):
        return None
    close = Decimal(raw_close)
    # raw.security_prices CHECKs close > 0 and is append-only with
    # on_conflict="ignore", so a zero row would squat its primary key and every
    # later corrected close for that date would be dropped in silence.
    if close <= 0:
        return None
    try:
        # Tiingo's documented field list does not pin the serialization;
        # fromisoformat reads both '2026-07-23' and '2026-07-23T00:00:00.000Z'.
        parsed_date = datetime.fromisoformat(raw_date).date()
    except ValueError:
        return None
    return parsed_date, close
