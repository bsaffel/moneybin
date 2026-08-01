"""Tests for the Tiingo end-of-day price adapter.

Every response body here is a recorded shape, served through respx. No test in
this module opens a socket, and none needs a Tiingo token.

Field names follow Tiingo's documented end-of-day response: date, open, high,
low, close, volume, adjOpen, adjHigh, adjLow, adjClose, adjVolume, divCash,
splitFactor. https://www.tiingo.com/documentation/end-of-day
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import httpx
import pytest
import respx

from moneybin.connectors.prices.errors import (
    PriceFeedAPIError,
    PriceFeedAuthError,
    PriceFeedRateLimitError,
)
from moneybin.connectors.prices.protocol import SecurityRef
from moneybin.connectors.prices.tiingo import TIINGO_BASE_URL, TiingoPriceAdapter

_TOKEN = "tiingo-test-token"  # noqa: S105  # fixture value, not a real credential
_START = date(2026, 7, 22)
_END = date(2026, 7, 24)


class _StubSecrets:
    """Minimal SecretReader: returns a token, or raises like SecretStore does."""

    def __init__(self, token: str | None = _TOKEN) -> None:
        self._token = token

    def get_key(self, name: str) -> str:
        from moneybin.secrets import SecretNotFoundError

        if self._token is None:
            raise SecretNotFoundError(name)
        return self._token


def _adapter(token: str | None = _TOKEN) -> TiingoPriceAdapter:
    return TiingoPriceAdapter(secrets=_StubSecrets(token))


def _bar(
    *,
    day: str = "2026-07-23T00:00:00.000Z",
    close: float | bool = 212.55,
    adj_close: float = 205.30,
) -> dict[str, object]:
    """One recorded end-of-day bar. adjClose deliberately differs from close."""
    return {
        "date": day,
        "open": 211.00,
        "high": 213.00,
        "low": 210.10,
        "close": close,
        "volume": 50_000_000,
        "adjOpen": 203.90,
        "adjHigh": 205.80,
        "adjLow": 203.00,
        "adjClose": adj_close,
        "adjVolume": 50_000_000,
        "divCash": 0.0,
        "splitFactor": 1.0,
    }


def _ref(key: str = "AAPL", currency: str = "USD") -> SecurityRef:
    return SecurityRef(provider_security_key=key, quote_currency=currency)


def _prices_route(ticker: str = "AAPL") -> str:
    return f"{TIINGO_BASE_URL}/tiingo/daily/{ticker}/prices"


@respx.mock
def test_it_reads_close_and_never_adjclose() -> None:
    """The whole reason Tiingo was chosen: an as-traded close it can declare.

    An adjusted series stops being correctly adjusted after the next corporate
    action, so a holding valued from adjClose drifts silently.
    """
    respx.get(_prices_route()).mock(
        return_value=httpx.Response(200, json=[_bar(close=212.55, adj_close=205.30)])
    )

    result = _adapter().fetch([_ref()], _START, _END)

    assert [obs.close for obs in result.observations] == [Decimal("212.55")]


@respx.mock
def test_it_never_routes_a_close_through_a_float() -> None:
    """A close must arrive as an exact Decimal, not a float widened after the fact.

    json.loads' default float parsing turns 212.55 into
    212.55000000000001136868377216160297393798828125, and that value lands in a
    DECIMAL(28,10) column on an append-only table.
    """
    respx.get(_prices_route()).mock(
        return_value=httpx.Response(200, text=json.dumps([_bar(close=212.55)]))
    )

    result = _adapter().fetch([_ref()], _START, _END)

    close = result.observations[0].close
    assert close == Decimal("212.55")
    assert str(close) == "212.55"


@respx.mock
def test_it_declares_the_raw_basis_on_every_observation() -> None:
    """price_basis is declared by the adapter, never inferred from the data."""
    respx.get(_prices_route()).mock(return_value=httpx.Response(200, json=[_bar()]))

    result = _adapter().fetch([_ref()], _START, _END)

    assert _adapter().price_basis == "raw"
    assert [obs.price_basis for obs in result.observations] == ["raw"]
    assert [obs.source_type for obs in result.observations] == ["tiingo"]


@respx.mock
def test_it_sends_the_token_in_a_header_and_never_in_the_url() -> None:
    """A token in the query string reaches access logs and error messages."""
    route = respx.get(_prices_route()).mock(
        return_value=httpx.Response(200, json=[_bar()])
    )

    _adapter().fetch([_ref()], _START, _END)

    request = route.calls.last.request
    assert request.headers["Authorization"] == f"Token {_TOKEN}"
    assert _TOKEN not in str(request.url)


@respx.mock
def test_it_echoes_the_currency_the_caller_declared() -> None:
    """Tiingo's end-of-day response carries no currency field.

    The caller passes the catalog's currency_code, so the adapter labels the row
    from a recorded fact rather than assuming USD for every listing.
    """
    respx.get(_prices_route("BHP")).mock(
        return_value=httpx.Response(200, json=[_bar()])
    )

    result = _adapter().fetch([_ref("BHP", "AUD")], _START, _END)

    assert [obs.quote_currency for obs in result.observations] == ["AUD"]


@respx.mock
def test_it_requests_only_the_window_it_was_given() -> None:
    """A wider fetch than asked for burns rate limit and stores unread rows."""
    route = respx.get(_prices_route()).mock(
        return_value=httpx.Response(200, json=[_bar()])
    )

    _adapter().fetch([_ref()], _START, _END)

    params = route.calls.last.request.url.params
    assert params["startDate"] == "2026-07-22"
    assert params["endDate"] == "2026-07-24"


@respx.mock
def test_one_unknown_ticker_does_not_lose_the_rest_of_the_batch() -> None:
    """A refresh that loses one security reports the others plus a named reason."""
    respx.get(_prices_route("AAPL")).mock(
        return_value=httpx.Response(200, json=[_bar()])
    )
    respx.get(_prices_route("NOPE")).mock(
        return_value=httpx.Response(404, json={"detail": "Error: Ticker not found"})
    )

    result = _adapter().fetch([_ref("AAPL"), _ref("NOPE")], _START, _END)

    assert [obs.provider_security_key for obs in result.observations] == ["AAPL"]
    assert [f.provider_security_key for f in result.failures] == ["NOPE"]
    assert "404" in result.failures[0].reason


@respx.mock
def test_a_missing_token_fails_the_batch_before_any_request() -> None:
    """No token is a setup error for the whole run, not a per-security failure."""
    route = respx.get(_prices_route())

    with pytest.raises(PriceFeedAuthError, match="Tiingo"):
        _adapter(token=None).fetch([_ref()], _START, _END)

    assert not route.called


@respx.mock
def test_a_rejected_token_fails_the_batch_rather_than_each_security() -> None:
    """A 401 means the credential is wrong for every security, so stop."""
    respx.get(_prices_route()).mock(
        return_value=httpx.Response(401, json={"detail": "Invalid token"})
    )

    with pytest.raises(PriceFeedAuthError):
        _adapter().fetch([_ref("AAPL"), _ref("MSFT")], _START, _END)


@respx.mock
def test_it_retries_a_rate_limited_security_then_succeeds() -> None:
    """Backoff applies to rate-limit responses only."""
    respx.get(_prices_route()).mock(
        side_effect=[
            httpx.Response(429, json={"detail": "Rate limit exceeded"}),
            httpx.Response(200, json=[_bar()]),
        ]
    )
    adapter = _adapter()
    slept: list[float] = []
    adapter._sleep = slept.append  # type: ignore[method-assign]  # test hook

    result = adapter.fetch([_ref()], _START, _END)

    assert len(result.observations) == 1
    assert slept, "a rate-limited request must back off before retrying"


@respx.mock
def test_it_does_not_retry_a_server_error() -> None:
    """Only rate limiting is retried; a 500 is raised once, not retried.

    It propagates rather than becoming a per-security failure because a broken
    provider is not a fact about this ticker — PriceService contains it per
    source, so the other sources still write their rows.
    """
    route = respx.get(_prices_route()).mock(return_value=httpx.Response(500))
    adapter = _adapter()
    adapter._sleep = lambda _seconds: None  # type: ignore[method-assign]  # test hook

    with pytest.raises(PriceFeedAPIError):
        adapter.fetch([_ref()], _START, _END)

    assert route.call_count == 1


@respx.mock
def test_a_nonpositive_close_never_becomes_an_observation() -> None:
    """raw.security_prices CHECKs close > 0 and is append-only.

    A zero row would squat its primary key permanently, so every later corrected
    close for that date is silently dropped by on_conflict="ignore".
    """
    respx.get(_prices_route()).mock(
        return_value=httpx.Response(
            200,
            json=[
                _bar(day="2026-07-22T00:00:00.000Z", close=0.0),
                _bar(day="2026-07-23T00:00:00.000Z", close=212.55),
            ],
        )
    )

    result = _adapter().fetch([_ref()], _START, _END)

    assert [obs.price_date for obs in result.observations] == [date(2026, 7, 23)]


@respx.mock
def test_a_boolean_close_never_becomes_an_observation() -> None:
    """`bool` is a subclass of `int`, so a type check alone lets JSON `true` in.

    `Decimal(True)` is `1`, and raw.security_prices is append-only: a $1 close
    stored for a date squats that primary key, so every later correct close for
    the same date is dropped by on_conflict="ignore". The value is plausible
    enough to survive the `> 0` check and wrong by orders of magnitude.
    """
    respx.get(_prices_route()).mock(
        return_value=httpx.Response(
            200,
            json=[
                _bar(day="2026-07-22T00:00:00.000Z", close=True),
                _bar(day="2026-07-23T00:00:00.000Z", close=212.55),
            ],
        )
    )

    result = _adapter().fetch([_ref()], _START, _END)

    assert [obs.price_date for obs in result.observations] == [date(2026, 7, 23)]


@respx.mock
def test_it_reads_a_bare_iso_date_as_well_as_a_timestamp() -> None:
    """Tiingo's documented field list does not pin the date's serialization."""
    respx.get(_prices_route()).mock(
        return_value=httpx.Response(200, json=[_bar(day="2026-07-23")])
    )

    result = _adapter().fetch([_ref()], _START, _END)

    assert [obs.price_date for obs in result.observations] == [date(2026, 7, 23)]


@respx.mock
def test_an_empty_series_is_a_failure_naming_the_security() -> None:
    """A held security no feed covers must be surfaced, not silently skipped.

    dim_holdings reports it as unpriced; the refresh result is what tells the
    user which security to bind or mark by hand.
    """
    respx.get(_prices_route()).mock(return_value=httpx.Response(200, json=[]))

    result = _adapter().fetch([_ref()], _START, _END)

    assert not result.observations
    assert [f.provider_security_key for f in result.failures] == ["AAPL"]


def _meta_route(ticker: str = "AAPL") -> str:
    return f"{TIINGO_BASE_URL}/tiingo/daily/{ticker}"


@respx.mock
def test_metadata_reads_the_name_and_exchange_code() -> None:
    """The two fields that let a feed key be verified rather than assumed.

    A ticker is not an identifier — BHP names different securities on NYSE and
    ASX — so binding one needs a signal beyond the symbol itself.
    """
    respx.get(_meta_route()).mock(
        return_value=httpx.Response(
            200,
            json={
                "ticker": "AAPL",
                "name": "Apple Inc",
                "exchangeCode": "NASDAQ",
                "description": "Apple Inc. designs...",
                "startDate": "1980-12-12",
                "endDate": "2026-07-23",
            },
        )
    )

    meta = _adapter().fetch_metadata("AAPL")

    assert meta is not None
    assert (meta.name, meta.exchange_code) == ("Apple Inc", "NASDAQ")


@respx.mock
def test_metadata_returns_none_for_a_ticker_tiingo_does_not_know() -> None:
    """An unknown symbol is not an error — it means no feed covers this security."""
    respx.get(_meta_route("NOPE")).mock(
        return_value=httpx.Response(404, json={"detail": "Not found"})
    )

    assert _adapter().fetch_metadata("NOPE") is None


@respx.mock
def test_metadata_reports_an_outage_instead_of_claiming_no_coverage() -> None:
    """A rate limit is not an answer about this ticker, so it must not read as one.

    `None` from fetch_metadata means "no Tiingo series covers this security", and
    _tiingo_key turns that into the `no_provider_coverage` reason a user sees. A
    quota exhaustion answers nothing about coverage, so returning None here tells
    every held security it is unsupported and hides the one condition the user
    could actually act on.
    """
    respx.get(_meta_route()).mock(return_value=httpx.Response(429))
    adapter = _adapter()
    adapter._sleep = lambda _seconds: None  # type: ignore[method-assign]  # test hook

    with pytest.raises(PriceFeedRateLimitError):
        adapter.fetch_metadata("AAPL")


@respx.mock
def test_metadata_reports_a_broken_provider_instead_of_claiming_no_coverage() -> None:
    """Same rule for a 5xx: the provider failed, it did not answer 'unknown'."""
    respx.get(_meta_route()).mock(return_value=httpx.Response(503))

    with pytest.raises(PriceFeedAPIError):
        _adapter().fetch_metadata("AAPL")


@respx.mock
def test_a_rate_limit_stops_the_batch_instead_of_spending_it_on_every_security() -> (
    None
):
    """The quota is the shared resource, so walking on deepens the very limit hit.

    Recording a per-security failure and continuing costs RETRY_MAX more requests
    for each remaining security, all of them certain to fail the same way, and
    leaves the run reporting 40 individually-unpriced securities rather than one
    actionable "Tiingo rate limit" a user can wait out.
    """
    first = respx.get(_prices_route("AAPL")).mock(return_value=httpx.Response(429))
    second = respx.get(_prices_route("MSFT")).mock(
        return_value=httpx.Response(200, json=[_bar()])
    )
    adapter = _adapter()
    adapter._sleep = lambda _seconds: None  # type: ignore[method-assign]  # test hook

    with pytest.raises(PriceFeedRateLimitError):
        adapter.fetch([_ref("AAPL"), _ref("MSFT")], _START, _END)

    assert first.call_count == 3, "the first security still exhausts its own retries"
    assert not second.called, "a whole-batch condition must not spend the next request"


@respx.mock
def test_metadata_sends_the_token_in_a_header() -> None:
    """Same credential rule as the price path: never in the URL."""
    route = respx.get(_meta_route()).mock(
        return_value=httpx.Response(
            200, json={"ticker": "AAPL", "name": "Apple Inc", "exchangeCode": "NASDAQ"}
        )
    )

    _adapter().fetch_metadata("AAPL")

    assert route.calls.last.request.headers["Authorization"] == f"Token {_TOKEN}"
    assert _TOKEN not in str(route.calls.last.request.url)
