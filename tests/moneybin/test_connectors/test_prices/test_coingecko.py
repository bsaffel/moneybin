"""Tests for the CoinGecko spot-price adapter.

Every response body here is a recorded shape, served through respx. No test in
this module opens a socket, and the adapter needs no credential.

Two response facts below were verified against the live keyless API on
2026-07-24, because the documentation does not state either:

- ``/coins/{id}/market_chart/range`` returns **401** without a key, while
  ``/coins/{id}/market_chart?days=N`` returns 200. The keyless adapter must use
  the ``days`` form.
- Granularity varies with window width: ``days=200`` returned points spaced
  86_400_000 ms at exact midnight UTC; ``days=5`` returned points spaced
  3_600_000 ms. The same date therefore carries different prices depending on
  how wide a window was requested.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import httpx
import pytest
import respx

from moneybin.connectors.prices.coingecko import (
    COINGECKO_BASE_URL,
    COINGECKO_MAX_HISTORY_DAYS,
    CoinGeckoPriceAdapter,
)
from moneybin.connectors.prices.errors import PriceFeedRateLimitError
from moneybin.connectors.prices.protocol import SecurityRef

_DAY_MS = 86_400_000
_HOUR_MS = 3_600_000


def _midnight_ms(day: date) -> int:
    """Milliseconds at exactly 00:00:00 UTC on ``day``."""
    return (day - date(1970, 1, 1)).days * _DAY_MS


def _ref(key: str = "bitcoin", currency: str = "USD") -> SecurityRef:
    return SecurityRef(provider_security_key=key, quote_currency=currency)


def _chart_route(coin: str = "bitcoin") -> str:
    return f"{COINGECKO_BASE_URL}/coins/{coin}/market_chart"


def _prices(points: list[tuple[int, str]]) -> dict[str, object]:
    """A recorded market_chart body. Prices are JSON numbers, not strings."""
    return {
        "prices": [[ts, float(price)] for ts, price in points],
        "market_caps": [],
        "total_volumes": [],
    }


def test_it_stores_the_midnight_point_against_the_preceding_date() -> None:
    """00:00 UTC on D is the value at the END of D-1.

    Labelling it D would blend a Friday equity close with Thursday-end crypto
    inside one portfolio total, and report that row's staleness as zero days.
    """
    with respx.mock:
        respx.get(_chart_route()).mock(
            return_value=httpx.Response(
                200,
                json=_prices([(_midnight_ms(date(2026, 7, 25)), "64000.10")]),
            )
        )

        result = CoinGeckoPriceAdapter().fetch(
            [_ref()], date(2026, 7, 20), date(2026, 7, 24)
        )

    assert [(obs.price_date, obs.close) for obs in result.observations] == [
        (date(2026, 7, 24), Decimal("64000.10"))
    ]


def test_a_point_that_is_not_exactly_midnight_is_discarded() -> None:
    """An hourly window yields 24 points per date; only the boundary is a close."""
    midnight = _midnight_ms(date(2026, 7, 25))
    with respx.mock:
        respx.get(_chart_route()).mock(
            return_value=httpx.Response(
                200,
                json=_prices([
                    (midnight, "64000.10"),
                    (midnight + _HOUR_MS, "64111.00"),
                    (midnight + 5 * _HOUR_MS, "63500.00"),
                ]),
            )
        )

        result = CoinGeckoPriceAdapter().fetch(
            [_ref()], date(2026, 7, 24), date(2026, 7, 24)
        )

    assert [(obs.price_date, obs.close) for obs in result.observations] == [
        (date(2026, 7, 24), Decimal("64000.10"))
    ]


def test_a_narrow_and_a_wide_window_agree_on_a_shared_date() -> None:
    """The stored price must be a pure function of the date, not of the window.

    raw.security_prices is append-only with on_conflict="ignore", so whichever
    refresh ran first would win that date permanently and silently.
    """
    midnight = _midnight_ms(date(2026, 7, 25))
    hourly = _prices([
        (midnight - _HOUR_MS, "63900.00"),
        (midnight, "64000.10"),
        (midnight + _HOUR_MS, "64111.00"),
    ])
    daily = _prices([
        (_midnight_ms(date(2026, 7, 24)), "63000.00"),
        (midnight, "64000.10"),
    ])

    def observations_for(body: dict[str, object], start: date) -> list[object]:
        with respx.mock:
            respx.get(_chart_route()).mock(return_value=httpx.Response(200, json=body))
            result = CoinGeckoPriceAdapter().fetch([_ref()], start, date(2026, 7, 24))
        return [
            (obs.price_date, obs.close)
            for obs in result.observations
            if obs.price_date == date(2026, 7, 24)
        ]

    assert observations_for(hourly, date(2026, 7, 24)) == observations_for(
        daily, date(2026, 7, 23)
    )


def test_it_uses_the_keyless_days_form_not_the_range_endpoint() -> None:
    """market_chart/range answers 401 without a key; the days form does not."""
    with respx.mock:
        route = respx.get(_chart_route()).mock(
            return_value=httpx.Response(
                200, json=_prices([(_midnight_ms(date(2026, 7, 25)), "64000.10")])
            )
        )

        CoinGeckoPriceAdapter().fetch([_ref()], date(2026, 7, 20), date(2026, 7, 24))

    url = route.calls.last.request.url
    assert not str(url.path).endswith("/range")
    assert "days" in url.params
    assert "from" not in url.params


def test_it_sends_no_credential() -> None:
    """The keyless tier is what makes this adapter free of a secret to manage."""
    with respx.mock:
        route = respx.get(_chart_route()).mock(
            return_value=httpx.Response(
                200, json=_prices([(_midnight_ms(date(2026, 7, 25)), "64000.10")])
            )
        )

        CoinGeckoPriceAdapter().fetch([_ref()], date(2026, 7, 20), date(2026, 7, 24))

    request = route.calls.last.request
    assert "x-cg-pro-api-key" not in request.headers
    assert "x-cg-demo-api-key" not in request.headers
    assert "x_cg_pro_api_key" not in request.url.params


def test_it_never_asks_for_more_history_than_the_keyless_tier_serves() -> None:
    """The free tier caps historical data at 365 days; a wider ask is clamped."""
    with respx.mock:
        route = respx.get(_chart_route()).mock(
            return_value=httpx.Response(
                200, json=_prices([(_midnight_ms(date(2026, 7, 25)), "64000.10")])
            )
        )

        CoinGeckoPriceAdapter().fetch([_ref()], date(2020, 1, 1), date(2026, 7, 24))

    days = int(route.calls.last.request.url.params["days"])
    assert days == COINGECKO_MAX_HISTORY_DAYS


def test_it_declares_the_raw_basis() -> None:
    """Spot crypto has no splits or dividends, so its prices are raw by construction."""
    with respx.mock:
        respx.get(_chart_route()).mock(
            return_value=httpx.Response(
                200, json=_prices([(_midnight_ms(date(2026, 7, 25)), "64000.10")])
            )
        )

        result = CoinGeckoPriceAdapter().fetch(
            [_ref()], date(2026, 7, 24), date(2026, 7, 24)
        )

    assert CoinGeckoPriceAdapter().price_basis == "raw"
    assert [obs.price_basis for obs in result.observations] == ["raw"]
    assert [obs.source_type for obs in result.observations] == ["coingecko"]


def test_it_requests_the_currency_the_caller_declared() -> None:
    """vs_currency is a request parameter, so the response needs no currency field."""
    with respx.mock:
        route = respx.get(_chart_route()).mock(
            return_value=httpx.Response(
                200, json=_prices([(_midnight_ms(date(2026, 7, 25)), "55000.00")])
            )
        )

        result = CoinGeckoPriceAdapter().fetch(
            [_ref("bitcoin", "EUR")], date(2026, 7, 24), date(2026, 7, 24)
        )

    assert route.calls.last.request.url.params["vs_currency"] == "eur"
    assert [obs.quote_currency for obs in result.observations] == ["EUR"]


def test_a_nonpositive_price_never_becomes_an_observation() -> None:
    """A zero row would squat its key on an append-only table forever."""
    with respx.mock:
        respx.get(_chart_route()).mock(
            return_value=httpx.Response(
                200,
                json=_prices([
                    (_midnight_ms(date(2026, 7, 24)), "0"),
                    (_midnight_ms(date(2026, 7, 25)), "64000.10"),
                ]),
            )
        )

        result = CoinGeckoPriceAdapter().fetch(
            [_ref()], date(2026, 7, 22), date(2026, 7, 24)
        )

    assert [obs.price_date for obs in result.observations] == [date(2026, 7, 24)]


def test_one_unknown_coin_does_not_lose_the_rest_of_the_batch() -> None:
    """A slug CoinGecko does not know must not discard the coins that resolved."""
    with respx.mock:
        respx.get(_chart_route("bitcoin")).mock(
            return_value=httpx.Response(
                200, json=_prices([(_midnight_ms(date(2026, 7, 25)), "64000.10")])
            )
        )
        respx.get(_chart_route("nosuchcoin")).mock(
            return_value=httpx.Response(404, json={"error": "coin not found"})
        )

        result = CoinGeckoPriceAdapter().fetch(
            [_ref("bitcoin"), _ref("nosuchcoin")],
            date(2026, 7, 24),
            date(2026, 7, 24),
        )

    assert [obs.provider_security_key for obs in result.observations] == ["bitcoin"]
    assert [f.provider_security_key for f in result.failures] == ["nosuchcoin"]


def test_a_rate_limit_stops_the_batch_instead_of_spending_it_on_every_coin() -> None:
    """The keyless tier's quota is shared, so continuing only deepens the limit.

    Same containment rule as the Tiingo adapter: a whole-batch condition leaves
    by exception so PriceService can report one actionable failure per source,
    rather than every coin arriving as its own unexplained per-security failure.
    """
    with respx.mock:
        first = respx.get(_chart_route("bitcoin")).mock(
            return_value=httpx.Response(429)
        )
        second = respx.get(_chart_route("ethereum")).mock(
            return_value=httpx.Response(
                200, json=_prices([(_midnight_ms(date(2026, 7, 25)), "3200.00")])
            )
        )
        adapter = CoinGeckoPriceAdapter()
        adapter._sleep = lambda _seconds: None  # type: ignore[method-assign]  # test hook

        with pytest.raises(PriceFeedRateLimitError):
            adapter.fetch(
                [_ref("bitcoin"), _ref("ethereum")],
                date(2026, 7, 24),
                date(2026, 7, 24),
            )

    assert first.call_count == 3, "the first coin still exhausts its own retries"
    assert not second.called, "a whole-batch condition must not spend the next request"


def test_it_retries_a_rate_limited_coin_then_succeeds() -> None:
    """CoinGecko's keyless tier rate-limits aggressively; back off and retry."""
    with respx.mock:
        respx.get(_chart_route()).mock(
            side_effect=[
                httpx.Response(429, json={"status": {"error_code": 429}}),
                httpx.Response(
                    200, json=_prices([(_midnight_ms(date(2026, 7, 25)), "64000.10")])
                ),
            ]
        )
        adapter = CoinGeckoPriceAdapter()
        slept: list[float] = []
        adapter._sleep = slept.append  # type: ignore[method-assign]  # test hook

        result = adapter.fetch([_ref()], date(2026, 7, 24), date(2026, 7, 24))

    assert len(result.observations) == 1
    assert slept, "a rate-limited request must back off before retrying"


@pytest.mark.parametrize("bad_body", [{"prices": [[123]]}, {"prices": [["x", "y"]]}])
def test_a_malformed_point_is_a_failure_not_a_crash(
    bad_body: dict[str, object],
) -> None:
    """A shape change upstream must degrade to a named failure, never a traceback."""
    with respx.mock:
        respx.get(_chart_route()).mock(return_value=httpx.Response(200, json=bad_body))

        result = CoinGeckoPriceAdapter().fetch(
            [_ref()], date(2026, 7, 24), date(2026, 7, 24)
        )

    assert not result.observations
    assert [f.provider_security_key for f in result.failures] == ["bitcoin"]
