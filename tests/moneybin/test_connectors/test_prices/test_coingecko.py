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

import time as _time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
import respx

from moneybin.connectors.prices.coingecko import (
    COINGECKO_BASE_URL,
    COINGECKO_MAX_HISTORY_DAYS,
    CoinGeckoPriceAdapter,
)
from moneybin.connectors.prices.errors import (
    PriceFeedRateLimitError,
    PriceFeedWindowUnsupportedError,
)
from moneybin.connectors.prices.protocol import SecurityRef

_DAY_MS = 86_400_000
_HOUR_MS = 3_600_000

# The window pair below pins its own "today" and injects it, rather than reading a
# clock. Reading date.today() built the window on the host's calendar while the
# adapter measures the span against UTC, so on any host whose local date has
# already turned over, a window spelled as exactly 365 days arrives as 366 and the
# boundary case refuses a backfill that is legal. A literal also stops the pair
# expiring the day the real clock runs 365 days past it.
_WINDOW_TODAY = date(2026, 7, 24)


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


def test_it_refuses_a_window_deeper_than_the_keyless_tier_serves() -> None:
    """Silently narrowing an explicit window returns a partial backfill as a whole one.

    Replaces a test that asserted the clamp. The free tier serves 365 days, and
    quietly substituting that for a five-year ``--since`` wrote the recent rows and
    reported an ordinary success, so nothing anywhere told the user the older four
    years were never requested. Refusing names the window CoinGecko can actually
    serve, and ``pull``'s per-source containment keeps every other feed's rows.

    No request is issued, so the refusal also costs none of the keyless quota.
    """
    today = _WINDOW_TODAY
    with respx.mock:
        route = respx.get(_chart_route()).mock(
            return_value=httpx.Response(200, json=_prices([]))
        )

        with pytest.raises(PriceFeedWindowUnsupportedError, match="365"):
            CoinGeckoPriceAdapter(today=today).fetch(
                [_ref()],
                today - timedelta(days=COINGECKO_MAX_HISTORY_DAYS),
                today,
            )

    assert not route.called


def test_it_serves_the_deepest_window_the_keyless_tier_allows() -> None:
    """The boundary itself is legal — an off-by-one here refuses a valid backfill.

    Paired with the refusal deliberately: a guard spelled ``>=`` passes that test
    and fails this one, and neither fixture on its own separates the two versions.
    """
    today = _WINDOW_TODAY
    with respx.mock:
        route = respx.get(_chart_route()).mock(
            return_value=httpx.Response(
                200, json=_prices([(_midnight_ms(today), "64000.10")])
            )
        )

        CoinGeckoPriceAdapter(today=today).fetch(
            [_ref()],
            today - timedelta(days=COINGECKO_MAX_HISTORY_DAYS - 1),
            today,
        )

    days = int(route.calls.last.request.url.params["days"])
    assert days == COINGECKO_MAX_HISTORY_DAYS


@pytest.mark.parametrize("tz", ["Pacific/Kiritimati", "Pacific/Midway"])
def test_the_default_span_is_measured_against_the_utc_day(
    monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """``days`` counts back from the UTC date, on any host.

    The pair above injects its clock, so nothing else here exercises the default,
    and the default is the whole point: CoinGecko's close for D is the 00:00 UTC
    point of D+1, so a span measured on the host's calendar asks for a window
    offset by a day from the one the response is filtered against. West of UTC
    that silently drops the oldest date asked for; east of UTC it reaches for a
    day the provider has not closed.

    UTC+14 and UTC-11 are chosen so the assertion is never vacuous: Kiritimati's
    local date runs ahead of UTC from 10:00 UTC and Midway's runs behind before
    11:00 UTC, so at any instant at least one of the two disagrees with UTC. A
    host-local clock cannot pass both.
    """
    monkeypatch.setenv("TZ", tz)
    _time.tzset()
    try:
        start = datetime.now(UTC).date() - timedelta(days=9)
        with respx.mock:
            route = respx.get(_chart_route()).mock(
                return_value=httpx.Response(
                    200,
                    json=_prices([
                        (_midnight_ms(start + timedelta(days=5)), "64000.10")
                    ]),
                )
            )

            CoinGeckoPriceAdapter().fetch([_ref()], start, start + timedelta(days=9))

        assert int(route.calls.last.request.url.params["days"]) == 10
    finally:
        monkeypatch.undo()
        _time.tzset()


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


def test_a_boolean_price_never_becomes_an_observation() -> None:
    """The twin of the Tiingo case: `bool` is an `int`, so `true` parses as 1.

    This adapter already excludes it. The exclusion left no behavioural trace,
    though — deleting it kept every other test in this module green — so the
    guard needed its own fixture to mean anything.
    """
    body = {
        "prices": [
            [_midnight_ms(date(2026, 7, 24)), True],
            [_midnight_ms(date(2026, 7, 25)), 64000.10],
        ],
        "market_caps": [],
        "total_volumes": [],
    }
    with respx.mock:
        respx.get(_chart_route()).mock(return_value=httpx.Response(200, json=body))

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


def test_one_unsupported_quote_currency_does_not_lose_the_rest_of_the_batch() -> None:
    """``vs_currency`` is per-coin, so CoinGecko rejecting one must not end the run.

    This adapter builds ``params`` INSIDE the loop from ``ref.quote_currency``,
    unlike the Tiingo adapter, whose query parameters are batch-wide and whose
    only per-security value rides in the URL path. So the shared reasoning "a
    non-404 answer is wrong for every coin" does not survive here: CoinGecko
    quotes roughly 60 of ISO-4217's ~180 codes, a holding's currency comes
    straight from ``app.securities.currency_code`` with no validation on the pull
    path, and a 400 about ONE coin's currency was aborting the whole source —
    leaving every other crypto position unpriced with an error naming none of it.
    """
    with respx.mock:
        respx.get(_chart_route("bitcoin")).mock(
            return_value=httpx.Response(
                200, json=_prices([(_midnight_ms(date(2026, 7, 25)), "64000.10")])
            )
        )
        respx.get(_chart_route("ethereum")).mock(
            return_value=httpx.Response(400, json={"error": "invalid vs_currency"})
        )

        result = CoinGeckoPriceAdapter().fetch(
            [_ref("bitcoin"), _ref("ethereum", currency="XPF")],
            date(2026, 7, 24),
            date(2026, 7, 24),
        )

    assert [obs.provider_security_key for obs in result.observations] == ["bitcoin"]
    assert [f.provider_security_key for f in result.failures] == ["ethereum"]


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


@pytest.mark.parametrize(
    "bad_ts",
    [
        # Both are exact midnight multiples, so each reaches the conversion
        # instead of being dropped by the granularity check ahead of it.
        3_000_000 * _DAY_MS,  # a representable day count, but past date.max
        10**12 * _DAY_MS,  # a day count timedelta itself refuses
    ],
)
def test_a_timestamp_outside_the_date_range_loses_only_its_own_point(
    bad_ts: int,
) -> None:
    """Every other unusable shape returns None; this one raised.

    `PriceService.pull` contains typed `PriceFeedError`s only, so the
    `OverflowError` escapes the whole run — one out-of-range integer discarding
    observations already fetched from every source, not just this coin. The two
    cases raise from different places (`date` arithmetic and `timedelta`'s own
    day limit), and only a point at exact UTC midnight gets far enough to hit
    either.
    """
    with respx.mock:
        respx.get(_chart_route()).mock(
            return_value=httpx.Response(
                200,
                json=_prices([
                    (bad_ts, "64000.10"),
                    (_midnight_ms(date(2026, 7, 25)), "64000.10"),
                ]),
            )
        )

        result = CoinGeckoPriceAdapter().fetch(
            [_ref()], date(2026, 7, 22), date(2026, 7, 24)
        )

    assert [obs.price_date for obs in result.observations] == [date(2026, 7, 24)]
    assert not result.failures


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
