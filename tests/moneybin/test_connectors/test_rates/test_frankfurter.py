"""Tests for the Frankfurter ECB reference-rate adapter.

Every response body here is a shape recorded from the live API on 2026-08-14,
served back through respx. No test opens a socket, and none needs a credential —
Frankfurter is keyless.

The recorded statuses are the point of several of these tests. The provider
answers an unknown currency and a pre-1999 date with 404, and an identity pair
with 422 — none of which is guessable from the shape of a successful response.
"""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal

import httpx
import pytest
import respx

from moneybin.connectors.feed_errors import FeedUnreachableError
from moneybin.connectors.rates import RateAdapter
from moneybin.connectors.rates.errors import RateFeedAPIError
from moneybin.connectors.rates.frankfurter import (
    FRANKFURTER_BASE_URL,
    FRANKFURTER_SOURCE,
    FrankfurterRateAdapter,
)
from moneybin.error_codes import RATE_FEED_ERROR

_FRIDAY = date(2026, 3, 13)
_SUNDAY = date(2026, 3, 15)


def test_the_adapter_satisfies_the_rate_adapter_protocol() -> None:
    """Keeps the Protocol load-bearing rather than decorative.

    The annotation is the assertion: pyright rejects the assignment if the
    adapter and the Protocol drift apart. Verified by removing a method and
    confirming pyright reports the mismatch.
    """
    adapter: RateAdapter = FrankfurterRateAdapter()

    assert adapter.source_type == FRANKFURTER_SOURCE


def _url(on: date) -> str:
    return f"{FRANKFURTER_BASE_URL}/{on.isoformat()}"


@respx.mock
def test_a_weekday_rate_parses_as_decimal() -> None:
    respx.get(_url(_FRIDAY)).mock(
        return_value=httpx.Response(
            200,
            json={
                "amount": 1.0,
                "base": "USD",
                "date": "2026-03-13",
                "rates": {"EUR": 0.87138},
            },
        )
    )

    obs = FrankfurterRateAdapter().fetch("USD", "EUR", _FRIDAY)

    assert obs is not None
    assert obs.rate == Decimal("0.87138")
    assert isinstance(obs.rate, Decimal), "a float rate would defeat DECIMAL(18,8)"
    assert obs.rate_date == _FRIDAY
    assert obs.from_currency == "USD"
    assert obs.to_currency == "EUR"
    assert obs.source_type == FRANKFURTER_SOURCE


@respx.mock
def test_a_lowercase_pair_is_normalized_before_the_request() -> None:
    route = respx.get(_url(_FRIDAY)).mock(
        return_value=httpx.Response(
            200,
            json={
                "amount": 1.0,
                "base": "USD",
                "date": "2026-03-13",
                "rates": {"EUR": 0.87138},
            },
        )
    )

    obs = FrankfurterRateAdapter().fetch("usd", "eur", _FRIDAY)

    assert obs is not None
    assert obs.from_currency == "USD"
    assert obs.to_currency == "EUR"
    assert route.calls.last.request.url.params["symbols"] == "EUR"


@respx.mock
def test_a_weekend_request_records_the_business_day_the_provider_answered() -> None:
    """2026-03-15 is a Sunday; ECB's last published day is Friday the 13th.

    Recorded live: requesting the Sunday returns `"date": "2026-03-13"`. The
    resolution is the provider's, and it must reach the caller rather than be
    silently relabelled as a Sunday rate.
    """
    respx.get(_url(_SUNDAY)).mock(
        return_value=httpx.Response(
            200,
            json={
                "amount": 1.0,
                "base": "USD",
                "date": "2026-03-13",
                "rates": {"EUR": 0.87138},
            },
        )
    )

    obs = FrankfurterRateAdapter().fetch("USD", "EUR", _SUNDAY)

    assert obs is not None
    assert obs.rate_date == _FRIDAY, "resolution must be recorded, not hidden"


@respx.mock
def test_an_unknown_currency_returns_none_rather_than_raising() -> None:
    """Recorded live: `?base=USD&symbols=XXX` answers 404, not an empty body."""
    respx.get(_url(_FRIDAY)).mock(
        return_value=httpx.Response(
            404, json={"message": "Could not find currency XXX"}
        )
    )

    assert FrankfurterRateAdapter().fetch("USD", "XXX", _FRIDAY) is None


@respx.mock
def test_a_date_before_the_ecb_series_returns_none() -> None:
    """Recorded live: 1990-06-01 answers 404; 1999-01-04 is the first day served."""
    early = date(1990, 6, 1)
    respx.get(_url(early)).mock(return_value=httpx.Response(404))

    assert FrankfurterRateAdapter().fetch("USD", "EUR", early) is None


@respx.mock
def test_the_absent_series_log_line_does_not_carry_the_requested_date(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A 404 is routine, so it logs — and the log outlives the session.

    The pair stays: a currency code is ``DataClass.CURRENCY``, which the fx
    payload module records as disclosing no balance and no amount. The date does
    not, because ``FxRatePayload`` classifies it ``TXN_DATE`` on the reasoning
    that one is asked about a particular day because money moved on it.

    Scoped to ``moneybin.*`` records on purpose, and the narrowing is a finding
    rather than a convenience: httpx logs ``HTTP Request: GET
    .../v1/2026-03-13?base=USD&symbols=EUR`` at INFO, and ``httpx`` sits in
    ``_CONSOLE_SUPPRESSED_PREFIXES``, which suppresses the console *and keeps
    the file copy*. So the same date still reaches the same log through the
    client, on every request rather than only on a 404. That exposure is not
    this branch's — `tiingo` has sent `startDate`/`endDate` and a ticker in the
    URL since #373 — and closing it means overturning a documented decision to
    retain per-request lines for sync debugging. Asserting it here would pin an
    unrelated config to this adapter's test.
    """
    respx.get(_url(_FRIDAY)).mock(return_value=httpx.Response(404))

    with caplog.at_level(logging.INFO):
        assert FrankfurterRateAdapter().fetch("USD", "EUR", _FRIDAY) is None

    logged = " ".join(
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("moneybin.")
    )
    assert _FRIDAY.isoformat() not in logged
    assert "USD/EUR" in logged, "the pair is what makes the line worth keeping"


@respx.mock
def test_a_response_without_the_requested_rate_returns_none() -> None:
    """A 200 that omits the pair is absence, not a malformed provider."""
    respx.get(_url(_FRIDAY)).mock(
        return_value=httpx.Response(
            200,
            json={"amount": 1.0, "base": "USD", "date": "2026-03-13", "rates": {}},
        )
    )

    assert FrankfurterRateAdapter().fetch("USD", "EUR", _FRIDAY) is None


@respx.mock
def test_a_whole_number_rate_is_a_rate_not_an_absence() -> None:
    """JSON `109` is an int, so `parse_float=Decimal` never sees it.

    A rate that happens to land on a whole number must not read as "the
    provider has no series" — that would drop a real rate silently, and the
    high-magnitude pairs where it can happen (JPY, KRW) are exactly the ones a
    conversion error would be largest on.
    """
    respx.get(_url(_FRIDAY)).mock(
        return_value=httpx.Response(
            200,
            json={
                "amount": 1,
                "base": "USD",
                "date": "2026-03-13",
                "rates": {"JPY": 109},
            },
        )
    )

    obs = FrankfurterRateAdapter().fetch("USD", "JPY", _FRIDAY)

    assert obs is not None
    assert obs.rate == Decimal(109)
    assert isinstance(obs.rate, Decimal)


@pytest.mark.parametrize(
    "body",
    [
        pytest.param([1, 2], id="body-is-not-an-object"),
        pytest.param(
            {"amount": 1.0, "base": "USD", "date": "2026-03-13", "rates": "0.87138"},
            id="rates-is-not-an-object",
        ),
        pytest.param(
            {
                "amount": 1.0,
                "base": "USD",
                "date": "2026-03-13",
                "rates": {"EUR": True},
            },
            id="rate-is-the-wrong-type",
        ),
        pytest.param(
            {
                "amount": 1.0,
                "base": "USD",
                "date": "not-a-date",
                "rates": {"EUR": 0.87138},
            },
            id="resolved-date-is-unreadable",
        ),
    ],
)
@respx.mock
def test_a_malformed_200_is_a_feed_failure_not_an_absent_series(
    body: object,
) -> None:
    """A present-but-corrupt field is the provider's fault, so it raises.

    ``RateAdapter.fetch`` promises None only for "an absence the caller can
    route around", and raises "for every condition that is the provider's fault
    or the network's". Returning None here spent that promise: ``_absence``
    would find both currencies supported and tell the user no rate was published
    for the date, so the remedy offered — try a nearby date, or record it
    yourself — cannot work, and the corrupt response never surfaces.

    All four malformed shapes are parametrized together because they are one
    defect with four spellings. Fixing the wrong-typed rate alone would leave
    three siblings answering absence for a provider fault, which is the same bug
    wearing a different field name.

    ``true`` earns its own case: it is an ``int`` in Python, so a conversion that
    forgot to exclude ``bool`` would store a rate of exactly 1.
    """
    respx.get(_url(_FRIDAY)).mock(return_value=httpx.Response(200, json=body))

    with pytest.raises(RateFeedAPIError):
        FrankfurterRateAdapter().fetch("USD", "EUR", _FRIDAY)


@respx.mock
def test_an_identity_pair_never_reaches_the_provider() -> None:
    """Recorded live: `?base=USD&symbols=USD` answers 422.

    A currency is worth exactly one of itself on every date, including the
    weekends and holidays the provider has no row for. Asking would turn an
    arithmetic certainty into a network call that reports a broken feed.
    """
    route = respx.get(_url(_SUNDAY)).mock(return_value=httpx.Response(422))

    obs = FrankfurterRateAdapter().fetch("USD", "usd", _SUNDAY)

    assert obs is not None
    assert obs.rate == Decimal(1)
    assert obs.rate_date == _SUNDAY, "identity holds on the day asked for"
    assert not route.called


_CURRENCIES_BODY = {
    "EUR": "Euro",
    "GBP": "British Pound",
    "JPY": "Japanese Yen",
    "USD": "United States Dollar",
}


@respx.mock
def test_supported_currencies_reads_the_published_set() -> None:
    """Recorded live: /v1/currencies answers a code → name object, 30 entries."""
    respx.get(f"{FRANKFURTER_BASE_URL}/currencies").mock(
        return_value=httpx.Response(200, json=_CURRENCIES_BODY)
    )

    supported = FrankfurterRateAdapter().supported_currencies()

    assert supported == frozenset({"EUR", "GBP", "JPY", "USD"})


@respx.mock
def test_supported_currencies_is_fetched_once_per_adapter() -> None:
    """The ECB set changes about once a decade; a conversion run must not refetch."""
    route = respx.get(f"{FRANKFURTER_BASE_URL}/currencies").mock(
        return_value=httpx.Response(200, json=_CURRENCIES_BODY)
    )
    adapter = FrankfurterRateAdapter()

    first = adapter.supported_currencies()
    second = adapter.supported_currencies()

    assert first == second
    assert route.call_count == 1


@respx.mock
def test_supported_currencies_raises_offline_rather_than_answering_empty() -> None:
    """An empty set would read as 'no currency is supported'.

    That is the inversion this method exists to prevent: it would relabel every
    pair permanently unsupported — sending the user to write manual overrides —
    on the strength of a dropped connection.
    """
    respx.get(f"{FRANKFURTER_BASE_URL}/currencies").mock(
        side_effect=httpx.ConnectError("no route to host")
    )

    with pytest.raises(FeedUnreachableError):
        FrankfurterRateAdapter().supported_currencies()


@respx.mock
def test_supported_currencies_raises_on_a_body_it_cannot_read() -> None:
    """Same inversion, reached by a broken provider instead of a dead link."""
    respx.get(f"{FRANKFURTER_BASE_URL}/currencies").mock(
        return_value=httpx.Response(200, json=["EUR", "USD"])
    )

    with pytest.raises(RateFeedAPIError):
        FrankfurterRateAdapter().supported_currencies()


@respx.mock
def test_a_failed_currency_lookup_is_not_memoized() -> None:
    """A transient outage must not pin the adapter to a permanent failure."""
    route = respx.get(f"{FRANKFURTER_BASE_URL}/currencies").mock(
        side_effect=[
            httpx.ConnectError("no route to host"),
            httpx.Response(200, json=_CURRENCIES_BODY),
        ]
    )
    adapter = FrankfurterRateAdapter()

    with pytest.raises(FeedUnreachableError):
        adapter.supported_currencies()

    assert adapter.supported_currencies() == frozenset({"EUR", "GBP", "JPY", "USD"})
    assert route.call_count == 2


@respx.mock
def test_offline_raises_rather_than_returning_none() -> None:
    """None means 'no such series'; offline must stay distinguishable from it."""
    respx.get(_url(_FRIDAY)).mock(side_effect=httpx.ConnectError("no route to host"))

    with pytest.raises(FeedUnreachableError) as exc:
        FrankfurterRateAdapter().fetch("USD", "EUR", _FRIDAY)

    assert exc.value.code == RATE_FEED_ERROR


@respx.mock
def test_the_error_never_echoes_the_provider_body() -> None:
    respx.get(_url(_FRIDAY)).mock(
        return_value=httpx.Response(500, text="secret upstream detail")
    )

    with pytest.raises(Exception) as exc:  # noqa: PT011  # feed base, asserted below
        FrankfurterRateAdapter().fetch("USD", "EUR", _FRIDAY)

    assert "secret upstream detail" not in str(exc.value)


@respx.mock
def test_the_message_names_the_rate_feed_not_the_price_feed() -> None:
    respx.get(_url(_FRIDAY)).mock(side_effect=httpx.ConnectError("no route to host"))

    with pytest.raises(FeedUnreachableError) as exc:
        FrankfurterRateAdapter().fetch("USD", "EUR", _FRIDAY)

    assert "exchange rate feed" in str(exc.value)
    assert "price" not in str(exc.value)
