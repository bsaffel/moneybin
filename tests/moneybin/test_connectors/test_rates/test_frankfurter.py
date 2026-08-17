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


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(
            {
                "amount": 1.0,
                "base": "GBP",
                "date": "2026-03-13",
                "rates": {"EUR": 1.17},
            },
            id="priced-from-a-different-base",
        ),
        pytest.param(
            {
                "amount": 100.0,
                "base": "USD",
                "date": "2026-03-13",
                "rates": {"EUR": 87.138},
            },
            id="priced-for-a-hundred-units",
        ),
        pytest.param(
            {
                "amount": 1.0,
                "base": 840,
                "date": "2026-03-13",
                "rates": {"EUR": 0.87138},
            },
            id="base-is-not-a-currency-code",
        ),
    ],
)
@respx.mock
def test_a_200_answering_another_question_is_a_feed_failure(body: object) -> None:
    """The response says which question it answered, so the adapter must read it.

    Every other malformed shape is unreadable, so it announces itself. This one
    parses cleanly and is wrong only in what it is *about* — a rate the provider
    computed from GBP, or over 100 units, relabelled as one USD buys. Nothing
    downstream can notice: `_store` keeps it, `raw.exchange_rates` is
    append-only, and every later conversion for that date answers from the
    cache, so one unchecked response becomes a wrong number presented as
    authoritative for as long as the row lives.

    ``amount`` earns a case beside ``base`` because the provider takes it as a
    request parameter. MoneyBin never sends one, but a proxy or a redirect that
    does turns 0.87 into 87 with no other field disagreeing.
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
def test_an_empty_currency_catalog_is_refused_rather_than_believed() -> None:
    """`{}` is the inversion itself, arriving with a 200 instead of an error.

    An object body clears the shape check, so its zero keys become "the set of
    everything this provider publishes" — the exact empty set the two tests
    above exist to prevent, reached by the one route that still allowed it.
    """
    respx.get(f"{FRANKFURTER_BASE_URL}/currencies").mock(
        return_value=httpx.Response(200, json={})
    )

    with pytest.raises(RateFeedAPIError):
        FrankfurterRateAdapter().supported_currencies()


@respx.mock
def test_a_status_payload_is_not_mistaken_for_a_currency_catalog() -> None:
    """A provider that answers 200 with prose must not define the published set.

    `{"message": "unavailable"}` is a dict and non-empty, so only the *shape of
    its keys* separates it from a catalog. Believed, it publishes exactly
    `MESSAGE`, which reports every real currency as permanently unsupported and
    sends the user to write manual overrides for a provider that was merely
    having a bad minute.
    """
    respx.get(f"{FRANKFURTER_BASE_URL}/currencies").mock(
        return_value=httpx.Response(200, json={"message": "unavailable"})
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


# --------------------------- bulk date-range fetch ---------------------------
#
# Recorded live 2026-08-15 from /v1/2026-03-09..2026-03-16?base=EUR&symbols=USD.
# Note 03-14 and 03-15 (a weekend) are simply ABSENT from `rates` — the provider
# sends no null, no carried-forward value, and no gap marker. A range running
# past the newest publication comes back with a clamped `end_date`, and a
# reversed range comes back 404; both were recorded the same day.

_RANGE_START = date(2026, 3, 9)
_RANGE_END = date(2026, 3, 16)

_RANGE_BODY = {
    "amount": 1.0,
    "base": "EUR",
    "start_date": "2026-03-09",
    "end_date": "2026-03-16",
    "rates": {
        "2026-03-09": {"USD": 1.1555},
        "2026-03-10": {"USD": 1.1641},
        "2026-03-11": {"USD": 1.1581},
        "2026-03-12": {"USD": 1.1547},
        "2026-03-13": {"USD": 1.1476},
        "2026-03-16": {"USD": 1.1478},
    },
}


def _range_url(start: date, end: date) -> str:
    return f"{FRANKFURTER_BASE_URL}/{start.isoformat()}..{end.isoformat()}"


@respx.mock
def test_a_range_yields_one_observation_per_published_day() -> None:
    """Six observations from an eight-day span — the weekend is simply not there."""
    respx.get(_range_url(_RANGE_START, _RANGE_END)).mock(
        return_value=httpx.Response(200, json=_RANGE_BODY)
    )

    observations = FrankfurterRateAdapter().fetch_range(
        "EUR", "USD", _RANGE_START, _RANGE_END
    )

    assert [obs.rate_date for obs in observations] == [
        date(2026, 3, 9),
        date(2026, 3, 10),
        date(2026, 3, 11),
        date(2026, 3, 12),
        date(2026, 3, 13),
        date(2026, 3, 16),
    ]


@respx.mock
def test_a_range_dates_each_rate_by_the_key_not_by_the_request() -> None:
    """The provider resolves backward, so only its own keys say what a rate is for.

    Asking for a single Sunday returns Friday's row under Friday's key. Filing it
    under the requested date would record a rate the provider never published,
    on a day it was closed.
    """
    respx.get(_range_url(_SUNDAY, _SUNDAY)).mock(
        return_value=httpx.Response(
            200,
            json={
                "amount": 1.0,
                "base": "EUR",
                "start_date": "2026-03-13",
                "end_date": "2026-03-13",
                "rates": {"2026-03-13": {"USD": 1.1476}},
            },
        )
    )

    observations = FrankfurterRateAdapter().fetch_range("EUR", "USD", _SUNDAY, _SUNDAY)

    assert [obs.rate_date for obs in observations] == [_FRIDAY]


@respx.mock
def test_a_range_parses_every_rate_as_decimal() -> None:
    """A float here would lose precision on an append-only DECIMAL(18,8) column."""
    respx.get(_range_url(_RANGE_START, _RANGE_END)).mock(
        return_value=httpx.Response(200, json=_RANGE_BODY)
    )

    observations = FrankfurterRateAdapter().fetch_range(
        "EUR", "USD", _RANGE_START, _RANGE_END
    )

    assert observations[0].rate == Decimal("1.1555")
    assert all(isinstance(obs.rate, Decimal) for obs in observations)
    assert all(obs.source_type == FRANKFURTER_SOURCE for obs in observations)
    assert all(obs.from_currency == "EUR" for obs in observations)
    assert all(obs.to_currency == "USD" for obs in observations)


@respx.mock
def test_a_range_the_provider_does_not_publish_is_an_absence_not_a_failure() -> None:
    """404 covers an unsupported currency, a pre-1999 range, and a reversed one."""
    respx.get(_range_url(_RANGE_START, _RANGE_END)).mock(
        return_value=httpx.Response(404, json={"message": "not found"})
    )

    assert (
        FrankfurterRateAdapter().fetch_range("EUR", "XXX", _RANGE_START, _RANGE_END)
        == ()
    )


def test_an_identity_range_is_answered_without_a_call() -> None:
    """Base == symbols is a 422, so the pair must never reach the network.

    No respx mock is installed here deliberately: a request would raise, so this
    test passing is itself the proof that the short-circuit fired.
    """
    observations = FrankfurterRateAdapter().fetch_range(
        "USD", "usd", _RANGE_START, _RANGE_END
    )

    assert [obs.rate for obs in observations] == [Decimal(1)]
    assert observations[0].rate_date == _RANGE_START


@respx.mock
def test_a_range_answered_for_a_different_base_is_refused() -> None:
    """Same reasoning as `fetch`: a redirected or cached body is not our question."""
    respx.get(_range_url(_RANGE_START, _RANGE_END)).mock(
        return_value=httpx.Response(200, json={**_RANGE_BODY, "base": "GBP"})
    )

    with pytest.raises(RateFeedAPIError):
        FrankfurterRateAdapter().fetch_range("EUR", "USD", _RANGE_START, _RANGE_END)


@respx.mock
def test_a_range_priced_in_other_than_one_unit_is_refused() -> None:
    """The range path owes the same refusal `fetch` gives, for a worse reason.

    A body priced in 100 units yields rates 100x too large on *every* day it
    covers, not one, and the cache is append-only — a rate stored wrong cannot
    be corrected in place.
    """
    respx.get(_range_url(_RANGE_START, _RANGE_END)).mock(
        return_value=httpx.Response(200, json={**_RANGE_BODY, "amount": 100})
    )

    with pytest.raises(RateFeedAPIError):
        FrankfurterRateAdapter().fetch_range("EUR", "USD", _RANGE_START, _RANGE_END)


@respx.mock
def test_a_range_carrying_no_rates_object_is_refused() -> None:
    """A body whose `rates` is not an object would otherwise read as "no days".

    An empty list rather than a string on purpose: a string is iterable, so
    without this guard it would reach the date check character by character and
    raise there instead — passing the test while proving nothing about this
    branch. A list iterates to nothing, so only this guard can refuse it.
    """
    respx.get(_range_url(_RANGE_START, _RANGE_END)).mock(
        return_value=httpx.Response(200, json={**_RANGE_BODY, "rates": []})
    )

    with pytest.raises(RateFeedAPIError):
        FrankfurterRateAdapter().fetch_range("EUR", "USD", _RANGE_START, _RANGE_END)


@respx.mock
def test_a_range_day_that_is_not_a_date_is_refused() -> None:
    """An unreadable key must not be silently dated to the window's start.

    `_as_date` takes a `default`, so a key it cannot parse would otherwise
    stamp that day's rate with the range's opening date — filing a real rate
    under the wrong day, in a table that cannot be corrected in place.
    """
    respx.get(_range_url(_RANGE_START, _RANGE_END)).mock(
        return_value=httpx.Response(
            200, json={**_RANGE_BODY, "rates": {"not-a-date": {"USD": 1.09}}}
        )
    )

    with pytest.raises(RateFeedAPIError):
        FrankfurterRateAdapter().fetch_range("EUR", "USD", _RANGE_START, _RANGE_END)
