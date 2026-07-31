"""Tests for the request path both price adapters share.

``fetch_json`` is where a provider failure becomes a typed MoneyBin error. Two of
its branches exist specifically to keep the request out of the message: an httpx
exception stringifies to the full request URL, and a provider's error page can
quote the request it received. Those are security properties, not formatting
preferences, so they are asserted here rather than left to the comments that
explain them — this is the one place a later "add more detail to the error" edit
could reintroduce a credential leak.

No test here opens a socket.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from moneybin.connectors.prices._http import fetch_json
from moneybin.connectors.prices.errors import (
    PriceFeedAPIError,
    PriceFeedNotFoundError,
    PriceFeedRateLimitError,
    PriceFeedUnreachableError,
)

_URL = "https://prices.example.com/v1/daily/AAPL"
_TOKEN = "s3cret-token-value"  # noqa: S105  # fixture value, not a real credential


def _fetch(client: httpx.Client) -> object:
    """The shared call under test, with the credential riding a query param.

    Tiingo puts its token in a header and CoinGecko's keyless tier has none, but
    the URL is the worst case the wrapping is written against, so that is what
    these tests hand it.
    """
    return fetch_json(
        client,
        _URL,
        params={"token": _TOKEN},
        sleep=lambda _seconds: None,
    )


@respx.mock
def test_an_unreachable_host_names_the_condition_not_the_request() -> None:
    """`str(httpx.ConnectError)` carries the URL it failed to reach, token and all."""
    respx.get(url__startswith=_URL).mock(
        side_effect=httpx.ConnectError("failed to connect to prices.example.com")
    )

    with httpx.Client() as client, pytest.raises(PriceFeedUnreachableError) as caught:
        _fetch(client)

    message = str(caught.value)
    assert "ConnectError" in message, "the condition must survive the wrapping"
    assert _TOKEN not in message
    assert "prices.example.com" not in message


@respx.mock
def test_a_body_that_is_not_json_names_the_status_not_the_body() -> None:
    """A proxy or WAF error page is HTML that can quote the request it rejected."""
    respx.get(url__startswith=_URL).mock(
        return_value=httpx.Response(
            200, text=f"<html>upstream rejected token={_TOKEN}</html>"
        )
    )

    with httpx.Client() as client, pytest.raises(PriceFeedAPIError) as caught:
        _fetch(client)

    message = str(caught.value)
    assert "200" in message, "the status is the only diagnostic the caller gets"
    assert _TOKEN not in message
    assert "upstream rejected" not in message


@respx.mock
def test_an_unknown_symbol_is_typed_apart_from_a_broken_provider() -> None:
    """404 is the one status an adapter may answer for a single security.

    Every other typed error is a whole-batch condition (see errors.py), so the
    adapters decide containment on the exception type alone. If a 404 arrived as
    the same PriceFeedAPIError a 500 does, either the provider being down reads
    as 'this symbol has no coverage' or one unknown ticker aborts the batch —
    the type is what keeps those apart.
    """
    respx.get(url__startswith=_URL).mock(
        return_value=httpx.Response(404, json={"detail": "Error: Ticker not found"})
    )

    with httpx.Client() as client, pytest.raises(PriceFeedNotFoundError) as caught:
        _fetch(client)

    assert not isinstance(caught.value, PriceFeedAPIError), (
        "a not-found must not be catchable as the generic API error"
    )
    assert "404" in str(caught.value)
    assert _TOKEN not in str(caught.value)


@respx.mock
def test_a_rate_limit_that_never_clears_raises_after_the_last_attempt() -> None:
    """Exhausting the retries must raise, not fall through to the defensive branch.

    The loop returns only on a parsed body, so a 429 on every attempt has to leave
    by the stored error. Falling out instead would reach the `RuntimeError` that
    exists to catch an impossible state, turning a routine quota condition into a
    crash `classify_user_error` has no clean message for.
    """
    route = respx.get(url__startswith=_URL).mock(return_value=httpx.Response(429))

    with httpx.Client() as client, pytest.raises(PriceFeedRateLimitError) as caught:
        _fetch(client)

    assert route.call_count == 3, "three attempts, per RETRY_MAX"
    assert "429" in str(caught.value)
