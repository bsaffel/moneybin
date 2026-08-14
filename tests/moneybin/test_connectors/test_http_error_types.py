"""The shared HTTP helper raises the calling feed's own error types.

The neutral shapes are supertypes of each feed's, not the other way round, so a
helper that raised `FeedNotFoundError` would slip straight past an adapter's
`except PriceFeedNotFoundError` and abort a batch that used to contain the
failure. It would also relabel every price failure `feed_error`. The caller
therefore supplies its own type bundle and its own message label.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from moneybin.connectors._http import fetch_json
from moneybin.connectors.feed_errors import (
    FeedAPIError,
    FeedAuthError,
    FeedErrorTypes,
    FeedNotFoundError,
    FeedRateLimitError,
    FeedRequestRejectedError,
    FeedUnreachableError,
)
from moneybin.connectors.prices.errors import (
    PRICE_FEED_ERRORS,
    PriceFeedAuthError,
    PriceFeedError,
    PriceFeedNotFoundError,
    PriceFeedUnreachableError,
)
from moneybin.error_codes import PRICE_FEED_ERROR

_URL = "https://feed.example/quote"


def _noop_sleep(_seconds: float) -> None:
    return None


@respx.mock
@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (401, PriceFeedAuthError),
        (403, PriceFeedAuthError),
        (404, PriceFeedNotFoundError),
    ],
)
def test_a_status_maps_onto_the_callers_own_type(
    status: int, expected: type[PriceFeedError]
) -> None:
    respx.get(_URL).mock(return_value=httpx.Response(status))

    with pytest.raises(expected) as exc:
        fetch_json(
            httpx.Client(), _URL, params={}, sleep=_noop_sleep, errors=PRICE_FEED_ERRORS
        )

    assert exc.value.code == PRICE_FEED_ERROR


@respx.mock
def test_a_transport_failure_maps_onto_the_callers_own_type() -> None:
    respx.get(_URL).mock(side_effect=httpx.ConnectError("no route to host"))

    with pytest.raises(PriceFeedUnreachableError) as exc:
        fetch_json(
            httpx.Client(), _URL, params={}, sleep=_noop_sleep, errors=PRICE_FEED_ERRORS
        )

    assert exc.value.code == PRICE_FEED_ERROR


@respx.mock
def test_the_label_names_the_feed_that_failed() -> None:
    """A rate failure must not tell the user their price feed is down."""
    rate_errors = FeedErrorTypes(
        label="exchange rate feed",
        auth=FeedAuthError,
        rate_limit=FeedRateLimitError,
        unreachable=FeedUnreachableError,
        not_found=FeedNotFoundError,
        rejected=FeedRequestRejectedError,
        api=FeedAPIError,
    )
    respx.get(_URL).mock(side_effect=httpx.ConnectError("no route to host"))

    with pytest.raises(Exception) as exc:  # noqa: PT011  # neutral base, asserted below
        fetch_json(
            httpx.Client(), _URL, params={}, sleep=_noop_sleep, errors=rate_errors
        )

    assert "exchange rate feed unreachable" in str(exc.value)
    assert "price" not in str(exc.value)
