"""The feed-neutral error base shared by every external data connector.

Exchange rates and market prices fail the same six transport ways, so the
shapes live in one hierarchy. These tests pin the two properties that make the
lift safe: a price error is still a price error to every existing caller, and
a non-price feed can raise the neutral shape without borrowing a price code.
"""

from __future__ import annotations

import pytest

from moneybin.connectors.feed_errors import (
    FeedAPIError,
    FeedAuthError,
    FeedError,
    FeedNotFoundError,
    FeedRateLimitError,
    FeedRequestRejectedError,
    FeedUnreachableError,
)
from moneybin.connectors.prices.errors import (
    PriceFeedAPIError,
    PriceFeedAuthError,
    PriceFeedError,
    PriceFeedNotFoundError,
    PriceFeedRateLimitError,
    PriceFeedRequestRejectedError,
    PriceFeedUnreachableError,
)
from moneybin.error_codes import FEED_ERROR, PRICE_FEED_ERROR
from moneybin.errors import UserError

_PAIRS = [
    (PriceFeedAuthError, FeedAuthError),
    (PriceFeedRateLimitError, FeedRateLimitError),
    (PriceFeedUnreachableError, FeedUnreachableError),
    (PriceFeedNotFoundError, FeedNotFoundError),
    (PriceFeedRequestRejectedError, FeedRequestRejectedError),
    (PriceFeedAPIError, FeedAPIError),
]


def test_the_neutral_base_is_a_user_error() -> None:
    """Feed failures must reach the CLI and MCP error paths like any UserError."""
    assert issubclass(FeedError, UserError)
    assert FeedError("offline").code == FEED_ERROR


@pytest.mark.parametrize(("price_error", "neutral_error"), _PAIRS)
def test_each_price_error_is_also_its_neutral_shape(
    price_error: type[PriceFeedError], neutral_error: type[FeedError]
) -> None:
    """A caller catching the neutral shape must also catch the price one."""
    assert issubclass(price_error, neutral_error)
    assert issubclass(price_error, PriceFeedError)


@pytest.mark.parametrize(("price_error", "neutral_error"), _PAIRS)
def test_a_price_error_keeps_its_own_code(
    price_error: type[PriceFeedError], neutral_error: type[FeedError]
) -> None:
    """Reparenting must not renumber the code the CLI and MCP already render.

    Inheriting from the neutral shape puts FeedError.__init__ in the MRO, and
    if it won, every price failure would start reporting `feed_error` — a
    silent break of the error contract that nothing else would catch.
    """
    assert price_error("boom").code == PRICE_FEED_ERROR
    assert neutral_error("boom").code == FEED_ERROR


def test_the_two_per_item_errors_stay_outside_the_generic_api_error() -> None:
    """Adapters catch these to contain one item; widening them breaks that.

    PriceFeedNotFoundError and PriceFeedRequestRejectedError are caught to
    record a per-security failure. If either inherited from the generic API
    error, that same catch would swallow a 500 and report a broken provider as
    "this security has no coverage".
    """
    assert not issubclass(PriceFeedNotFoundError, PriceFeedAPIError)
    assert not issubclass(PriceFeedRequestRejectedError, PriceFeedAPIError)
    assert not issubclass(FeedNotFoundError, FeedAPIError)
    assert not issubclass(FeedRequestRejectedError, FeedAPIError)
