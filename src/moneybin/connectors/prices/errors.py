"""Typed exceptions raised by the price-feed adapters.

The CLI's handle_cli_errors and the MCP decorator map these to user-facing
messages via classify_user_error, which returns any UserError unchanged.

Most of these are whole-batch conditions: the credential, the quota, the
connection, and the provider's health answer for every security in the run, so
they leave by exception and PriceService contains them per source.

Two are per-security. PriceFeedNotFoundError is the provider answering that it
does not know this symbol; PriceFeedRequestRejectedError is the provider
answering that this request's own parameters are wrong. An adapter catches those
and reports a PriceFetchFailure, so a refresh over 40 securities that loses 2
still writes the other 38.

Adapters decide containment on the type alone, which is why these cases need
separate types rather than one status-carrying error: a 404 and a 500 are both
"a 4xx/5xx arrived", but treating them alike either reports a broken provider as
"this security has no coverage" or lets one unknown ticker abort the batch.

Each type also inherits the matching feed-neutral shape from
``connectors.feed_errors``, so the shared HTTP helper can raise a transport
error without knowing which feed asked. The price code is preserved by
calling ``UserError.__init__`` directly: ``FeedError.__init__`` sits in the
MRO and would otherwise relabel every price failure ``feed_error``.
"""

from moneybin.connectors.feed_errors import (
    FeedAPIError,
    FeedAuthError,
    FeedError,
    FeedErrorTypes,
    FeedNotFoundError,
    FeedRateLimitError,
    FeedRequestRejectedError,
    FeedUnreachableError,
)
from moneybin.error_codes import PRICE_FEED_ERROR
from moneybin.errors import UserError


class PriceFeedError(FeedError):
    """Base for all price-feed adapter errors."""

    def __init__(self, message: str) -> None:
        """Initialize with a user-safe message, keeping the price-feed code."""
        UserError.__init__(self, message, code=PRICE_FEED_ERROR)


class PriceFeedAuthError(PriceFeedError, FeedAuthError):
    """The provider's credential is absent, malformed, or rejected (401/403)."""


class PriceFeedRateLimitError(PriceFeedError, FeedRateLimitError):
    """The provider rate-limited the request (429)."""


class PriceFeedUnreachableError(PriceFeedError, FeedUnreachableError):
    """DNS failure, connection refused, or timeout — no response arrived."""


class PriceFeedNotFoundError(PriceFeedError, FeedNotFoundError):
    """The provider does not know this symbol (404) — the one per-security error.

    Deliberately not a PriceFeedAPIError: adapters catch this to record a
    per-security failure, and inheriting from the generic API error would make
    that same catch swallow a 500 as well.
    """


class PriceFeedRequestRejectedError(PriceFeedError, FeedRequestRejectedError):
    """The provider rejected this request's parameters (400) — per-security.

    Deliberately not a PriceFeedAPIError, for the same reason as
    PriceFeedNotFoundError: adapters catch this to record a per-security failure,
    and inheriting from the generic API error would make that catch swallow a 500.

    Only an adapter that sends a PER-SECURITY parameter should catch it. That is
    CoinGecko, whose vs_currency comes from the security's own currency_code, and
    not Tiingo, whose query parameters are the same for every security in the run
    so a 400 there really is the whole batch. Catching it in the wrong adapter
    turns one batch-wide mistake into N identical requests against the quota.
    """


class PriceFeedAPIError(PriceFeedError, FeedAPIError):
    """A provider response that is not auth, rate limit, not-found, rejected, or unreachable."""


class PriceFeedWindowUnsupportedError(PriceFeedError):
    """The requested window reaches further back than this provider serves.

    Unlike its siblings this is decided before any request: the window is a
    property of the ask, not of a response. It is still a whole-batch condition —
    the same window applies to every security in the run — so it contains per
    source and leaves other feeds' rows intact.

    Its own type rather than PriceFeedAPIError because the remedy is different in
    kind: nothing is wrong with the provider, and the fix is a narrower window
    argument rather than a retry.
    """


# What the shared HTTP helper raises on a price feed's behalf. The label keeps
# every message reading "price feed ..." exactly as it did before the helper
# became feed-neutral.
PRICE_FEED_ERRORS = FeedErrorTypes(
    label="price feed",
    auth=PriceFeedAuthError,
    rate_limit=PriceFeedRateLimitError,
    unreachable=PriceFeedUnreachableError,
    not_found=PriceFeedNotFoundError,
    rejected=PriceFeedRequestRejectedError,
    api=PriceFeedAPIError,
)
