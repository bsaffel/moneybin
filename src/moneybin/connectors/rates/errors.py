"""Typed exceptions raised by the exchange-rate feed adapters.

Mirrors ``connectors.prices.errors``: the CLI's handle_cli_errors and the MCP
decorator map these through classify_user_error, which returns any UserError
unchanged.

The whole-batch / per-item split lands differently here than it does for prices.
A rate adapter answers one pair on one date, so the provider saying "no such
series" is not an error at all — the adapter turns it into ``None`` and lets the
caller segment. What remains is genuinely whole-run: the connection, the quota,
and the provider's health apply to every pair a conversion needs.

Each type also inherits the matching feed-neutral shape from
``connectors.feed_errors``, so the shared HTTP helper can raise a transport
error without knowing which feed asked. The rate code is preserved by calling
``UserError.__init__`` directly: ``FeedError.__init__`` sits in the MRO and
would otherwise relabel every rate failure ``feed_error``.
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
from moneybin.error_codes import RATE_FEED_ERROR
from moneybin.errors import UserError


class RateFeedError(FeedError):
    """Base for all exchange-rate feed adapter errors."""

    def __init__(self, message: str) -> None:
        """Initialize with a user-safe message, keeping the rate-feed code."""
        UserError.__init__(self, message, code=RATE_FEED_ERROR)


class RateFeedAuthError(RateFeedError, FeedAuthError):
    """The provider's credential is absent, malformed, or rejected (401/403).

    Frankfurter is keyless and cannot raise this. It exists because the bundle
    the shared helper takes is total over the six transport conditions, and
    because the documented fallback providers are not all keyless.
    """


class RateFeedRateLimitError(RateFeedError, FeedRateLimitError):
    """The provider rate-limited the request (429)."""


class RateFeedUnreachableError(RateFeedError, FeedUnreachableError):
    """DNS failure, connection refused, or timeout — no response arrived.

    The one the offline case turns on. Conversion fails loud here rather than
    falling back to an unconverted or stale number, so this must never be
    reachable by the same path that reports an out-of-coverage pair.
    """


class RateFeedNotFoundError(RateFeedError, FeedNotFoundError):
    """The provider publishes no series for this pair or date (404).

    Absence, not failure: the adapter catches this and returns None. Kept as a
    distinct type rather than folded into RateFeedAPIError so that catch cannot
    also swallow a 500 and report a broken provider as an unsupported currency.
    """


class RateFeedRequestRejectedError(RateFeedError, FeedRequestRejectedError):
    """The provider rejected this request's parameters (400)."""


class RateFeedAPIError(RateFeedError, FeedAPIError):
    """A provider response that is not auth, rate limit, not-found, rejected, or unreachable."""


# What the shared HTTP helper raises on a rate feed's behalf. The label is what
# keeps a rate outage from being reported to the user as a price-feed outage.
RATE_FEED_ERRORS = FeedErrorTypes(
    label="exchange rate feed",
    auth=RateFeedAuthError,
    rate_limit=RateFeedRateLimitError,
    unreachable=RateFeedUnreachableError,
    not_found=RateFeedNotFoundError,
    rejected=RateFeedRequestRejectedError,
    api=RateFeedAPIError,
)
