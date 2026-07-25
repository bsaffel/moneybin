"""Typed exceptions raised by the price-feed adapters.

The CLI's handle_cli_errors and the MCP decorator map these to user-facing
messages via classify_user_error, which returns any UserError unchanged.

These are whole-batch conditions only. A failure that affects one security —
an unknown ticker, a coin slug the provider dropped — is reported as a
PriceFetchFailure in the result instead, so a refresh over 40 securities that
loses 2 still writes the other 38.
"""

from moneybin.error_codes import PRICE_FEED_ERROR
from moneybin.errors import UserError


class PriceFeedError(UserError):
    """Base for all price-feed adapter errors."""

    def __init__(self, message: str) -> None:
        """Initialize with a user-safe message."""
        super().__init__(message, code=PRICE_FEED_ERROR)


class PriceFeedAuthError(PriceFeedError):
    """The provider's credential is absent, malformed, or rejected (401/403)."""


class PriceFeedRateLimitError(PriceFeedError):
    """The provider rate-limited the request (429)."""


class PriceFeedUnreachableError(PriceFeedError):
    """DNS failure, connection refused, or timeout — no response arrived."""


class PriceFeedAPIError(PriceFeedError):
    """A provider response that is not auth, rate limit, or unreachable."""
