"""Typed exceptions raised by the price-feed adapters.

The CLI's handle_cli_errors and the MCP decorator map these to user-facing
messages via classify_user_error, which returns any UserError unchanged.

All but one of these are whole-batch conditions: the credential, the quota, the
connection, and the provider's health answer for every security in the run, so
they leave by exception and PriceService contains them per source.

PriceFeedNotFoundError is the single per-security exception — the provider
answered, and its answer is that it does not know this symbol. An adapter
catches that one and reports a PriceFetchFailure, so a refresh over 40
securities that loses 2 still writes the other 38.

Adapters decide containment on the type alone, which is why the two cases need
separate types rather than one status-carrying error: a 404 and a 500 are both
"a 4xx/5xx arrived", but treating them alike either reports a broken provider as
"this security has no coverage" or lets one unknown ticker abort the batch.
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


class PriceFeedNotFoundError(PriceFeedError):
    """The provider does not know this symbol (404) — the one per-security error.

    Deliberately not a PriceFeedAPIError: adapters catch this to record a
    per-security failure, and inheriting from the generic API error would make
    that same catch swallow a 500 as well.
    """


class PriceFeedAPIError(PriceFeedError):
    """A provider response that is not auth, rate limit, not-found, or unreachable."""
