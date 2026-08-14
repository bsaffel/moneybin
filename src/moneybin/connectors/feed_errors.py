"""Transport-shaped errors shared by every external data feed.

Market prices and exchange rates reach different providers but fail the same
six ways, and one HTTP helper raises for both. The shapes live here so that
helper depends on no particular feed; each feed subclasses them to attach its
own user-facing error code.

The split between whole-batch and per-item conditions is the feed's to make,
not this module's — see ``connectors.prices.errors`` for why a 404 and a 500
need separate types rather than one status-carrying error.
"""

from __future__ import annotations

from dataclasses import dataclass

from moneybin.error_codes import FEED_ERROR
from moneybin.errors import UserError


class FeedError(UserError):
    """Base for all external-feed adapter errors."""

    def __init__(self, message: str) -> None:
        """Initialize with a user-safe message."""
        super().__init__(message, code=FEED_ERROR)


class FeedAuthError(FeedError):
    """The provider's credential is absent, malformed, or rejected (401/403)."""


class FeedRateLimitError(FeedError):
    """The provider rate-limited the request (429)."""


class FeedUnreachableError(FeedError):
    """DNS failure, connection refused, or timeout — no response arrived."""


class FeedNotFoundError(FeedError):
    """The provider answered that it does not know this item (404).

    Deliberately not a FeedAPIError: adapters catch this to record a per-item
    failure, and inheriting from the generic API error would make that same
    catch swallow a 500.
    """


class FeedRequestRejectedError(FeedError):
    """The provider rejected this request's parameters (400).

    Deliberately not a FeedAPIError, for the same reason as FeedNotFoundError.
    Only a feed that varies a PER-ITEM parameter should catch it; elsewhere a
    400 really is the whole batch.
    """


class FeedAPIError(FeedError):
    """A provider response that is not auth, rate limit, not-found, rejected, or unreachable."""


@dataclass(frozen=True, slots=True)
class FeedErrorTypes:
    """The six types one feed wants raised, plus how to name it in a message.

    The shared HTTP helper takes this rather than raising the neutral shapes
    directly. Each feed's types are *subclasses* of the neutral ones, so a
    helper that raised the supertype would slip past an adapter's `except
    PriceFeedNotFoundError` — turning a contained per-item failure into an
    aborted batch — and would report the neutral error code to the user.
    """

    label: str
    auth: type[FeedError]
    rate_limit: type[FeedError]
    unreachable: type[FeedError]
    not_found: type[FeedError]
    rejected: type[FeedError]
    api: type[FeedError]
