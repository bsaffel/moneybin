"""Tests for the price-feed error hierarchy.

Mirrors test_gsheet/test_errors.py: a connector's errors are part of its public
surface, so the wiring to the taxonomy code is asserted rather than assumed.
"""

from moneybin.connectors.prices.errors import (
    PriceFeedAPIError,
    PriceFeedAuthError,
    PriceFeedError,
    PriceFeedRateLimitError,
    PriceFeedUnreachableError,
)
from moneybin.error_codes import PRICE_FEED_ERROR
from moneybin.errors import UserError, classify_user_error

_SUBCLASSES = (
    PriceFeedAuthError,
    PriceFeedUnreachableError,
    PriceFeedRateLimitError,
    PriceFeedAPIError,
)


class TestPriceFeedErrorHierarchy:
    """Verify error class hierarchy and subclass relationships."""

    def test_error_hierarchy_subclasses_base(self) -> None:
        """All price-feed subclasses are subclasses of PriceFeedError."""
        for cls in _SUBCLASSES:
            assert issubclass(cls, PriceFeedError)

    def test_price_feed_error_subclasses_user_error(self) -> None:
        """PriceFeedError is a subclass of UserError."""
        assert issubclass(PriceFeedError, UserError)

    def test_all_subclasses_emit_taxonomy_code(self) -> None:
        """Every price-feed error carries the taxonomy code, not a bare string."""
        for cls in (PriceFeedError, *_SUBCLASSES):
            assert cls("boom").code == PRICE_FEED_ERROR

    def test_classify_user_error_surfaces_them(self) -> None:
        """The spec requires these register with classify_user_error.

        They do so by subclassing UserError, which the classifier returns
        unchanged — so a price-feed failure reaches the CLI and MCP as a clean
        message rather than a re-raised traceback.
        """
        for cls in (PriceFeedError, *_SUBCLASSES):
            classified = classify_user_error(cls("boom"))
            assert classified is not None
            assert classified.code == PRICE_FEED_ERROR
