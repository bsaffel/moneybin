"""Tests for the price-feed error hierarchy.

Mirrors test_gsheet/test_errors.py: a connector's errors are part of its public
surface, so the wiring to the taxonomy code is asserted rather than assumed.
"""

from moneybin.connectors.prices.errors import (
    PriceFeedAPIError,
    PriceFeedAuthError,
    PriceFeedError,
    PriceFeedNotFoundError,
    PriceFeedRateLimitError,
    PriceFeedUnreachableError,
    PriceFeedWindowUnsupportedError,
)
from moneybin.error_codes import PRICE_FEED_ERROR
from moneybin.errors import UserError, classify_user_error

_SUBCLASSES = (
    PriceFeedAuthError,
    PriceFeedUnreachableError,
    PriceFeedRateLimitError,
    PriceFeedNotFoundError,
    PriceFeedAPIError,
    PriceFeedWindowUnsupportedError,
)


class TestPriceFeedErrorHierarchy:
    """Verify error class hierarchy and subclass relationships."""

    def test_every_subclass_is_covered_by_this_module(self) -> None:
        """_SUBCLASSES must equal the real hierarchy, not merely be part of it.

        The rest of this module iterates _SUBCLASSES, so a new error type added
        without touching this list would ship with none of its wiring asserted —
        and the taxonomy-code and classifier tests below would still pass. Set
        equality is what makes adding a type require naming it here.
        """
        assert set(PriceFeedError.__subclasses__()) == set(_SUBCLASSES)

    def test_error_hierarchy_subclasses_base(self) -> None:
        """All price-feed subclasses are subclasses of PriceFeedError."""
        for cls in _SUBCLASSES:
            assert issubclass(cls, PriceFeedError)

    def test_a_not_found_is_not_catchable_as_the_generic_api_error(self) -> None:
        """Adapters branch on the type: not-found is contained, everything else propagates.

        If PriceFeedNotFoundError ever became a PriceFeedAPIError, both adapters'
        `except PriceFeedNotFoundError` would start swallowing 5xx responses as
        per-security failures — silently restoring the bug this split removed.
        """
        assert not issubclass(PriceFeedNotFoundError, PriceFeedAPIError)

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
