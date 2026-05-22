"""Tests for gsheet error hierarchy."""

from moneybin.connectors.gsheet.errors import (
    GSheetAPIError,
    GSheetAuthError,
    GSheetError,
    GSheetRateLimitError,
    GSheetUnreachableError,
)
from moneybin.errors import UserError


class TestGSheetErrorHierarchy:
    """Verify error class hierarchy and subclass relationships."""

    def test_error_hierarchy_subclasses_base(self) -> None:
        """All gsheet subclasses are subclasses of GSheetError."""
        subclasses = [
            GSheetAuthError,
            GSheetUnreachableError,
            GSheetRateLimitError,
            GSheetAPIError,
        ]
        for cls in subclasses:
            assert issubclass(cls, GSheetError)

    def test_gsheet_error_subclasses_user_error(self) -> None:
        """GSheetError is a subclass of UserError."""
        assert issubclass(GSheetError, UserError)

    def test_all_subclasses_emit_taxonomy_code(self) -> None:
        """Every gsheet error carries the taxonomy code, not a bare string.

        Locks the contract that the error class wires to error_codes.GSHEET_ERROR
        (in VALID_PREFIXES); the taxonomy test only checks the constant exists.
        """
        from moneybin.error_codes import GSHEET_ERROR

        for cls in (
            GSheetError,
            GSheetAuthError,
            GSheetUnreachableError,
            GSheetRateLimitError,
            GSheetAPIError,
        ):
            assert cls("boom").code == GSHEET_ERROR
