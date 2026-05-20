"""Tests for gsheet error hierarchy."""

from moneybin.connectors.gsheet.errors import (
    GSheetAPIError,
    GSheetAuthError,
    GSheetDriftError,
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
            GSheetDriftError,
        ]
        for cls in subclasses:
            assert issubclass(cls, GSheetError)

    def test_gsheet_error_subclasses_user_error(self) -> None:
        """GSheetError is a subclass of UserError."""
        assert issubclass(GSheetError, UserError)


class TestGSheetDriftError:
    """Tests specific to GSheetDriftError constructor and attributes."""

    def test_drift_error_carries_reason(self) -> None:
        """GSheetDriftError exposes reason, missing, and type_changed attributes."""
        missing_cols = ["Column1", "Column2"]
        type_changed_cols = ["Amount"]
        error = GSheetDriftError(
            reason="Schema mismatch",
            missing=missing_cols,
            type_changed=type_changed_cols,
        )
        assert error.reason == "Schema mismatch"
        assert error.missing == missing_cols
        assert error.type_changed == type_changed_cols

    def test_drift_error_string_form(self) -> None:
        """str(GSheetDriftError) contains the reason."""
        reason = "Column missing"
        error = GSheetDriftError(reason=reason)
        assert reason in str(error)

    def test_drift_error_defaults(self) -> None:
        """GSheetDriftError defaults missing and type_changed to empty lists."""
        error = GSheetDriftError(reason="x")
        assert error.missing == []
        assert error.type_changed == []
        assert isinstance(error.missing, list)
        assert isinstance(error.type_changed, list)
