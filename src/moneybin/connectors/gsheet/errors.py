"""Typed exceptions raised by Google Sheets connector.

The CLI's handle_cli_errors maps these to user-facing messages + exit codes.
"""

from moneybin.errors import UserError


class GSheetError(UserError):
    """Base for all Google Sheets connector errors."""

    def __init__(self, message: str) -> None:
        """Initialize with a user-safe message."""
        super().__init__(message, code="gsheet_error")


class GSheetAuthError(GSheetError):
    """OAuth flow failed or refresh token revoked."""


class GSheetUnreachableError(GSheetError):
    """Sheet deleted, unshared, or inaccessible (403/404/network)."""


class GSheetRateLimitError(GSheetError):
    """Google API rate-limited (429)."""


class GSheetAPIError(GSheetError):
    """Other Google API errors not classified above."""


class GSheetDriftError(GSheetError):
    """Sheet structure no longer matches pinned mapping."""

    def __init__(
        self,
        reason: str,
        missing: list[str] | None = None,
        type_changed: list[str] | None = None,
    ) -> None:
        """Initialize with reason and optional lists of drift details.

        Args:
            reason: Description of the schema mismatch
            missing: Columns that were expected but are missing
            type_changed: Columns whose types have changed
        """
        super().__init__(reason)
        self.reason = reason
        self.missing = missing if missing is not None else []
        self.type_changed = type_changed if type_changed is not None else []
