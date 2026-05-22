"""Typed exceptions raised by Google Sheets connector.

The CLI's handle_cli_errors maps these to user-facing messages + exit codes.
"""

from moneybin.errors import UserError


class GSheetError(UserError):
    """Base for all Google Sheets connector errors."""

    def __init__(self, message: str) -> None:
        """Initialize with a user-safe message."""
        # NOTE: "gsheet_error" is not yet in moneybin.error_codes' prefix
        # taxonomy (added by #193). Reconciling connector error codes with
        # the taxonomy is deferred to the recovery-contract retrofit
        # (data-recovery-contract.md Req 3 / PRs 9a-N), which migrates all
        # connector codes uniformly. Tracked in private/followups.md.
        super().__init__(message, code="gsheet_error")


class GSheetAuthError(GSheetError):
    """OAuth flow failed or refresh token revoked."""


class GSheetUnreachableError(GSheetError):
    """Sheet deleted, unshared, or inaccessible (403/404/network)."""


class GSheetRateLimitError(GSheetError):
    """Google API rate-limited (429)."""


class GSheetAPIError(GSheetError):
    """Other Google API errors not classified above."""


# Note: drift is propagated via the DriftReport return value from
# detect_drift() / GSheetAdapter.check_drift(), not via an exception. The
# pull pipeline converts a drift_report.is_drift=True into PullResult(
# status="drift_detected", drift_reason=...) and an app.gsheet_connections
# status update — no exception leaves the adapter. If a future call-site
# needs the exception form, define it then; we intentionally don't keep a
# dead class around.
