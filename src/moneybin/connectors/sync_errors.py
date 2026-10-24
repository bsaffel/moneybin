"""Typed exceptions raised by SyncClient.

The CLI's handle_cli_errors maps these to user-facing messages + exit codes.
"""


class SyncError(Exception):
    """Base for all sync-related errors."""


class SyncAuthError(SyncError):
    """Authentication failed: missing token, refresh failed, user denied device flow."""


class SyncLinkError(SyncError):
    """Link session terminated with status='failed' on the server."""


class SyncTimeoutError(SyncError):
    """Operation exceeded its timeout (long poll, blocking trigger)."""


class SyncAPIError(SyncError):
    """Generic server error (5xx, unexpected response shape)."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        """Carry bounded transport status separately from safe public text."""
        super().__init__(message)
        self.status_code = (
            status_code
            if status_code is not None and 400 <= status_code <= 599
            else None
        )
