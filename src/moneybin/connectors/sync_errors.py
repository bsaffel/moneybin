"""Typed exceptions raised by SyncClient.

The CLI's handle_cli_errors maps these to user-facing messages + exit codes.
"""


class SyncError(Exception):
    """Base for all sync-related errors."""


class SyncAuthError(SyncError):
    """Authentication failed: missing token, refresh failed, user denied device flow."""


class SyncConnectError(SyncError):
    """Connect session terminated with status='failed' on the server."""


class SyncTimeoutError(SyncError):
    """Operation exceeded its timeout (long poll, blocking trigger)."""


class SyncAPIError(SyncError):
    """Generic server error (5xx, unexpected response shape)."""
