"""In-process stub for GoogleOAuthClient. Drives unit + integration tests.

Structurally satisfies the `OAuthCredentialsProvider` protocol from
`sheets_api.py`, so it can be passed wherever `SheetsClient` is expected.
"""

from __future__ import annotations

from moneybin.connectors.gsheet.errors import GSheetAuthError
from moneybin.connectors.gsheet.oauth_client import (
    GOOGLE_SHEETS_READ_SCOPE,
    GOOGLE_SHEETS_WRITE_SCOPE,
    OAuthGrant,
)


class TestOAuthClient:
    """Implements OAuthCredentialsProvider with canned auth state."""

    # Tell pytest this is a test stub, not a test class.
    __test__ = False

    def __init__(
        self,
        token: str = "test-token",  # noqa: S107  # test stub, harmless default
        authorized: bool = True,
        write_authorized: bool = False,
    ) -> None:
        """Initialize with an optional token value and authorization state."""
        self._token = token
        self._authorized = authorized
        self._write_authorized = authorized and write_authorized
        self.authorize_called = 0
        self.authorize_require_write: list[bool] = []

    def is_authorized(self, *, require_write: bool = False) -> bool:
        """Return whether the fake has the requested persisted capability."""
        return self._authorized and (not require_write or self._write_authorized)

    def get_access_token(self, *, require_write: bool = False) -> str:
        """Return the canned access token when its grant is sufficient."""
        if require_write and not self._write_authorized:
            raise GSheetAuthError("Google Sheets write authorization is required")
        return self._token

    def authorize(self, *, require_write: bool = False) -> OAuthGrant:
        """Simulate completing the requested OAuth flow."""
        self.authorize_called += 1
        self.authorize_require_write.append(require_write)
        self._authorized = True
        self._write_authorized = self._write_authorized or require_write
        scopes = {GOOGLE_SHEETS_READ_SCOPE}
        if self._write_authorized:
            scopes.add(GOOGLE_SHEETS_WRITE_SCOPE)
        return OAuthGrant(scopes=frozenset(scopes))

    def revoke(self) -> None:
        """Simulate revoking credentials; flips authorized to False."""
        self._authorized = False
        self._write_authorized = False

    # Test helpers ---------------------------------------------------------
    def expire_token(self) -> None:
        """Simulate an expired / revoked refresh token."""
        self._authorized = False
