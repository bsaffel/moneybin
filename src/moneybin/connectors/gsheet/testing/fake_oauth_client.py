"""In-process stub for GoogleOAuthClient. Drives unit + integration tests.

Structurally satisfies the `OAuthCredentialsProvider` protocol from
`sheets_api.py`, so it can be passed wherever `SheetsClient` is expected.
"""

from __future__ import annotations


class TestOAuthClient:
    """Implements OAuthCredentialsProvider with canned auth state."""

    # Tell pytest this is a test stub, not a test class.
    __test__ = False

    def __init__(
        self,
        token: str = "test-token",  # noqa: S107  # test stub, harmless default
        authorized: bool = True,
    ) -> None:
        """Initialize with an optional token value and authorization state."""
        self._token = token
        self._authorized = authorized
        self.authorize_called = 0

    def is_authorized(self) -> bool:
        """Return whether the fake client has a refresh token."""
        return self._authorized

    def get_access_token(self) -> str:
        """Return the canned access token."""
        return self._token

    def authorize(self) -> None:
        """Simulate completing the OAuth flow; flips authorized to True."""
        self.authorize_called += 1
        self._authorized = True

    def revoke(self) -> None:
        """Simulate revoking credentials; flips authorized to False."""
        self._authorized = False

    # Test helpers ---------------------------------------------------------
    def expire_token(self) -> None:
        """Simulate an expired / revoked refresh token."""
        self._authorized = False
