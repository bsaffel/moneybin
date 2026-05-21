"""Google OAuth 2.0 client for the Sheets connector (installed-app + PKCE).

Tokens (refresh + cached access token + expiry) are persisted to the
project's `SecretStore`. The browser flow itself lives in
`google-auth-oauthlib`'s `InstalledAppFlow.run_local_server`, which handles
PKCE and the loopback redirect.

This module's responsibility is the wiring around that flow: secret-store
persistence, cached-token reuse, refresh, and revoke. The actual PKCE
browser dance is not unit-testable without manual interaction and is
covered by an explicit real-network/manual test.
"""

from __future__ import annotations

import logging
import time

from moneybin.config import MoneyBinSettings
from moneybin.connectors.gsheet.errors import GSheetAuthError
from moneybin.secrets import (
    GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
    GSHEET_ACCESS_TOKEN_KEY,
    GSHEET_REFRESH_TOKEN_KEY,
    SecretNotFoundError,
    SecretStore,
)

logger = logging.getLogger(__name__)

# Read-only Sheets scope; drive.readonly is intentionally NOT requested.
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# Refresh the access token slightly before it expires to avoid races.
_REFRESH_LEEWAY_SECONDS = 60


class GoogleOAuthClient:
    """OAuth 2.0 installed-app + PKCE flow for the Sheets connector."""

    def __init__(
        self,
        secrets: SecretStore,
        settings: MoneyBinSettings,
    ) -> None:
        """Initialize with persisted secret store and resolved settings."""
        self._secrets = secrets
        self._settings = settings

    # OAuthCredentialsProvider protocol -----------------------------------
    def is_authorized(self) -> bool:
        """Return True iff a refresh token is persisted."""
        try:
            self._secrets.get_key(GSHEET_REFRESH_TOKEN_KEY)
        except SecretNotFoundError:
            return False
        return True

    def get_access_token(self) -> str:
        """Return a current access token, refreshing if necessary.

        Reuses the cached access token when it has at least
        ``_REFRESH_LEEWAY_SECONDS`` of remaining lifetime; otherwise
        refreshes via the persisted refresh token.
        """
        cached = self._cached_access_token()
        if cached is not None:
            return cached
        return self._refresh_access_token()

    def authorize(self) -> None:
        """Run the installed-app + PKCE browser flow and persist tokens."""
        client_id = self._settings.gsheet.oauth_client_id
        if not client_id:
            raise GSheetAuthError(
                "Google Sheets OAuth client ID is not configured. Set "
                "MONEYBIN_GSHEET__OAUTH_CLIENT_ID before running `gsheet connect`."
            )

        # Import lazily so the connector is importable in environments that
        # don't have google-auth-oauthlib installed (e.g. minimal CI jobs).
        from google_auth_oauthlib.flow import InstalledAppFlow

        client_config = {
            "installed": {
                "client_id": client_id,
                # PKCE-only flow: no client secret required for an installed
                # app. google-auth-oauthlib accepts an empty string here.
                "client_secret": "",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://localhost"],
            }
        }
        flow = InstalledAppFlow.from_client_config(  # type: ignore[reportUnknownMemberType]
            client_config, _SCOPES
        )
        try:
            creds = flow.run_local_server(  # type: ignore[reportUnknownMemberType]
                # port=0 → google-auth-oauthlib picks any free ephemeral port.
                # The redirect_port_min/max settings on GSheetSettings are
                # reserved for a future wired-up implementation and currently
                # do not constrain port selection.
                port=0,
                bind_addr="127.0.0.1",
            )
        except Exception as exc:  # noqa: BLE001  # google-auth raises untyped errors
            # str(exc) on google_auth_oauthlib errors can include OAuth
            # state params, redirect URIs, or CSRF token fragments — keep
            # the typed exception message generic so downstream
            # logger.warning(str(e)) or envelope construction doesn't leak.
            logger.exception("OAuth authorization flow failed")
            raise GSheetAuthError(
                "OAuth authorization failed. See application logs for detail."
            ) from exc

        refresh_token = getattr(creds, "refresh_token", None)
        access_token = getattr(creds, "token", None)
        expiry = getattr(creds, "expiry", None)
        if not refresh_token or not access_token:
            raise GSheetAuthError(
                "OAuth flow completed without returning refresh + access tokens."
            )

        self._secrets.set_key(GSHEET_REFRESH_TOKEN_KEY, refresh_token)
        self._secrets.set_key(GSHEET_ACCESS_TOKEN_KEY, access_token)
        if expiry is not None:
            self._secrets.set_key(
                GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
                str(int(expiry.timestamp())),
            )
        logger.info("gsheet OAuth authorize completed")

    def revoke(self) -> None:
        """Clear all persisted OAuth secrets.

        Silent if a key is already missing — multiple revoke calls and
        partial-state cleanup must both succeed.
        """
        for key in (
            GSHEET_REFRESH_TOKEN_KEY,
            GSHEET_ACCESS_TOKEN_KEY,
            GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
        ):
            try:
                self._secrets.delete_key(key)
            except SecretNotFoundError:
                continue
        logger.info("gsheet OAuth credentials revoked")

    # Internal helpers ----------------------------------------------------
    def _cached_access_token(self) -> str | None:
        """Return the cached access token if still valid, else None."""
        try:
            token = self._secrets.get_key(GSHEET_ACCESS_TOKEN_KEY)
            expires_at_raw = self._secrets.get_key(GSHEET_ACCESS_TOKEN_EXPIRES_KEY)
        except SecretNotFoundError:
            return None
        try:
            expires_at = int(expires_at_raw)
        except ValueError:
            return None
        if expires_at - _REFRESH_LEEWAY_SECONDS <= int(time.time()):
            return None
        return token

    def _refresh_access_token(self) -> str:
        """Use the persisted refresh token to mint a new access token."""
        try:
            refresh_token = self._secrets.get_key(GSHEET_REFRESH_TOKEN_KEY)
        except SecretNotFoundError as exc:
            raise GSheetAuthError(
                "No Google Sheets refresh token is persisted. Run "
                "`moneybin gsheet connect` to authorize."
            ) from exc

        client_id = self._settings.gsheet.oauth_client_id
        if not client_id:
            raise GSheetAuthError(
                "Google Sheets OAuth client ID is not configured. Set "
                "MONEYBIN_GSHEET__OAUTH_CLIENT_ID."
            )

        # Lazy import for the same reason as authorize().
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials

        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",  # noqa: S106  # Google OAuth endpoint URL, not a credential
            client_id=client_id,
            client_secret=None,
            scopes=_SCOPES,
        )
        try:
            creds.refresh(Request())  # type: ignore[reportUnknownMemberType]
        except Exception as exc:  # noqa: BLE001  # google-auth raises untyped errors
            # Same sanitization discipline as authorize() — google-auth
            # error text can carry token fragments / endpoint URLs.
            logger.exception("OAuth token refresh failed")
            raise GSheetAuthError(
                "OAuth token refresh failed. See application logs for detail."
            ) from exc

        access_token = getattr(creds, "token", None)
        expiry = getattr(creds, "expiry", None)
        if not access_token:
            raise GSheetAuthError("Token refresh did not return an access token.")

        self._secrets.set_key(GSHEET_ACCESS_TOKEN_KEY, access_token)
        if expiry is not None:
            self._secrets.set_key(
                GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
                str(int(expiry.timestamp())),
            )
        return access_token
