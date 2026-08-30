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
from dataclasses import dataclass

from moneybin.config import GSHEET_PUBLIC_OAUTH_CLIENT_ID, MoneyBinSettings
from moneybin.connectors.gsheet.errors import GSheetAuthError
from moneybin.secrets import (
    GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
    GSHEET_ACCESS_TOKEN_KEY,
    GSHEET_REFRESH_TOKEN_KEY,
    GSHEET_WRITE_ACCESS_TOKEN_EXPIRES_KEY,
    GSHEET_WRITE_ACCESS_TOKEN_KEY,
    GSHEET_WRITE_REFRESH_TOKEN_KEY,
    SecretNotFoundError,
    SecretStore,
)

logger = logging.getLogger(__name__)

GOOGLE_SHEETS_READ_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
GOOGLE_SHEETS_WRITE_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
GSHEET_GRANTED_SCOPES_KEY = "gsheet:granted_scopes"  # noqa: S105  # keyring metadata name, not a secret value
GSHEET_WRITE_GRANTED_SCOPES_KEY = "gsheet:write_granted_scopes"  # noqa: S105  # keyring metadata name, not a secret value

# Refresh the access token slightly before it expires to avoid races.
_REFRESH_LEEWAY_SECONDS = 60


@dataclass(frozen=True, slots=True)
class OAuthGrant:
    """Persisted Google authorization capabilities without exposing tokens."""

    scopes: frozenset[str]

    @property
    def can_write(self) -> bool:
        """Return whether the grant permits Sheets mutations."""
        return GOOGLE_SHEETS_WRITE_SCOPE in self.scopes


@dataclass(frozen=True, slots=True)
class _CapabilityKeys:
    refresh: str
    access: str
    expires: str
    scopes: str


_READ_KEYS = _CapabilityKeys(
    GSHEET_REFRESH_TOKEN_KEY,
    GSHEET_ACCESS_TOKEN_KEY,
    GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
    GSHEET_GRANTED_SCOPES_KEY,
)
_WRITE_KEYS = _CapabilityKeys(
    GSHEET_WRITE_REFRESH_TOKEN_KEY,
    GSHEET_WRITE_ACCESS_TOKEN_KEY,
    GSHEET_WRITE_ACCESS_TOKEN_EXPIRES_KEY,
    GSHEET_WRITE_GRANTED_SCOPES_KEY,
)


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
    def is_authorized(self, *, require_write: bool = False) -> bool:
        """Return whether a persisted refresh grant satisfies the capability."""
        # A grant the next refresh cannot use is not an authorized state: the
        # CLI and MCP auth paths would report already_authorized and leave the
        # missing variable to surface as a generic failure at refresh time.
        try:
            self._oauth_client_credentials()
        except GSheetAuthError:
            return False
        keys = _WRITE_KEYS if require_write else _READ_KEYS
        try:
            self._secrets.get_key(keys.refresh)
        except SecretNotFoundError:
            return False
        return self._grant_satisfies(
            self._persisted_grant(require_write=require_write),
            require_write=require_write,
        )

    def get_access_token(self, *, require_write: bool = False) -> str:
        """Return a current token valid for the requested capability.

        Reuses the cached access token when it has at least
        ``_REFRESH_LEEWAY_SECONDS`` of remaining lifetime; otherwise
        refreshes via the persisted refresh token.
        """
        grant = self._persisted_grant(require_write=require_write)
        if not self._grant_satisfies(grant, require_write=require_write):
            capability = "write authorization" if require_write else "authorization"
            raise GSheetAuthError(
                f"Google Sheets {capability} is required. Re-authorize the connection."
            )
        cached = self._cached_access_token(require_write=require_write)
        if cached is not None:
            return cached
        return self._refresh_access_token(grant, require_write=require_write)

    def _oauth_client_credentials(self) -> tuple[str, str]:
        """Return the configured client id and secret, or refuse by name.

        Both grants need both values, and both fail unhelpfully without them:
        the browser flow only fails *after* the user has already consented,
        and google-auth's refresh raises a generic error naming no cause. So
        the refusal happens here, before either can start.
        """
        client_id = self._settings.gsheet.oauth_client_id
        if not client_id:
            raise GSheetAuthError(
                "Google Sheets OAuth client ID is not configured. Set "
                "MONEYBIN_GSHEET__OAUTH_CLIENT_ID. See "
                "docs/guides/connect-gsheet.md."
            )
        client_secret = self._settings.gsheet.oauth_client_secret
        if not client_secret:
            raise GSheetAuthError(
                "Google Sheets OAuth client secret is not configured. Google's "
                "Desktop clients require it alongside the client ID for both "
                "the authorization exchange and later token refreshes. Set "
                "MONEYBIN_GSHEET__OAUTH_CLIENT_SECRET. See "
                "docs/guides/connect-gsheet.md."
            )
        # Google issues each secret for one specific client ID, and MoneyBin
        # ships none for the embedded one. So a secret set without its own ID
        # is a mismatched pair by construction, and Google only says so after
        # the user has consented.
        if client_id == GSHEET_PUBLIC_OAUTH_CLIENT_ID:
            raise GSheetAuthError(
                "Google Sheets OAuth client secret is set, but the client ID is "
                "still MoneyBin's embedded one, which has no secret. A secret "
                "belongs to the client ID it was issued with, so set your own "
                "MONEYBIN_GSHEET__OAUTH_CLIENT_ID alongside it. See "
                "docs/guides/connect-gsheet.md."
            )
        return client_id, client_secret.get_secret_value()

    def authorize(self, *, require_write: bool = False) -> OAuthGrant:
        """Establish or incrementally upgrade the persisted OAuth grant."""
        client_id, client_secret = self._oauth_client_credentials()

        # Import lazily so the connector is importable in environments that
        # don't have google-auth-oauthlib installed (e.g. minimal CI jobs).
        from google_auth_oauthlib.flow import InstalledAppFlow

        client_config = {
            "installed": {
                "client_id": client_id,
                # Google's Desktop clients require the secret in the
                # code->token exchange even under PKCE. RFC 8252 §8.5: a
                # secret shipped to every user is not confidential; PKCE and
                # the loopback redirect carry the security.
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                # 127.0.0.1, not "localhost": on hosts where localhost
                # resolves to ::1 (IPv6) the callback can miss the IPv4
                # loopback server bound below, or be intercepted by a
                # local IPv6 listener on the same port.
                "redirect_uris": ["http://127.0.0.1"],
            }
        }
        requested_scope = (
            GOOGLE_SHEETS_WRITE_SCOPE if require_write else GOOGLE_SHEETS_READ_SCOPE
        )
        try:
            flow = InstalledAppFlow.from_client_config(  # type: ignore[reportUnknownMemberType]
                client_config, [requested_scope]
            )
            # Google recommends incremental authorization in context. The
            # combined grant applies to refreshes even when an upgrade omits a
            # replacement refresh token, so we retain the existing token below:
            # https://developers.google.com/identity/protocols/oauth2/web-server#incrementalAuth
            creds = flow.run_local_server(  # type: ignore[reportUnknownMemberType]
                # port=0 → google-auth-oauthlib picks any free ephemeral port.
                port=0,
                bind_addr="127.0.0.1",
                access_type="offline",
                include_granted_scopes="true" if require_write else "false",
            )
        except Exception as exc:  # noqa: BLE001  # google-auth raises untyped errors
            # str(exc) on google_auth_oauthlib errors can include OAuth
            # state params, redirect URIs, or CSRF token fragments — keep
            # the typed exception message generic so downstream
            # logger.warning(str(e)) or envelope construction doesn't leak.
            # Log the chain only at debug (exc_info) so the token/state
            # fragments don't land in error-level logs that ship by default.
            logger.error("OAuth authorization flow failed")
            logger.debug("OAuth authorization flow failure detail", exc_info=exc)
            raise GSheetAuthError(
                "OAuth authorization failed. See application logs for detail."
            ) from exc

        refresh_token = getattr(creds, "refresh_token", None)
        if not refresh_token:
            fallback_keys = (_WRITE_KEYS, _READ_KEYS)
            for fallback in fallback_keys:
                try:
                    refresh_token = self._secrets.get_key(fallback.refresh)
                    break
                except SecretNotFoundError:
                    continue
        access_token = getattr(creds, "token", None)
        expiry = getattr(creds, "expiry", None)
        if not refresh_token or not access_token:
            raise GSheetAuthError(
                "OAuth flow completed without returning refresh + access tokens."
            )

        raw_scopes = getattr(creds, "granted_scopes", None) or getattr(
            creds, "scopes", None
        )
        granted_scopes = frozenset(raw_scopes or [requested_scope])
        grant = OAuthGrant(scopes=granted_scopes)
        if not self._grant_satisfies(grant, require_write=require_write):
            scope = "write scope" if require_write else "read-only scope"
            raise GSheetAuthError(
                f"OAuth authorization completed, but Google Sheets {scope} was not "
                "granted."
            )

        keys = _WRITE_KEYS if require_write else _READ_KEYS
        self._secrets.set_key(keys.refresh, refresh_token)
        self._secrets.set_key(keys.access, access_token)
        self._secrets.set_key(keys.scopes, " ".join(sorted(granted_scopes)))
        if expiry is not None:
            self._secrets.set_key(
                keys.expires,
                str(int(expiry.timestamp())),
            )
        logger.info("gsheet OAuth authorize completed")
        return grant

    def revoke(self) -> None:
        """Clear all persisted OAuth secrets.

        Silent if a key is already missing — multiple revoke calls and
        partial-state cleanup must both succeed.
        """
        for key in (
            GSHEET_REFRESH_TOKEN_KEY,
            GSHEET_ACCESS_TOKEN_KEY,
            GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
            GSHEET_GRANTED_SCOPES_KEY,
            GSHEET_WRITE_REFRESH_TOKEN_KEY,
            GSHEET_WRITE_ACCESS_TOKEN_KEY,
            GSHEET_WRITE_ACCESS_TOKEN_EXPIRES_KEY,
            GSHEET_WRITE_GRANTED_SCOPES_KEY,
        ):
            try:
                self._secrets.delete_key(key)
            except SecretNotFoundError:
                continue
        logger.info("gsheet OAuth credentials revoked")

    # Internal helpers ----------------------------------------------------
    def _cached_access_token(self, *, require_write: bool) -> str | None:
        """Return the cached access token if still valid, else None."""
        keys = _WRITE_KEYS if require_write else _READ_KEYS
        try:
            token = self._secrets.get_key(keys.access)
            expires_at_raw = self._secrets.get_key(keys.expires)
        except SecretNotFoundError:
            return None
        try:
            expires_at = int(expires_at_raw)
        except ValueError:
            return None
        if expires_at - _REFRESH_LEEWAY_SECONDS <= int(time.time()):
            return None
        return token

    def _persisted_grant(self, *, require_write: bool) -> OAuthGrant:
        """Load capability metadata, treating pre-metadata grants as read-only."""
        keys = _WRITE_KEYS if require_write else _READ_KEYS
        try:
            raw_scopes = self._secrets.get_key(keys.scopes)
        except SecretNotFoundError:
            scopes: set[str] = (
                {GOOGLE_SHEETS_READ_SCOPE} if not require_write else set()
            )
            return OAuthGrant(scopes=frozenset(scopes))
        return OAuthGrant(scopes=frozenset(raw_scopes.split()))

    def _refresh_access_token(self, grant: OAuthGrant, *, require_write: bool) -> str:
        """Use the persisted refresh token to mint a new access token."""
        keys = _WRITE_KEYS if require_write else _READ_KEYS
        try:
            refresh_token = self._secrets.get_key(keys.refresh)
        except SecretNotFoundError as exc:
            raise GSheetAuthError(
                "No Google Sheets refresh token is persisted. Run "
                "`moneybin gsheet connect` to authorize."
            ) from exc

        # Google's refresh grant needs the secret too, and google-auth refuses
        # locally without it (Credentials._perform_refresh_token raises a
        # RefreshError naming no cause), which this method's broad handler
        # would flatten into "See application logs for detail."
        client_id, client_secret = self._oauth_client_credentials()

        # Lazy import for the same reason as authorize().
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials

        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",  # noqa: S106  # Google OAuth endpoint URL, not a credential
            client_id=client_id,
            client_secret=client_secret,
            scopes=sorted(grant.scopes),
        )
        try:
            creds.refresh(Request())  # type: ignore[reportUnknownMemberType]
        except Exception as exc:  # noqa: BLE001  # google-auth raises untyped errors
            # Same sanitization discipline as authorize() — google-auth
            # error text can carry token fragments / endpoint URLs, so the
            # chain goes to debug-only, not error-level logs.
            logger.error("OAuth token refresh failed")
            logger.debug("OAuth token refresh failure detail", exc_info=exc)
            raise GSheetAuthError(
                "OAuth token refresh failed. See application logs for detail."
            ) from exc

        access_token = getattr(creds, "token", None)
        expiry = getattr(creds, "expiry", None)
        if not access_token:
            raise GSheetAuthError("Token refresh did not return an access token.")

        raw_scopes = getattr(creds, "granted_scopes", None)
        if raw_scopes is None:
            raw_scopes = getattr(creds, "scopes", None)
        refreshed_grant = OAuthGrant(scopes=frozenset(raw_scopes or ()))
        if not self._grant_satisfies(refreshed_grant, require_write=require_write):
            for cache_key in (keys.access, keys.expires):
                try:
                    self._secrets.delete_key(cache_key)
                except SecretNotFoundError:
                    continue
            self._secrets.set_key(keys.scopes, " ".join(sorted(refreshed_grant.scopes)))
            raise GSheetAuthError(
                "OAuth token refresh no longer grants the required Google Sheets "
                "scope. Re-authorize the connection."
            )

        self._secrets.set_key(keys.access, access_token)
        self._secrets.set_key(keys.scopes, " ".join(sorted(refreshed_grant.scopes)))
        if expiry is not None:
            self._secrets.set_key(
                keys.expires,
                str(int(expiry.timestamp())),
            )
        return access_token

    @staticmethod
    def _grant_satisfies(grant: OAuthGrant, *, require_write: bool) -> bool:
        if require_write:
            return GOOGLE_SHEETS_WRITE_SCOPE in grant.scopes
        return grant.scopes == frozenset({GOOGLE_SHEETS_READ_SCOPE})
