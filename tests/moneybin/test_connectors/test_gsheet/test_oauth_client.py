"""Tests for GoogleOAuthClient + TestOAuthClient stub."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from moneybin.config import MoneyBinSettings
from moneybin.connectors.gsheet.errors import GSheetAuthError
from moneybin.connectors.gsheet.oauth_client import (
    GOOGLE_SHEETS_READ_SCOPE,
    GOOGLE_SHEETS_WRITE_SCOPE,
    GSHEET_GRANTED_SCOPES_KEY,
    GSHEET_WRITE_GRANTED_SCOPES_KEY,
    GoogleOAuthClient,
)
from moneybin.connectors.gsheet.sheets_api import OAuthCredentialsProvider
from moneybin.connectors.gsheet.testing.fake_oauth_client import TestOAuthClient
from moneybin.secrets import (
    GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
    GSHEET_ACCESS_TOKEN_KEY,
    GSHEET_REFRESH_TOKEN_KEY,
    GSHEET_WRITE_ACCESS_TOKEN_EXPIRES_KEY,
    GSHEET_WRITE_ACCESS_TOKEN_KEY,
    GSHEET_WRITE_REFRESH_TOKEN_KEY,
    SecretNotFoundError,
)

# -- TestOAuthClient stub -----------------------------------------------------


def test_fake_oauth_starts_authorized_by_default() -> None:
    client = TestOAuthClient()
    assert client.is_authorized() is True


def test_fake_oauth_can_start_unauthorized() -> None:
    client = TestOAuthClient(authorized=False)
    assert client.is_authorized() is False


def test_fake_oauth_authorize_flips_state_and_counts() -> None:
    client = TestOAuthClient(authorized=False)
    client.authorize()
    assert client.is_authorized() is True
    assert client.authorize_called == 1


def test_fake_oauth_revoke_flips_state() -> None:
    client = TestOAuthClient()
    client.revoke()
    assert client.is_authorized() is False


def test_fake_oauth_expire_token_revokes() -> None:
    client = TestOAuthClient()
    client.expire_token()
    assert client.is_authorized() is False


def test_fake_oauth_default_token() -> None:
    client = TestOAuthClient()
    assert client.get_access_token() == "test-token"


def test_fake_oauth_requires_explicit_write_grant() -> None:
    client = TestOAuthClient(write_authorized=False)

    with pytest.raises(GSheetAuthError, match="write authorization"):
        client.get_access_token(require_write=True)

    assert client.get_access_token(require_write=False) == "test-token"


def test_fake_oauth_can_upgrade_to_write() -> None:
    client = TestOAuthClient(write_authorized=False)

    grant = client.authorize(require_write=True)

    assert grant.can_write is True
    assert client.is_authorized(require_write=True) is True
    assert client.authorize_require_write == [True]


def test_fake_oauth_implements_oauth_credentials_provider_protocol() -> None:
    """TestOAuthClient must structurally satisfy OAuthCredentialsProvider."""
    client: OAuthCredentialsProvider = TestOAuthClient()
    assert callable(client.get_access_token)


# -- GoogleOAuthClient --------------------------------------------------------


def _make_settings(
    client_id: str = "fake-client-id.apps.googleusercontent.com",
    client_secret: str | None = "fake-client-secret",  # noqa: S107  # test credential
) -> MoneyBinSettings:
    """Build a settings instance with a configured gsheet client id and secret.

    Both are configured by default because authorize() refuses without a
    secret; pass client_secret=None to exercise that refusal.
    """
    return MoneyBinSettings.model_validate({
        "gsheet": {
            "oauth_client_id": client_id,
            "oauth_client_secret": client_secret,
        }
    })


def _store_with(values: dict[str, str | None]) -> MagicMock:
    """Build a SecretStore mock; values mapping `key -> value` or `key -> None` to raise."""

    def _get(name: str) -> str:
        if name not in values or values[name] is None:
            raise SecretNotFoundError(f"missing: {name}")
        val = values[name]
        assert val is not None  # narrow for type-checker
        return val

    store = MagicMock()
    store.get_key.side_effect = _get
    return store


def test_google_oauth_is_authorized_true_when_refresh_token_present() -> None:
    store = _store_with({GSHEET_REFRESH_TOKEN_KEY: "refresh-abc"})
    client = GoogleOAuthClient(store, _make_settings())
    assert client.is_authorized() is True


def test_google_oauth_legacy_refresh_token_is_readonly_not_write_capable() -> None:
    store = _store_with({GSHEET_REFRESH_TOKEN_KEY: "refresh-abc"})
    client = GoogleOAuthClient(store, _make_settings())

    assert client.is_authorized(require_write=False) is True
    assert client.is_authorized(require_write=True) is False

    with pytest.raises(GSheetAuthError, match="write authorization"):
        client.get_access_token(require_write=True)


def test_google_oauth_is_authorized_false_when_secret_not_found() -> None:
    store = _store_with({})
    client = GoogleOAuthClient(store, _make_settings())
    assert client.is_authorized() is False


def test_google_oauth_get_access_token_returns_cached_when_unexpired() -> None:
    future = int(time.time()) + 3600
    store = _store_with({
        GSHEET_ACCESS_TOKEN_KEY: "cached-access",
        GSHEET_ACCESS_TOKEN_EXPIRES_KEY: str(future),
    })
    client = GoogleOAuthClient(store, _make_settings())

    result = client.get_access_token()

    assert result == "cached-access"
    # Refresh path must not have been touched — only the two cache keys were read.
    read_names = [call.args[0] for call in store.get_key.call_args_list]
    assert GSHEET_REFRESH_TOKEN_KEY not in read_names


def test_google_oauth_get_access_token_raises_when_no_refresh_token() -> None:
    # No cached access token, no refresh token — must raise GSheetAuthError.
    store = _store_with({})
    client = GoogleOAuthClient(store, _make_settings())
    with pytest.raises(GSheetAuthError, match="refresh token"):
        client.get_access_token()


def test_google_oauth_revoke_deletes_all_grant_keys() -> None:
    store = MagicMock()
    client = GoogleOAuthClient(store, _make_settings())

    client.revoke()

    deleted = [call.args[0] for call in store.delete_key.call_args_list]
    assert set(deleted) == {
        GSHEET_REFRESH_TOKEN_KEY,
        GSHEET_ACCESS_TOKEN_KEY,
        GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
        GSHEET_GRANTED_SCOPES_KEY,
        GSHEET_WRITE_REFRESH_TOKEN_KEY,
        GSHEET_WRITE_ACCESS_TOKEN_KEY,
        GSHEET_WRITE_ACCESS_TOKEN_EXPIRES_KEY,
        GSHEET_WRITE_GRANTED_SCOPES_KEY,
    }
    assert store.delete_key.call_count == 8


def test_google_oauth_revoke_survives_missing_keys() -> None:
    store = MagicMock()
    store.delete_key.side_effect = SecretNotFoundError("missing")
    client = GoogleOAuthClient(store, _make_settings())
    # Should not raise even when every key is already gone.
    client.revoke()
    assert store.delete_key.call_count == 8


def test_google_oauth_cached_read_token_is_rejected_for_write() -> None:
    future = int(time.time()) + 3600
    store = _store_with({
        GSHEET_REFRESH_TOKEN_KEY: "read-refresh",
        GSHEET_ACCESS_TOKEN_KEY: "read-access",
        GSHEET_ACCESS_TOKEN_EXPIRES_KEY: str(future),
        GSHEET_GRANTED_SCOPES_KEY: GOOGLE_SHEETS_READ_SCOPE,
    })
    client = GoogleOAuthClient(store, _make_settings())

    with pytest.raises(GSheetAuthError, match="write authorization"):
        client.get_access_token(require_write=True)


def test_google_oauth_write_grant_never_serves_a_read_request() -> None:
    future = int(time.time()) + 3600
    store = _store_with({
        GSHEET_REFRESH_TOKEN_KEY: "read-refresh",
        GSHEET_ACCESS_TOKEN_KEY: "read-access",
        GSHEET_ACCESS_TOKEN_EXPIRES_KEY: str(future),
        GSHEET_GRANTED_SCOPES_KEY: GOOGLE_SHEETS_READ_SCOPE,
        GSHEET_WRITE_REFRESH_TOKEN_KEY: "write-refresh",
        GSHEET_WRITE_ACCESS_TOKEN_KEY: "write-access",
        GSHEET_WRITE_ACCESS_TOKEN_EXPIRES_KEY: str(future),
        GSHEET_WRITE_GRANTED_SCOPES_KEY: GOOGLE_SHEETS_WRITE_SCOPE,
    })
    client = GoogleOAuthClient(store, _make_settings())

    assert client.get_access_token(require_write=False) == "read-access"
    assert client.get_access_token(require_write=True) == "write-access"
    read_names = [call.args[0] for call in store.get_key.call_args_list[:3]]
    assert GSHEET_WRITE_ACCESS_TOKEN_KEY not in read_names


def test_google_oauth_upgrade_retains_refresh_token_when_google_omits_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_with({
        GSHEET_REFRESH_TOKEN_KEY: "existing-read-refresh",
        GSHEET_GRANTED_SCOPES_KEY: GOOGLE_SHEETS_READ_SCOPE,
    })
    creds = MagicMock(
        refresh_token=None,
        token="combined-access",  # noqa: S106  # test credential
        expiry=None,
        granted_scopes=[GOOGLE_SHEETS_WRITE_SCOPE],
        scopes=[GOOGLE_SHEETS_WRITE_SCOPE],
    )
    flow = MagicMock()
    flow.run_local_server.return_value = creds
    from google_auth_oauthlib.flow import InstalledAppFlow

    from_config = MagicMock(return_value=flow)
    monkeypatch.setattr(InstalledAppFlow, "from_client_config", from_config)
    client = GoogleOAuthClient(store, _make_settings())

    grant = client.authorize(require_write=True)

    assert grant.can_write is True
    from_config.assert_called_once()
    assert from_config.call_args.args[1] == [GOOGLE_SHEETS_WRITE_SCOPE]
    assert flow.run_local_server.call_args.kwargs["include_granted_scopes"] == "true"
    store.set_key.assert_any_call(
        GSHEET_WRITE_REFRESH_TOKEN_KEY, "existing-read-refresh"
    )
    store.set_key.assert_any_call(
        GSHEET_WRITE_GRANTED_SCOPES_KEY, GOOGLE_SHEETS_WRITE_SCOPE
    )
    assert not any(
        call.args[0] == GSHEET_REFRESH_TOKEN_KEY
        for call in store.set_key.call_args_list
    )


def test_google_oauth_read_authorization_reuses_write_refresh_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_with({
        GSHEET_WRITE_REFRESH_TOKEN_KEY: "existing-write-refresh",
    })
    creds = MagicMock(
        refresh_token=None,
        token="read-access",  # noqa: S106  # test credential
        expiry=None,
        granted_scopes=[GOOGLE_SHEETS_READ_SCOPE],
        scopes=[GOOGLE_SHEETS_READ_SCOPE],
    )
    flow = MagicMock()
    flow.run_local_server.return_value = creds
    from google_auth_oauthlib.flow import InstalledAppFlow

    monkeypatch.setattr(
        InstalledAppFlow,
        "from_client_config",
        MagicMock(return_value=flow),
    )

    grant = GoogleOAuthClient(store, _make_settings()).authorize(require_write=False)

    assert grant.can_write is False
    store.set_key.assert_any_call(GSHEET_REFRESH_TOKEN_KEY, "existing-write-refresh")


def test_google_oauth_write_upgrade_rejects_partial_scope_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_with({
        GSHEET_REFRESH_TOKEN_KEY: "existing-read-refresh",
        GSHEET_GRANTED_SCOPES_KEY: GOOGLE_SHEETS_READ_SCOPE,
    })
    creds = MagicMock(
        refresh_token=None,
        token="read-access",  # noqa: S106  # test credential
        expiry=None,
        granted_scopes=[GOOGLE_SHEETS_READ_SCOPE],
        scopes=[GOOGLE_SHEETS_WRITE_SCOPE],
    )
    flow = MagicMock()
    flow.run_local_server.return_value = creds
    from google_auth_oauthlib.flow import InstalledAppFlow

    monkeypatch.setattr(
        InstalledAppFlow, "from_client_config", MagicMock(return_value=flow)
    )
    client = GoogleOAuthClient(store, _make_settings())

    with pytest.raises(GSheetAuthError, match="write scope was not granted"):
        client.authorize(require_write=True)

    store.set_key.assert_not_called()


def test_google_oauth_authorize_raises_when_client_id_empty() -> None:
    store = MagicMock()
    client = GoogleOAuthClient(store, _make_settings(client_id=""))
    with pytest.raises(GSheetAuthError, match="client ID is not configured"):
        client.authorize()
    # Must fail before touching the secret store.
    store.set_key.assert_not_called()


def test_google_oauth_authorize_sanitizes_google_auth_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real Google auth exception must not expose OAuth state to callers."""
    from google.auth.exceptions import RefreshError
    from google_auth_oauthlib.flow import InstalledAppFlow

    leaked_detail = "refresh_token=secret-token&state=csrf-fragment"
    monkeypatch.setattr(
        InstalledAppFlow,
        "from_client_config",
        MagicMock(side_effect=RefreshError(leaked_detail)),
    )

    with pytest.raises(GSheetAuthError) as raised:
        GoogleOAuthClient(MagicMock(), _make_settings()).authorize()

    assert leaked_detail not in str(raised.value)
    assert (
        str(raised.value)
        == "OAuth authorization failed. See application logs for detail."
    )


def test_google_oauth_get_access_token_refreshes_when_expired_token_no_client_id() -> (
    None
):
    """Expired cache + missing client id surfaces a clear error, not a refresh attempt."""
    past = int(time.time()) - 3600
    store = _store_with({
        GSHEET_ACCESS_TOKEN_KEY: "stale-token",
        GSHEET_ACCESS_TOKEN_EXPIRES_KEY: str(past),
        GSHEET_REFRESH_TOKEN_KEY: "refresh-xyz",
    })
    client = GoogleOAuthClient(store, _make_settings(client_id=""))
    with pytest.raises(GSheetAuthError, match="client ID is not configured"):
        client.get_access_token()


def test_google_oauth_refresh_downgrade_clears_capability_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store_with({
        GSHEET_WRITE_REFRESH_TOKEN_KEY: "write-refresh",
        GSHEET_WRITE_GRANTED_SCOPES_KEY: GOOGLE_SHEETS_WRITE_SCOPE,
    })
    creds = MagicMock()
    creds.token = "downgraded-access"  # noqa: S105  # test credential
    creds.expiry = None
    creds.granted_scopes = [GOOGLE_SHEETS_READ_SCOPE]
    creds.scopes = [GOOGLE_SHEETS_WRITE_SCOPE]
    from google.oauth2 import credentials as credentials_module

    monkeypatch.setattr(
        credentials_module, "Credentials", MagicMock(return_value=creds)
    )
    client = GoogleOAuthClient(store, _make_settings())

    with pytest.raises(GSheetAuthError, match="required Google Sheets scope"):
        client.get_access_token(require_write=True)

    store.delete_key.assert_any_call(GSHEET_WRITE_ACCESS_TOKEN_KEY)
    store.delete_key.assert_any_call(GSHEET_WRITE_ACCESS_TOKEN_EXPIRES_KEY)
    store.set_key.assert_any_call(
        GSHEET_WRITE_GRANTED_SCOPES_KEY, GOOGLE_SHEETS_READ_SCOPE
    )
    assert not any(
        call.args == (GSHEET_WRITE_ACCESS_TOKEN_KEY, "downgraded-access")
        for call in store.set_key.call_args_list
    )


def test_google_oauth_authorize_sends_configured_client_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Desktop-app client's code->token exchange needs the client secret.

    Consent succeeds and Google returns an authorization code, then the
    exchange fails when client_secret is empty, so the configured value has to
    reach the client config the flow is built from.
    """
    creds = MagicMock(
        refresh_token="refresh-xyz",  # noqa: S106  # test credential
        token="access-xyz",  # noqa: S106  # test credential
        expiry=None,
        granted_scopes=[GOOGLE_SHEETS_READ_SCOPE],
        scopes=[GOOGLE_SHEETS_READ_SCOPE],
    )
    flow = MagicMock()
    flow.run_local_server.return_value = creds
    from google_auth_oauthlib.flow import InstalledAppFlow

    from_config = MagicMock(return_value=flow)
    monkeypatch.setattr(InstalledAppFlow, "from_client_config", from_config)
    expected = "fake-desktop-secret"
    settings = MoneyBinSettings.model_validate({
        "gsheet": {
            "oauth_client_id": "fake-client-id.apps.googleusercontent.com",
            "oauth_client_secret": expected,
        }
    })
    client = GoogleOAuthClient(_store_with({}), settings)

    client.authorize()

    client_config = from_config.call_args.args[0]
    assert client_config["installed"]["client_secret"] == expected


def test_google_oauth_authorize_refuses_before_browser_when_secret_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No secret means the exchange is doomed, so refuse before consent.

    Without this the user opens a browser, picks an account, grants access, and
    only then gets a generic failure for a cause no part of the flow named.
    """
    from google_auth_oauthlib.flow import InstalledAppFlow

    from_config = MagicMock()
    monkeypatch.setattr(InstalledAppFlow, "from_client_config", from_config)
    client = GoogleOAuthClient(_store_with({}), _make_settings(client_secret=None))

    with pytest.raises(GSheetAuthError, match="OAUTH_CLIENT_SECRET"):
        client.authorize()

    from_config.assert_not_called()


def test_google_oauth_refresh_sends_configured_client_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refresh grant needs the secret for the same reason the exchange does.

    google-auth puts client_secret into the refresh body unconditionally and
    urlencodes it, so None is transmitted as the literal string "None" and
    Google answers invalid_client. Authorizing would succeed and every pull
    after the access token's first hour would fail.
    """
    store = _store_with({
        GSHEET_REFRESH_TOKEN_KEY: "refresh-abc",
        GSHEET_GRANTED_SCOPES_KEY: GOOGLE_SHEETS_READ_SCOPE,
    })
    creds = MagicMock(
        token="access-xyz",  # noqa: S106  # test credential
        expiry=None,
        granted_scopes=[GOOGLE_SHEETS_READ_SCOPE],
        scopes=[GOOGLE_SHEETS_READ_SCOPE],
    )
    from google.oauth2 import credentials as credentials_module

    constructor = MagicMock(return_value=creds)
    monkeypatch.setattr(credentials_module, "Credentials", constructor)
    expected = "fake-desktop-secret"
    settings = MoneyBinSettings.model_validate({
        "gsheet": {
            "oauth_client_id": "fake-client-id.apps.googleusercontent.com",
            "oauth_client_secret": expected,
        }
    })
    client = GoogleOAuthClient(store, settings)

    client.get_access_token()

    assert constructor.call_args.kwargs["client_secret"] == expected
