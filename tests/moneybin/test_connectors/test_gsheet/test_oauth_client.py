"""Tests for GoogleOAuthClient + TestOAuthClient stub."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from moneybin.config import MoneyBinSettings
from moneybin.connectors.gsheet.errors import GSheetAuthError
from moneybin.connectors.gsheet.oauth_client import GoogleOAuthClient
from moneybin.connectors.gsheet.sheets_api import OAuthCredentialsProvider
from moneybin.connectors.gsheet.testing.fake_oauth_client import TestOAuthClient
from moneybin.secrets import (
    GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
    GSHEET_ACCESS_TOKEN_KEY,
    GSHEET_REFRESH_TOKEN_KEY,
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


def test_fake_oauth_implements_oauth_credentials_provider_protocol() -> None:
    """TestOAuthClient must structurally satisfy OAuthCredentialsProvider."""
    client: OAuthCredentialsProvider = TestOAuthClient()
    assert callable(client.get_access_token)


# -- GoogleOAuthClient --------------------------------------------------------


def _make_settings(
    client_id: str = "fake-client-id.apps.googleusercontent.com",
) -> MoneyBinSettings:
    """Build a settings instance with a configured gsheet client id."""
    return MoneyBinSettings.model_validate({"gsheet": {"oauth_client_id": client_id}})


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


def test_google_oauth_revoke_deletes_all_three_keys() -> None:
    store = MagicMock()
    client = GoogleOAuthClient(store, _make_settings())

    client.revoke()

    deleted = [call.args[0] for call in store.delete_key.call_args_list]
    assert set(deleted) == {
        GSHEET_REFRESH_TOKEN_KEY,
        GSHEET_ACCESS_TOKEN_KEY,
        GSHEET_ACCESS_TOKEN_EXPIRES_KEY,
    }
    assert store.delete_key.call_count == 3


def test_google_oauth_revoke_survives_missing_keys() -> None:
    store = MagicMock()
    store.delete_key.side_effect = SecretNotFoundError("missing")
    client = GoogleOAuthClient(store, _make_settings())
    # Should not raise even when every key is already gone.
    client.revoke()
    assert store.delete_key.call_count == 3


def test_google_oauth_authorize_raises_when_client_id_empty() -> None:
    store = MagicMock()
    client = GoogleOAuthClient(store, _make_settings(client_id=""))
    with pytest.raises(GSheetAuthError, match="client ID is not configured"):
        client.authorize()
    # Must fail before touching the secret store.
    store.set_key.assert_not_called()


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
