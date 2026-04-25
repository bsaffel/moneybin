"""Tests for SecretStore — centralized secret management."""

from unittest.mock import patch

import pytest

from moneybin.secrets import SecretNotFoundError, SecretStore


class TestGetKey:
    """SecretStore.get_key() — keychain → env var → error."""

    def test_returns_key_from_keychain(self) -> None:
        """Keychain contains the secret — returns it directly."""
        store = SecretStore(profile="alice")
        with patch("moneybin.secrets.keyring") as mock_kr:
            mock_kr.get_password.return_value = "secret-from-keychain"
            result = store.get_key("DATABASE__ENCRYPTION_KEY")

        assert result == "secret-from-keychain"
        mock_kr.get_password.assert_called_once_with(
            "moneybin-alice", "DATABASE__ENCRYPTION_KEY"
        )

    def test_falls_back_to_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keychain miss + env var set — returns env var value."""
        monkeypatch.setenv("MONEYBIN_DATABASE__ENCRYPTION_KEY", "secret-from-env")
        store = SecretStore()
        with patch("moneybin.secrets.keyring") as mock_kr:
            mock_kr.get_password.return_value = None
            result = store.get_key("DATABASE__ENCRYPTION_KEY")

        assert result == "secret-from-env"

    def test_raises_when_both_miss(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Both keychain and env var miss — raises SecretNotFoundError."""
        monkeypatch.delenv("MONEYBIN_DATABASE__ENCRYPTION_KEY", raising=False)
        store = SecretStore()
        with patch("moneybin.secrets.keyring") as mock_kr:
            mock_kr.get_password.return_value = None
            with pytest.raises(SecretNotFoundError, match="DATABASE__ENCRYPTION_KEY"):
                store.get_key("DATABASE__ENCRYPTION_KEY")


class TestGetEnv:
    """SecretStore.get_env() — env var only, no keychain."""

    def test_returns_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MONEYBIN_SYNC__API_KEY", "api-key-123")
        store = SecretStore()
        assert store.get_env("SYNC__API_KEY") == "api-key-123"

    def test_raises_when_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MONEYBIN_SYNC__API_KEY", raising=False)
        store = SecretStore()
        with pytest.raises(SecretNotFoundError, match="SYNC__API_KEY"):
            store.get_env("SYNC__API_KEY")


class TestSetAndDeleteKey:
    """SecretStore.set_key() and delete_key() — keychain writes."""

    def test_set_key_writes_to_keychain(self) -> None:
        store = SecretStore(profile="alice")
        with patch("moneybin.secrets.keyring") as mock_kr:
            store.set_key("DATABASE__ENCRYPTION_KEY", "new-key-value")

        mock_kr.set_password.assert_called_once_with(
            "moneybin-alice", "DATABASE__ENCRYPTION_KEY", "new-key-value"
        )

    def test_delete_key_clears_from_keychain(self) -> None:
        store = SecretStore(profile="alice")
        with patch("moneybin.secrets.keyring") as mock_kr:
            store.delete_key("DATABASE__ENCRYPTION_KEY")

        mock_kr.delete_password.assert_called_once_with(
            "moneybin-alice", "DATABASE__ENCRYPTION_KEY"
        )

    def test_two_profiles_use_distinct_service_names(self) -> None:
        """Different profiles must hit different keychain services."""
        alice = SecretStore(profile="alice")
        bob = SecretStore(profile="bob")
        with patch("moneybin.secrets.keyring") as mock_kr:
            alice.set_key("DATABASE__ENCRYPTION_KEY", "alice-key")
            bob.set_key("DATABASE__ENCRYPTION_KEY", "bob-key")

        services = {call.args[0] for call in mock_kr.set_password.call_args_list}
        assert services == {"moneybin-alice", "moneybin-bob"}

    def test_delete_key_raises_secret_not_found_when_absent(self) -> None:
        """PasswordDeleteError from keyring backend is wrapped as SecretNotFoundError."""
        store = SecretStore(profile="alice")
        with patch("moneybin.secrets.keyring") as mock_kr:
            mock_kr.errors.PasswordDeleteError = type(
                "PasswordDeleteError", (Exception,), {}
            )
            mock_kr.delete_password.side_effect = mock_kr.errors.PasswordDeleteError(
                "not found"
            )
            with pytest.raises(SecretNotFoundError, match="not found in keychain"):
                store.delete_key("DATABASE__ENCRYPTION_KEY")
