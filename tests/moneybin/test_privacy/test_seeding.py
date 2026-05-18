"""Profile-scoped HMAC key for deterministic redaction transforms."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from moneybin.privacy.seeding import (
    _CACHE,  # pyright: ignore[reportPrivateUsage]
    REDACTION_KEY_NAME,
    get_redaction_key,
)
from moneybin.secrets import SecretNotFoundError


@pytest.fixture(autouse=True)
def clear_cache() -> None:
    _CACHE.clear()


def test_returns_existing_key_from_secret_store() -> None:
    expected = bytes.fromhex("ab" * 32)  # 32-byte key, hex-encoded
    mock_store = MagicMock()
    mock_store.get_key.return_value = expected.hex()
    with patch("moneybin.privacy.seeding.SecretStore", return_value=mock_store):
        key = get_redaction_key()
    assert key == expected
    mock_store.get_key.assert_called_once_with(REDACTION_KEY_NAME)


def test_generates_and_stores_when_missing() -> None:
    mock_store = MagicMock()
    mock_store.get_key.side_effect = SecretNotFoundError("missing")
    with patch("moneybin.privacy.seeding.SecretStore", return_value=mock_store):
        key = get_redaction_key()
    assert len(key) == 32
    mock_store.set_key.assert_called_once()
    name, value = mock_store.set_key.call_args.args
    assert name == REDACTION_KEY_NAME
    assert bytes.fromhex(value) == key


def test_caches_after_first_fetch() -> None:
    expected = bytes.fromhex("cd" * 32)
    mock_store = MagicMock()
    mock_store.get_key.return_value = expected.hex()
    with patch("moneybin.privacy.seeding.SecretStore", return_value=mock_store):
        first = get_redaction_key()
        second = get_redaction_key()
    assert first == second
    assert mock_store.get_key.call_count == 1
