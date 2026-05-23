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


def test_distinct_keys_per_profile() -> None:
    """Two profiles in the same process must receive distinct cached keys.

    Regression guard for the cross-profile cache-bleed defect where
    ``_CACHE_KEY = "current"`` returned the first-resolved profile's key
    for every subsequent call. Without per-profile keying, an HMAC-based
    redaction transform would emit identifiers that cross profile
    boundaries — a key-confusion defect.
    """
    key_a = bytes.fromhex("aa" * 32)
    key_b = bytes.fromhex("bb" * 32)

    # The SecretStore's per-profile vault returns whatever key belongs to
    # the active profile; emulate that by toggling get_key with the same
    # toggle the seeding module reads (get_current_profile).
    def _store_for_profile(profile: str) -> MagicMock:
        store = MagicMock()
        store.get_key.return_value = (key_a if profile == "alice" else key_b).hex()
        return store

    active_profile = {"name": "alice"}

    def _fake_get_current_profile() -> str:
        return active_profile["name"]

    def _fake_store_factory() -> MagicMock:
        return _store_for_profile(active_profile["name"])

    with (
        patch(
            "moneybin.config.get_current_profile", side_effect=_fake_get_current_profile
        ),
        patch("moneybin.privacy.seeding.SecretStore", side_effect=_fake_store_factory),
    ):
        alice_key = get_redaction_key()
        active_profile["name"] = "bob"
        bob_key = get_redaction_key()
        active_profile["name"] = "alice"
        alice_key_again = get_redaction_key()

    assert alice_key == key_a
    assert bob_key == key_b
    assert alice_key != bob_key
    assert alice_key_again == alice_key  # cache still holds alice's key after bob
