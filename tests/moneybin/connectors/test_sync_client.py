"""Unit tests for SyncClient."""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.connectors.sync_client import (
    _DEFAULT_TIMEOUT,  # type: ignore[reportPrivateUsage]
    _LONG_TIMEOUT,  # type: ignore[reportPrivateUsage]
    SyncClient,
)


@pytest.fixture
def sync_client(tmp_path: Path) -> SyncClient:
    """A SyncClient pointed at a fake URL with file-based token storage in tmp.

    Uses the `_token_path` escape hatch so tests never touch the user's keyring.
    """
    token_path = tmp_path / ".sync_token"
    return SyncClient(server_url="https://test.api", token_path=token_path)


def test_sync_client_initial_state_has_no_token(sync_client: SyncClient) -> None:
    assert sync_client._read_token() is None  # type: ignore[reportPrivateUsage]


def test_sync_client_store_and_read_token(sync_client: SyncClient) -> None:
    sync_client._store_tokens(access_token="jwt-1", refresh_token="ref-1")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential
    assert sync_client._read_token() == "jwt-1"  # type: ignore[reportPrivateUsage]
    assert sync_client._read_refresh_token() == "ref-1"  # type: ignore[reportPrivateUsage]


def test_sync_client_clear_tokens(sync_client: SyncClient) -> None:
    sync_client._store_tokens(access_token="jwt-1", refresh_token="ref-1")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential
    sync_client._clear_tokens()  # type: ignore[reportPrivateUsage]
    assert sync_client._read_token() is None  # type: ignore[reportPrivateUsage]
    assert sync_client._read_refresh_token() is None  # type: ignore[reportPrivateUsage]


def test_timeout_constants() -> None:
    """Two timeout constants — default and long — per design decision (no per-endpoint config knobs)."""
    assert _DEFAULT_TIMEOUT.read == 15.0  # type: ignore[reportPrivateUsage]
    assert _LONG_TIMEOUT.read == 120.0  # type: ignore[reportPrivateUsage]
