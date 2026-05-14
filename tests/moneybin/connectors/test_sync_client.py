"""Unit tests for SyncClient."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from moneybin.connectors.sync_client import (
    _DEFAULT_TIMEOUT,  # type: ignore[reportPrivateUsage]
    _LONG_TIMEOUT,  # type: ignore[reportPrivateUsage]
    SyncClient,
)
from moneybin.connectors.sync_errors import SyncAuthError


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


@respx.mock
def test_login_happy_path(sync_client: SyncClient) -> None:
    respx.post("https://test.api/auth/device/code").mock(
        return_value=httpx.Response(
            200,
            json={
                "device_code": "Ag_EE...",
                "user_code": "ABCD-EFGH",
                "verification_uri": "https://tenant.auth0.com/activate",
                "verification_uri_complete": "https://tenant.auth0.com/activate?user_code=ABCD-EFGH",
                "expires_in": 900,
                "interval": 0,  # 0 so test doesn't actually sleep
            },
        )
    )
    respx.post("https://test.api/auth/device/token").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "eyJ-jwt",  # noqa: S106  # test fixture, not a real credential
                "refresh_token": "v1.refresh",  # noqa: S106  # test fixture, not a real credential
                "expires_in": 3600,
                "token_type": "Bearer",
                "id_token": "eyJ-id",
            },
        )
    )
    sync_client.login(open_browser=False)
    assert sync_client._read_token() == "eyJ-jwt"  # type: ignore[reportPrivateUsage]
    assert sync_client._read_refresh_token() == "v1.refresh"  # type: ignore[reportPrivateUsage]


@respx.mock
def test_login_pending_then_success(sync_client: SyncClient) -> None:
    respx.post("https://test.api/auth/device/code").mock(
        return_value=httpx.Response(
            200,
            json={
                "device_code": "dc-1",
                "user_code": "ABCD-EFGH",
                "verification_uri": "https://x",
                "verification_uri_complete": "https://x",
                "expires_in": 900,
                "interval": 0,
            },
        )
    )
    respx.post("https://test.api/auth/device/token").mock(
        side_effect=[
            httpx.Response(202, json={"status": "pending"}),
            httpx.Response(202, json={"status": "pending"}),
            httpx.Response(
                200,
                json={
                    "access_token": "eyJ-jwt",  # noqa: S106  # test fixture, not a real credential
                    "refresh_token": "v1.refresh",  # noqa: S106  # test fixture, not a real credential
                    "expires_in": 3600,
                    "token_type": "Bearer",
                    "id_token": "eyJ-id",
                },
            ),
        ]
    )
    sync_client.login(open_browser=False)
    assert sync_client._read_token() == "eyJ-jwt"  # type: ignore[reportPrivateUsage]


@respx.mock
def test_login_slow_down_increases_interval(sync_client: SyncClient) -> None:
    respx.post("https://test.api/auth/device/code").mock(
        return_value=httpx.Response(
            200,
            json={
                "device_code": "dc-1",
                "user_code": "ABCD-EFGH",
                "verification_uri": "https://x",
                "verification_uri_complete": "https://x",
                "expires_in": 900,
                "interval": 0,
            },
        )
    )
    respx.post("https://test.api/auth/device/token").mock(
        side_effect=[
            httpx.Response(202, json={"status": "slow_down"}),
            httpx.Response(
                200,
                json={
                    "access_token": "eyJ-jwt",  # noqa: S106  # test fixture, not a real credential
                    "refresh_token": "v1.refresh",  # noqa: S106  # test fixture, not a real credential
                    "expires_in": 3600,
                    "token_type": "Bearer",
                    "id_token": "eyJ-id",
                },
            ),
        ]
    )
    # Capture sleep duration to verify slow_down triggers interval bump
    sleeps: list[float] = []
    sync_client._sleep = sleeps.append  # type: ignore[method-assign]  # test hook
    sync_client.login(open_browser=False)
    # Expect: first poll sees slow_down (interval was 0, bumped to 5); second succeeds.
    assert sleeps[-1] >= 5.0


@respx.mock
def test_login_user_denied_raises(sync_client: SyncClient) -> None:
    respx.post("https://test.api/auth/device/code").mock(
        return_value=httpx.Response(
            200,
            json={
                "device_code": "dc-1",
                "user_code": "ABCD-EFGH",
                "verification_uri": "https://x",
                "verification_uri_complete": "https://x",
                "expires_in": 900,
                "interval": 0,
            },
        )
    )
    respx.post("https://test.api/auth/device/token").mock(
        return_value=httpx.Response(403, json={"error": "access_denied"})
    )
    with pytest.raises(SyncAuthError):
        sync_client.login(open_browser=False)
