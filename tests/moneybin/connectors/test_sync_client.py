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
from moneybin.connectors.sync_errors import SyncAuthError, SyncConnectError
from moneybin.connectors.sync_models import SyncDataResponse


@pytest.fixture
def sync_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SyncClient:
    """A SyncClient pointed at a fake URL with file-based token storage in tmp.

    Uses the `_token_path` escape hatch so tests never touch the user's keyring.
    Clears proxy env vars so httpx doesn't try to load `socksio` in sandbox envs.
    """
    for var in (
        "ALL_PROXY",
        "all_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ):
        monkeypatch.delenv(var, raising=False)
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


@respx.mock
def test_authed_request_refreshes_on_401_then_retries(sync_client: SyncClient) -> None:
    sync_client._store_tokens(access_token="old-jwt", refresh_token="old-refresh")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential

    # First call: 401. Refresh succeeds with rotated tokens. Retry: 200.
    institutions_route = respx.get("https://test.api/institutions").mock(
        side_effect=[
            httpx.Response(401, json={"error": "Unauthorized"}),
            httpx.Response(200, json=[]),
        ]
    )
    refresh_route = respx.post("https://test.api/auth/refresh").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "new-jwt",  # noqa: S106  # test fixture, not a real credential
                "refresh_token": "new-refresh",  # noqa: S106  # test fixture, not a real credential
                "expires_in": 3600,
                "token_type": "Bearer",
            },
        )
    )

    result = sync_client.list_institutions()
    assert result == []
    assert sync_client._read_token() == "new-jwt"  # type: ignore[reportPrivateUsage]
    assert sync_client._read_refresh_token() == "new-refresh"  # type: ignore[reportPrivateUsage]
    assert institutions_route.call_count == 2
    assert refresh_route.call_count == 1


@respx.mock
def test_refresh_failure_clears_tokens_and_raises(sync_client: SyncClient) -> None:
    sync_client._store_tokens(access_token="old-jwt", refresh_token="expired-refresh")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential

    respx.get("https://test.api/institutions").mock(
        return_value=httpx.Response(401, json={"error": "Unauthorized"})
    )
    respx.post("https://test.api/auth/refresh").mock(
        return_value=httpx.Response(401, json={"error": "refresh token expired"})
    )

    with pytest.raises(SyncAuthError):
        sync_client.list_institutions()
    assert sync_client._read_token() is None  # type: ignore[reportPrivateUsage]
    assert sync_client._read_refresh_token() is None  # type: ignore[reportPrivateUsage]


@respx.mock
def test_401_after_successful_refresh_raises_auth_not_api(
    sync_client: SyncClient,
) -> None:
    """401-after-refresh must classify as auth failure, not generic API error.

    Token store drift / server-side revocation: refresh issues a new token,
    but the retry still 401s. Must surface as SyncAuthError (run sync login),
    not as a generic SyncAPIError.
    """
    sync_client._store_tokens(access_token="old-jwt", refresh_token="old-refresh")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential

    respx.get("https://test.api/institutions").mock(
        side_effect=[
            httpx.Response(401, json={"error": "Unauthorized"}),
            httpx.Response(401, json={"error": "still unauthorized"}),
        ]
    )
    respx.post("https://test.api/auth/refresh").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "new-jwt",
                "refresh_token": "new-refresh",
                "expires_in": 3600,
                "token_type": "Bearer",
            },
        )
    )

    with pytest.raises(SyncAuthError, match="session expired after refresh"):
        sync_client.list_institutions()
    assert sync_client._read_token() is None  # type: ignore[reportPrivateUsage]


@respx.mock
def test_initiate_connect_returns_session_and_url(sync_client: SyncClient) -> None:
    sync_client._store_tokens(access_token="jwt", refresh_token="r")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential
    respx.post("https://test.api/sync/connect/initiate").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": "sess_abc",
                "link_url": "https://hosted.plaid.com/link/xyz",
                "connect_type": "widget_flow",
                "expiration": "2026-05-13T13:30:00Z",
            },
        )
    )
    result = sync_client.initiate_connect()
    assert result.session_id == "sess_abc"
    assert result.connect_type == "widget_flow"


@respx.mock
def test_initiate_connect_passes_provider_item_id_for_update_mode(
    sync_client: SyncClient,
) -> None:
    sync_client._store_tokens(access_token="jwt", refresh_token="r")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential
    route = respx.post("https://test.api/sync/connect/initiate").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": "sess_abc",
                "link_url": "https://hosted.plaid.com/link/xyz",
                "connect_type": "widget_flow",
                "expiration": "2026-05-13T13:30:00Z",
            },
        )
    )
    sync_client.initiate_connect(provider_item_id="item_existing")
    sent_body = route.calls.last.request.content
    assert b"item_existing" in sent_body


@respx.mock
def test_poll_connect_until_connected(sync_client: SyncClient) -> None:
    sync_client._store_tokens(access_token="jwt", refresh_token="r")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential
    respx.get("https://test.api/sync/connect/status").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "session_id": "sess_abc",
                    "status": "pending",
                    "expiration": "2026-05-13T13:30:00Z",
                },
            ),
            httpx.Response(
                200,
                json={
                    "session_id": "sess_abc",
                    "status": "connected",
                    "provider_item_id": "item_new",
                    "institution_name": "Chase",
                    "expiration": "2026-05-13T13:30:00Z",
                },
            ),
        ]
    )
    sync_client._sleep = lambda _: None  # skip real sleep  # type: ignore[method-assign]
    result = sync_client.poll_connect_status("sess_abc")
    assert result.status == "connected"
    assert result.provider_item_id == "item_new"


@respx.mock
def test_poll_connect_failed_raises(sync_client: SyncClient) -> None:
    sync_client._store_tokens(access_token="jwt", refresh_token="r")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential
    respx.get("https://test.api/sync/connect/status").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": "sess_abc",
                "status": "failed",
                "error": "user cancelled flow",
                "expiration": "2026-05-13T13:30:00Z",
            },
        )
    )
    sync_client._sleep = lambda _: None  # type: ignore[method-assign]
    with pytest.raises(SyncConnectError, match="user cancelled"):
        sync_client.poll_connect_status("sess_abc")


@respx.mock
def test_trigger_sync_returns_synchronous_result(sync_client: SyncClient) -> None:
    sync_client._store_tokens(access_token="jwt", refresh_token="r")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential
    respx.post("https://test.api/sync/trigger").mock(
        return_value=httpx.Response(
            201,
            json={
                "job_id": "job-abc",
                "status": "completed",
                "transaction_count": 42,
            },
        )
    )
    result = sync_client.trigger_sync()
    assert result.job_id == "job-abc"
    assert result.status == "completed"
    assert result.transaction_count == 42


@respx.mock
def test_trigger_sync_passes_provider_item_id_and_force(
    sync_client: SyncClient,
) -> None:
    sync_client._store_tokens(access_token="jwt", refresh_token="r")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential
    route = respx.post("https://test.api/sync/trigger").mock(
        return_value=httpx.Response(201, json={"job_id": "j", "status": "completed"})
    )
    sync_client.trigger_sync(provider_item_id="item_x", reset_cursor=True)
    body = route.calls.last.request.content
    assert b"item_x" in body
    assert b'"reset_cursor":true' in body or b'"reset_cursor": true' in body


@respx.mock
def test_get_data_returns_parsed_sync_data(sync_client: SyncClient) -> None:
    sync_client._store_tokens(access_token="jwt", refresh_token="r")  # type: ignore[reportPrivateUsage]  # noqa: S106  # test fixture, not a real credential
    respx.get("https://test.api/sync/data").mock(
        return_value=httpx.Response(
            200,
            json={
                "accounts": [
                    {
                        "account_id": "a1",
                        "account_type": "depository",
                        "account_subtype": "checking",
                        "institution_name": "Chase",
                        "official_name": "Total",
                        "mask": "0001",
                    }
                ],
                "transactions": [
                    {
                        "transaction_id": "t1",
                        "account_id": "a1",
                        "transaction_date": "2026-04-07",
                        "amount": "10.00",
                        "description": "x",
                        "pending": False,
                    }
                ],
                "balances": [],
                "removed_transactions": [],
                "metadata": {
                    "job_id": "job-abc",
                    "synced_at": "2026-04-08T00:00:00Z",
                    "institutions": [
                        {
                            "provider_item_id": "item_x",
                            "status": "completed",
                            "transaction_count": 1,
                        }
                    ],
                },
            },
        )
    )
    result = sync_client.get_data("job-abc")
    assert isinstance(result, SyncDataResponse)
    assert result.metadata.job_id == "job-abc"
    assert len(result.transactions) == 1
