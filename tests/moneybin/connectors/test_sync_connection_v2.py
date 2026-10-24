"""V2 current connections retain typed history and hidden consumer identity."""

import json
from typing import cast
from uuid import UUID

import httpx
import pytest
import respx

from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_errors import SyncAPIError
from tests.moneybin.connectors import test_sync_client_v2 as synthetic_client

client = synthetic_client.client


@respx.mock
def test_v2_listing_parses_nullable_simplefin_item_and_history(
    client: SyncClient,
) -> None:
    route = respx.get("https://test.api/institutions").respond(
        200,
        json=[
            {
                "id": str(UUID(int=100)),
                "provider": "simplefin",
                "provider_item_id": None,
                "institution_name": None,
                "created_at": "2026-01-01T00:00:00Z",
                "last_sync": None,
                "status": "active",
                "credential_generation": 2,
                "accounts": [],
                "protocol_version": "2.0.0-draft",
                "history": {
                    "observed_at": "2026-10-01T00:00:00Z",
                    "replay_required": True,
                    "discovery_history_target": "2026-07-01",
                    "accounts": [],
                },
                "history_unavailable_reason": None,
                "error_code": None,
            }
        ],
    )
    row = client.list_institutions()[0]
    assert row.provider_item_id is None
    assert row.credential_generation == 2
    assert row.history is not None and row.history.replay_required
    query = route.calls[0].request.url.params  # pyright: ignore[reportUnknownMemberType]  # respx request typing is incomplete
    assert query["contract_version"] == "2"
    assert query["consumer_id"] == "synthetic-profile"


@respx.mock
@pytest.mark.parametrize("provider", ["plaid", "simplefin"])
def test_link_transport_uses_admitted_provider_specific_contract(
    client: SyncClient, provider: str
) -> None:
    route = respx.post("https://test.api/sync/link/initiate").respond(
        200,
        json={
            "session_id": str(UUID(int=500)),
            "link_url": "https://test.api/browser-link",
            "link_type": "widget_flow",
            "expiration": "2026-10-01T12:00:00Z",
        },
    )
    if provider == "simplefin":
        client.initiate_link(provider=provider, connection_id=str(UUID(int=100)))
        expected = {
            "provider": provider,
            "connection_id": str(UUID(int=100)),
            "consumer_id": "synthetic-profile",
        }
    else:
        client.initiate_link(provider=provider, provider_item_id="synthetic-item")
        expected = {"provider": provider, "provider_item_id": "synthetic-item"}
    assert json.loads(route.calls[0].request.content) == expected  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]  # respx request typing is incomplete


@respx.mock
def test_disconnect_consumes_grant_revocation_result(client: SyncClient) -> None:
    connection = str(UUID(int=100))
    route = respx.delete(f"https://test.api/connections/{connection}").respond(
        200,
        json={
            "connection_id": connection,
            "credential_generation": 2,
            "accounts": [],
            "remote_revocation": "user_action_required",
        },
    )
    response = client.disconnect(connection, credential_generation=2, accounts=[])
    assert response.connection_id == UUID(connection)
    assert response.remote_revocation == "user_action_required"
    assert route.calls[0].request.url.params["consumer_id"] == "synthetic-profile"  # pyright: ignore[reportUnknownMemberType]  # respx runtime request

    request = cast(httpx.Request, route.calls[0].request)  # pyright: ignore[reportUnknownMemberType]  # respx call typing is incomplete
    assert json.loads(request.content) == {
        "credential_generation": 2,
        "accounts": [],
    }
    assert response.credential_generation == 2


@respx.mock
def test_disconnect_changed_confirmation_is_safe_and_never_retried(
    client: SyncClient,
) -> None:
    connection = str(UUID(int=100))
    route = respx.delete(f"https://test.api/connections/{connection}").respond(
        409, json={"detail": "private native-account-123456"}
    )
    with pytest.raises(SyncAPIError) as error:
        client.disconnect(connection, credential_generation=2, accounts=[])
    assert "reconfirm" in str(error.value).lower()
    assert "private" not in str(error.value) and "native-account" not in str(
        error.value
    )
    assert route.call_count == 1


@respx.mock
@pytest.mark.parametrize("mismatch", ["generation", "accounts"])
def test_disconnect_rejects_mismatched_removed_grant_response(
    client: SyncClient, mismatch: str
) -> None:
    connection = str(UUID(int=100))
    response = {
        "connection_id": connection,
        "credential_generation": 3 if mismatch == "generation" else 2,
        "accounts": [
            {
                "account_id": "synthetic-new",
                "native_scope": {"kind": "simplefin_v2_connection", "conn_id": "scope"},
                "name": "Synthetic",
            }
        ]
        if mismatch == "accounts"
        else [],
        "remote_revocation": "user_action_required",
    }
    respx.delete(f"https://test.api/connections/{connection}").respond(
        200, json=response
    )
    with pytest.raises(SyncAPIError, match="Invalid disconnect response"):
        client.disconnect(connection, credential_generation=2, accounts=[])
