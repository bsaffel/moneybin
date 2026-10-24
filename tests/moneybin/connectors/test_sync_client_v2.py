"""Version negotiation and hidden profile identity on the broker client boundary."""

import json
from pathlib import Path
from typing import cast
from uuid import UUID

import httpx
import pytest
import respx

from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_errors import SyncAPIError
from tests.moneybin.connectors.test_sync_client import FakeSecretStore


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SyncClient:
    for name in (
        "ALL_PROXY",
        "all_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    result = SyncClient(
        server_url="https://test.api",
        profile_id="synthetic-profile",
        secret_store=FakeSecretStore(),
    )
    result._store_tokens(  # pyright: ignore[reportPrivateUsage]  # synthetic token fixture
        access_token="synthetic-jwt",  # noqa: S106  # synthetic fixture credentials
        refresh_token="synthetic-refresh",  # noqa: S106  # synthetic fixture credentials
    )
    return result


@respx.mock
@pytest.mark.parametrize("version", [None, 1])
def test_old_broker_requires_upgrade(client: SyncClient, version: int | None) -> None:
    """Missing/wrong envelope negotiation must fail before downloading or loading."""
    response: dict[str, object] = {
        "job_id": str(UUID(int=800)),
        "status": "completed",
        "transaction_count": 0,
    }
    if version is not None:
        response["contract_version"] = version
    respx.post("https://test.api/sync/trigger").respond(201, json=response)
    with pytest.raises(SyncAPIError, match="upgrade"):
        client.trigger_sync()


@respx.mock
def test_v2_selectors_and_profile_are_sent_without_human_identity(
    client: SyncClient,
) -> None:
    """Explicit connection/history selectors preserve the internally owned consumer."""
    from datetime import date

    route = respx.post("https://test.api/sync/trigger").respond(
        201,
        json={
            "job_id": str(UUID(int=800)),
            "contract_version": 2,
            "status": "partial",
            "transaction_count": 1,
        },
    )
    result = client.trigger_sync(
        provider="simplefin",
        connection_id=str(UUID(int=100)),
        history_start=date(2026, 1, 1),
    )
    assert result.status == "partial"
    body = json.loads(cast(httpx.Request, route.calls[0].request).content)  # pyright: ignore[reportUnknownMemberType]  # respx call typing is incomplete
    assert body == {
        "contract_version": 2,
        "consumer_id": "synthetic-profile",
        "provider": "simplefin",
        "connection_id": str(UUID(int=100)),
        "history_start": "2026-01-01",
    }


@respx.mock
def test_get_data_rejects_wrong_job_identity(client: SyncClient) -> None:
    """A valid envelope for another job cannot authorize this job's acknowledgement."""
    respx.get("https://test.api/sync/data").respond(
        200,
        json={
            "job_id": str(UUID(int=801)),
            "contract_version": 2,
            "status": "completed",
            "sources": [],
        },
    )
    with pytest.raises(SyncAPIError):
        client.get_data(str(UUID(int=800)))
