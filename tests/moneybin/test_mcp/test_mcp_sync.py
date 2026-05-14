"""Unit tests for sync_* MCP tools (envelope shape + service delegation)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from moneybin.connectors.sync_models import (
    ConnectInitiateResponse,
    InstitutionResult,
    PullResult,
    SyncConnectionView,
)


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_pull_returns_envelope_with_summary(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.pull.return_value = PullResult(
        job_id="j1",
        transactions_loaded=5,
        accounts_loaded=1,
        balances_loaded=1,
        transactions_removed=0,
        institutions=[
            InstitutionResult(
                provider_item_id="item_a",
                institution_name="Chase",
                status="completed",
                transaction_count=5,
            ),
        ],
    )
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_pull

    envelope = await sync_pull()
    assert envelope.summary.sensitivity == "medium"
    assert envelope.data["transactions_loaded"] == 5
    service.pull.assert_called_once()


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_status_returns_low_sensitivity(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="active",
            last_sync=datetime(2026, 4, 7, tzinfo=UTC),
            guidance=None,
        ),
    ]
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_status

    envelope = await sync_status()
    assert envelope.summary.sensitivity == "low"
    assert envelope.data[0]["institution_name"] == "Chase"


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_client")
async def test_sync_connect_returns_link_url_with_medium_sensitivity(
    mock_client_builder: MagicMock,
) -> None:
    client = MagicMock()
    client.initiate_connect.return_value = ConnectInitiateResponse(
        session_id="sess_abc",
        link_url="https://hosted.plaid.com/link/xyz",
        connect_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    mock_client_builder.return_value = client
    from moneybin.mcp.tools.sync import sync_connect

    envelope = await sync_connect()
    # link_url is a one-time bearer credential → medium sensitivity per design
    assert envelope.summary.sensitivity == "medium"
    assert envelope.data["session_id"] == "sess_abc"
    assert envelope.data["link_url"].startswith("https://hosted.plaid.com")
    # Agent should know about expiration to decide when to give up polling
    assert "expiration" in envelope.data


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_client")
async def test_sync_connect_status_pending(mock_client_builder: MagicMock) -> None:
    client = MagicMock()
    # sync_connect_status calls _authed_request directly (single-shot, no poll loop)
    resp_mock = MagicMock()
    resp_mock.json.return_value = {
        "session_id": "sess_abc",
        "status": "pending",
        "expiration": "2026-05-13T13:30:00Z",
    }
    client._authed_request.return_value = resp_mock
    mock_client_builder.return_value = client
    from moneybin.mcp.tools.sync import sync_connect_status

    envelope = await sync_connect_status(session_id="sess_abc")
    assert envelope.data["status"] == "pending"
    assert "expiration" in envelope.data


@pytest.mark.unit
@patch("moneybin.mcp.tools.sync._build_sync_service")
async def test_sync_disconnect_calls_service(mock_build: MagicMock) -> None:
    service = MagicMock()
    mock_build.return_value.__enter__.return_value = service
    from moneybin.mcp.tools.sync import sync_disconnect

    envelope = await sync_disconnect(institution="Chase")
    service.disconnect.assert_called_once_with(institution="Chase")
    assert envelope.summary.sensitivity == "medium"
