"""Unit tests for sync_* MCP tools (envelope shape + service delegation)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from moneybin.connectors.sync_models import (
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
