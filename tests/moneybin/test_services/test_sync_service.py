"""Unit tests for SyncService — business logic orchestrating client + loader."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    SyncDataResponse,
    SyncTriggerResponse,
)
from moneybin.database import Database
from moneybin.loaders.plaid_loader import PlaidLoader
from moneybin.services.sync_service import SyncService

FIXTURE = (
    Path(__file__).parent.parent
    / "test_loaders"
    / "fixtures"
    / "plaid_sync_response.yaml"
)


@pytest.fixture
def sync_data() -> SyncDataResponse:
    with FIXTURE.open() as f:
        return SyncDataResponse.model_validate(yaml.safe_load(f))


@pytest.fixture
def loader(db: Database) -> PlaidLoader:
    return PlaidLoader(db)


@pytest.fixture
def mock_client(sync_data: SyncDataResponse) -> MagicMock:
    client = MagicMock()
    client.trigger_sync.return_value = SyncTriggerResponse(
        job_id=sync_data.metadata.job_id,
        status="completed",
        transaction_count=3,
    )
    client.get_data.return_value = sync_data
    return client


def test_pull_happy_path(
    mock_client: MagicMock,
    db: Database,
    loader: PlaidLoader,
    sync_data: SyncDataResponse,
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    result = service.pull()

    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id=None,
        reset_cursor=False,
    )
    mock_client.get_data.assert_called_once_with(sync_data.metadata.job_id)
    assert result.transactions_loaded == 3
    assert result.accounts_loaded == 2
    assert result.balances_loaded == 2
    assert result.transactions_removed == 1
    assert result.institutions[0].provider_item_id == "item_chase_abc"


def test_pull_with_institution_resolves_to_provider_item_id(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    mock_client.list_institutions.return_value = [
        ConnectedInstitution(
            id="u1",
            provider_item_id="item_chase_abc",
            provider="plaid",
            institution_name="Chase",
            status="active",
            created_at=datetime(2026, 3, 15, tzinfo=UTC),
        ),
    ]
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.pull(institution="Chase")
    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id="item_chase_abc",
        reset_cursor=False,
    )


def test_pull_with_unknown_institution_raises(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    mock_client.list_institutions.return_value = []
    service = SyncService(client=mock_client, db=db, loader=loader)
    with pytest.raises(ValueError, match="no connected institution"):
        service.pull(institution="UnknownBank")


def test_pull_with_force_passes_reset_cursor(
    mock_client: MagicMock, db: Database, loader: PlaidLoader
) -> None:
    service = SyncService(client=mock_client, db=db, loader=loader)
    service.pull(force=True)
    mock_client.trigger_sync.assert_called_once_with(
        provider_item_id=None,
        reset_cursor=True,
    )
