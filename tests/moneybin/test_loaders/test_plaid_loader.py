"""Unit tests for PlaidLoader."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.database import Database
from moneybin.loaders.plaid_loader import PlaidLoader

FIXTURE = Path(__file__).parent / "fixtures" / "plaid_sync_response.yaml"


@pytest.fixture
def sync_data() -> SyncDataResponse:
    with FIXTURE.open() as f:
        payload = yaml.safe_load(f)
    return SyncDataResponse.model_validate(payload)


def test_loader_writes_accounts(db: Database, sync_data: SyncDataResponse) -> None:
    loader = PlaidLoader(db)
    result = loader.load(sync_data, job_id=sync_data.metadata.job_id)
    assert result.accounts == 2

    rows = db.execute(
        """
        SELECT account_id, institution_name, mask, source_file, source_type, source_origin
        FROM raw.plaid_accounts ORDER BY account_id
        """
    ).fetchall()
    assert len(rows) == 2
    assert rows[0] == (
        "acc_chase_check",
        "Chase",
        "1234",
        "sync_550e8400-e29b-41d4-a716-446655440000",
        "plaid",
        "item_chase_abc",
    )
