"""End-to-end smoke: mocked SyncClient → SyncService → PlaidLoader → raw → SQLMesh → core."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from moneybin.connectors.sync_models import SyncDataResponse, SyncTriggerResponse
from moneybin.database import Database, sqlmesh_context
from moneybin.loaders.plaid_loader import PlaidLoader
from moneybin.services.sync_service import SyncService

FIXTURE = Path(__file__).parent / "test_loaders" / "fixtures" / "plaid_sync_response.yaml"


@pytest.mark.slow
def test_full_sync_pipeline_to_core(db: Database) -> None:
    """Sync data → raw → staging → core, with sign flip verified at core."""
    with FIXTURE.open() as f:
        sync_data = SyncDataResponse.model_validate(yaml.safe_load(f))

    client = MagicMock()
    client.trigger_sync.return_value = SyncTriggerResponse(
        job_id=sync_data.metadata.job_id,
        status="completed",
        transaction_count=len(sync_data.transactions),
    )
    client.get_data.return_value = sync_data
    client.list_institutions.return_value = []

    loader = PlaidLoader(db)
    service = SyncService(client=client, db=db, loader=loader)

    result = service.pull()
    assert result.transactions_loaded == 3
    assert result.accounts_loaded == 2

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT amount FROM core.fct_transactions
        WHERE source_type = 'plaid' AND description LIKE '%STARBUCKS%'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == Decimal("-42.50")

    row = db.execute(
        """
        SELECT amount FROM core.fct_transactions
        WHERE source_type = 'plaid' AND description LIKE '%PAYROLL%'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == Decimal("1500.00")

    row = db.execute(
        "SELECT COUNT(*) FROM raw.plaid_transactions WHERE transaction_id = 'txn_removed_old'"
    ).fetchone()
    assert row is not None
    assert row[0] == 0
