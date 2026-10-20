"""End-to-end smoke: mocked SyncClient → SyncService → PlaidExtractor → raw → SQLMesh → core."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, uuid5

import pytest
import yaml

from moneybin.connectors.sync_models import (
    PlaidSyncDataResponse,
    SyncAckV2Response,
    SyncEnvelopeV2,
    SyncStatusV2Response,
    SyncTriggerV2Response,
)
from moneybin.database import Database, sqlmesh_context
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.services.sync_service import SyncService

pytestmark = pytest.mark.integration

FIXTURE = (
    Path(__file__).parent / "test_extractors" / "fixtures" / "plaid_sync_response.yaml"
)


@pytest.mark.slow
def test_full_sync_pipeline_to_core(db: Database) -> None:
    """Sync data → raw → staging → core, with sign flip verified at core."""
    with FIXTURE.open() as f:
        sync_data = PlaidSyncDataResponse.model_validate(yaml.safe_load(f))

    client = MagicMock()
    client.consumer_id = "test-consumer"
    job_id = uuid5(NAMESPACE_URL, f"sync-job:{sync_data.metadata.job_id}")
    payload = sync_data.model_dump(mode="json")
    payload["metadata"]["job_id"] = str(job_id)
    source = {
        "provider": "plaid",
        "connection_id": uuid5(NAMESPACE_URL, "sync-connection:e2e"),
        "credential_generation": 1,
        "batches": [
            {
                "batch_id": uuid5(NAMESPACE_URL, "sync-batch:e2e"),
                "fetched_at": datetime(2026, 4, 8, tzinfo=UTC),
                "window": None,
                "payload": payload,
            }
        ],
        "outcomes": [
            {
                "account_id": None,
                "native_scope": {
                    "kind": "plaid_item",
                    "provider_item_id": item.provider_item_id,
                },
                "window": None,
                "status": "complete",
                "error_code": None,
            }
            for item in sync_data.metadata.institutions
        ],
    }
    envelope = SyncEnvelopeV2.model_validate({
        "job_id": job_id,
        "contract_version": 2,
        "status": "completed",
        "sources": [source],
    })
    results = [item.model_dump(exclude={"batches"}) for item in envelope.sources]
    client.trigger_sync.return_value = SyncTriggerV2Response(
        job_id=job_id,
        contract_version=2,
        status="completed",
        transaction_count=len(sync_data.transactions),
    )
    client.get_data.return_value = envelope
    client.get_status.return_value = SyncStatusV2Response.model_validate({
        "job_id": job_id,
        "contract_version": 2,
        "status": "completed",
        "transaction_count": len(sync_data.transactions),
        "error": None,
        "started_at": datetime(2026, 4, 8, tzinfo=UTC),
        "completed_at": datetime(2026, 4, 8, 0, 1, tzinfo=UTC),
        "acked_at": None,
        "results": results,
    })
    client.ack.return_value = SyncAckV2Response.model_validate({
        "job_id": job_id,
        "contract_version": 2,
        "status": "acked",
        "acked_at": datetime(2026, 4, 8, 0, 2, tzinfo=UTC),
        "results": results,
    })
    client.list_institutions.return_value = []

    loader = PlaidExtractor(db)
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

    # Tier-1 extended fields flow from Plaid all the way to core.fct_transactions:
    # original_description is a new column (raw bank text); the rest fill columns
    # the Plaid CTE previously hardcoded NULL/'USD'.
    enriched = db.execute(
        """
        SELECT original_description, currency_code, authorized_date,
               payment_channel, location_city
        FROM core.fct_transactions
        WHERE source_type = 'plaid' AND description LIKE '%STARBUCKS%'
        """
    ).fetchone()
    assert enriched is not None
    assert enriched[0] == "SQ *STARBUCKS 1234 SEATTLE WA"
    assert enriched[1] == "USD"
    assert str(enriched[2]) == "2026-04-06"
    assert enriched[3] == "in store"
    assert enriched[4] == "Seattle"

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
