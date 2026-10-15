"""Regression scenario: Plaid sync auto-refreshes core.dim_accounts.

Mirrors test_scenario_import_dim_freshness.py for the sync path. Guards the
contract from smart-import-transform.md (extended to sync in this change):
after SyncService.pull() succeeds with refresh=True (default),
core.dim_accounts must reflect every account from the sync payload, with
``updated_at`` populated. Without the end-of-pull transform call,
dim_accounts stays stale until the next import or manual transform_apply —
the headline staleness bug on the Plaid path.
"""

from __future__ import annotations

from datetime import UTC, datetime
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
from moneybin.database import Database
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.services.sync_service import SyncService
from moneybin.services.system_service import SystemService

FIXTURE = (
    Path(__file__).parent.parent
    / "moneybin"
    / "test_extractors"
    / "fixtures"
    / "plaid_sync_response.yaml"
)
_KEY = "scenario-sync-freshness-key-0123456789ab"


def _secret_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _KEY
    return store


def _build_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    db_path = tmp_path / "sync_freshness.duckdb"
    db = Database(db_path, secret_store=_secret_store(), read_only=False)
    settings = MagicMock()
    settings.database.path = db_path
    monkeypatch.setattr("moneybin.database.get_settings", lambda: settings)
    return db


def _set_v2_job(client: MagicMock, native: PlaidSyncDataResponse) -> None:
    """Serve the native Plaid fixture through one accepted broker v2 job."""
    client.consumer_id = "test-consumer"
    job_id = uuid5(NAMESPACE_URL, f"sync-freshness-job:{native.metadata.job_id}")
    source = {
        "provider": "plaid",
        "connection_id": uuid5(NAMESPACE_URL, "sync-freshness-connection"),
        "credential_generation": 1,
        "batches": [
            {
                "batch_id": uuid5(NAMESPACE_URL, "sync-freshness-batch"),
                "fetched_at": datetime(2026, 4, 8, tzinfo=UTC),
                "window": None,
                "payload": native.model_dump(mode="json"),
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
                "status": "failed" if item.status == "failed" else "complete",
                "error_code": item.error_code,
            }
            for item in native.metadata.institutions
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
        transaction_count=len(native.transactions),
    )
    client.get_data.return_value = envelope
    client.get_status.return_value = SyncStatusV2Response.model_validate({
        "job_id": job_id,
        "contract_version": 2,
        "status": "completed",
        "transaction_count": len(native.transactions),
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


@pytest.mark.integration
@pytest.mark.slow
def test_sync_pull_makes_new_accounts_visible_in_dim_accounts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two accounts in the sync payload → both visible in core.dim_accounts after one pull().

    Derived from hand-inspection of plaid_sync_response.yaml:
      - acc_chase_check (Total Checking)
      - acc_chase_save  (Total Savings)
    Total distinct account_ids: 2. Count is derived from the fixture, not from
    observing program output (per .claude/rules/testing.md §Scenario
    Expectations Must Be Independently Derived).
    """
    db = _build_db(tmp_path, monkeypatch)

    with FIXTURE.open() as f:
        sync_data = PlaidSyncDataResponse.model_validate(yaml.safe_load(f))

    client = MagicMock()
    _set_v2_job(client, sync_data)

    loader = PlaidExtractor(db)
    service = SyncService(client=client, db=db, loader=loader)
    result = service.pull()

    assert result.accounts_loaded == 2
    assert result.transforms_applied is True, (
        f"sync.pull() did not auto-apply transforms; error={result.transforms_error}"
    )

    account_count = db.execute("SELECT COUNT(*) FROM core.dim_accounts").fetchone()
    assert account_count is not None
    assert account_count[0] == 2, (
        f"Expected 2 accounts in core.dim_accounts, found {account_count[0]}. "
        "Regression: this is the sync-path equivalent of the 2026-05-15 "
        "dim-staleness pattern that motivated smart-import-transform."
    )

    post_max = db.execute(
        "SELECT MAX(updated_at)::TIMESTAMP FROM core.dim_accounts"
    ).fetchone()
    assert post_max is not None
    assert post_max[0] is not None, "core.dim_accounts.updated_at is NULL"

    status = SystemService(db).status()
    assert status.transforms_pending is False
    assert status.transforms_last_apply_at is not None


@pytest.mark.integration
@pytest.mark.slow
def test_sync_pull_no_refresh_leaves_dim_accounts_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """refresh=False loads raw but defers SQLMesh; dim_accounts stays empty.

    This is the opt-out branch of the contract: raw rows must land durably
    but core.* models stay stale until the next transform_apply.
    """
    db = _build_db(tmp_path, monkeypatch)

    with FIXTURE.open() as f:
        sync_data = PlaidSyncDataResponse.model_validate(yaml.safe_load(f))

    client = MagicMock()
    _set_v2_job(client, sync_data)

    loader = PlaidExtractor(db)
    service = SyncService(client=client, db=db, loader=loader)
    result = service.pull(refresh=False)

    assert result.accounts_loaded == 2
    assert result.transforms_applied is False
    assert result.transforms_error is None

    raw_count = db.execute("SELECT COUNT(*) FROM raw.plaid_accounts").fetchone()
    assert raw_count is not None
    assert raw_count[0] == 2, "raw.plaid_accounts must persist regardless of transforms"

    status = SystemService(db).status()
    assert status.transforms_pending is True, (
        "raw rows landed without transform; system_status must surface pending"
    )
