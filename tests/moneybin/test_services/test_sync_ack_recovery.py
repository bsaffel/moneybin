"""Crash and terminal acknowledgement recovery never requires held provider data."""

from copy import deepcopy
from typing import Any, cast
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from moneybin.connectors.sync_errors import SyncAPIError
from moneybin.connectors.sync_models import ScopeOutcome, SimpleFINSource
from moneybin.database import Database
from moneybin.repositories.sync_job_state_repo import SyncJobStateRepo
from tests.moneybin.test_services.test_simplefin_sync_service import (
    JOB,
    NOW,
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
    _service,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
)  # pyright: ignore[reportPrivateUsage]  # shared synthetic fixtures


def test_crash_after_broker_ack_recovers_via_status_after_reopen(
    db: Database, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _client(_envelope())
    original = SyncJobStateRepo.save
    monkeypatch.setattr(
        SyncJobStateRepo,
        "save",
        MagicMock(side_effect=OSError("synthetic disk failure")),
    )
    first = _service(db, client).pull(refresh=False)
    assert first.ingestion_state == "committed"
    assert db.execute("SELECT ack_state FROM app.sync_job_state").fetchone() == (
        "pending",
    )
    path = db._db_path  # pyright: ignore[reportPrivateUsage]  # reopen owned synthetic DB
    db.close()
    monkeypatch.setattr(SyncJobStateRepo, "save", original)
    client.get_status.return_value.acked_at = NOW
    client.reset_mock()
    client.ack.side_effect = AssertionError("confirmed status does not require re-ack")
    client.get_data.side_effect = AssertionError("held payload has expired")
    client.trigger_sync.side_effect = AssertionError(
        "recovery cannot trigger provider retrieval"
    )
    with Database(
        path, read_only=False, secret_store=mock_secret_store, no_auto_upgrade=True
    ) as reopened:
        result = _service(reopened, client).recover(str(JOB), refresh=False)
        assert result.ack_state == "confirmed"
        assert result.refresh_state == "deferred"
        assert reopened.execute(
            "SELECT count(*) FROM raw.simplefin_sync_receipts"
        ).fetchone() == (1,)
    client.get_status.assert_called_once_with(str(JOB))
    client.ack.assert_not_called()


@pytest.mark.parametrize("http_status", [404, 410])
def test_terminal_missing_job_stops_ack_retry_without_erasing_local_truth(
    db: Database,
    http_status: int,
) -> None:
    client = _client(_envelope())
    client.ack.side_effect = OSError("synthetic lost response")
    service = _service(db, client)
    service.pull(refresh=False)
    unavailable = SyncAPIError("Sync result unavailable")
    unavailable.status_code = http_status
    client.get_status.side_effect = unavailable
    client.reset_mock()
    first = service.recover(str(JOB), refresh=False)
    assert first.ack_state == "unavailable"
    assert first.ingestion_state == "committed"
    client.get_status.assert_called_once()
    client.reset_mock()
    second = service.recover(str(JOB), refresh=False)
    assert second.ack_state == "unavailable"
    client.get_status.assert_not_called()
    client.ack.assert_not_called()


def test_confirmed_ack_never_downgrades_on_old_payload_redelivery(db: Database) -> None:
    client = _client(_envelope())
    service = _service(db, client)
    first = service.pull(refresh=False)
    assert first.ack_state == "confirmed"
    previous = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert previous is not None
    client.reset_mock()
    client.ack.side_effect = OSError("old replay must not acknowledge again")
    second = service.pull(refresh=False)
    current = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert current is not None
    assert second.ack_state == "confirmed"
    assert current.acked_at == previous.acked_at
    assert current.details.acknowledgement == previous.details.acknowledgement
    assert second.transactions_loaded == 0
    client.ack.assert_not_called()


def test_mismatched_ack_generation_remains_unknown(db: Database) -> None:
    client = _client(_envelope())
    client.ack.return_value.results = deepcopy(client.ack.return_value.results)
    client.ack.return_value.results[0]["credential_generation"] = 999
    result = _service(db, client).pull(refresh=False)
    assert result.ingestion_state == "committed"
    assert result.ack_state == "unknown"
    assert db.execute("SELECT acked_at FROM app.sync_job_state").fetchone() == (None,)


@pytest.mark.parametrize("recover", [False, True])
@pytest.mark.parametrize("missing", [False, True])
def test_changed_ack_outcomes_remain_unknown(
    db: Database, recover: bool, missing: bool
) -> None:
    data = _envelope()
    native = data.sources[0]
    assert isinstance(native, SimpleFINSource)
    native.outcomes = [
        ScopeOutcome(
            account_id=None,
            native_scope=None,
            window=native.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    client = _client(data)
    service = _service(db, client)
    changed = deepcopy(client.ack.return_value.results)
    if missing:
        changed[0]["outcomes"] = []
    else:
        changed[0]["outcomes"][0]["status"] = "failed"
        changed[0]["outcomes"][0]["error_code"] = "provider_error"
    if recover:
        client.ack.side_effect = OSError("synthetic lost response")
        assert service.pull(refresh=False).ack_state == "unknown"
        client.get_status.return_value.acked_at = NOW
        client.get_status.return_value.results = changed
        result = service.recover(str(JOB), refresh=False)
    else:
        client.ack.return_value.results = changed
        result = service.pull(refresh=False)

    assert result.ack_state == "unknown"
    assert result.complete is False
    assert result.ingestion_state == "committed"
    state = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert state is not None
    assert state.acked_at is None
    assert state.details.acknowledgement is None
    assert state.details.sources[0].source.outcomes[0].status == "complete"
    assert db.execute(
        "SELECT count(*) FROM raw.simplefin_sync_receipts"
    ).fetchone() == (1,)


def test_ack_preserves_local_validation_outcomes(db: Database) -> None:
    data = _envelope()
    payload = cast(dict[str, Any], data.sources[0].batches[0].payload)
    payload["accounts"][0]["transactions"][0]["amount"] = "-1.001"
    client = _client(data)

    result = _service(db, client).pull(refresh=False)

    assert result.ack_state == "confirmed"
    assert any(
        outcome.error_code == "unsupported_precision"
        for outcome in result.source_results[0].outcomes
    )
    state = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert state is not None
    assert state.details.sources[0].source.outcomes == []
    assert state.details.acknowledgement is not None
    assert state.details.acknowledgement.results[0].outcomes == []


def test_recovery_refuses_acknowledged_status_for_another_job(db: Database) -> None:
    client = _client(_envelope())
    client.ack.side_effect = OSError("synthetic lost response")
    service = _service(db, client)
    assert service.pull(refresh=False).ack_state == "unknown"
    client.get_status.return_value.acked_at = NOW
    client.get_status.return_value.job_id = UUID(int=801)

    result = service.recover(str(JOB), refresh=False)

    assert result.ack_state == "unknown"
    assert result.ingestion_state == "committed"
    state = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert state is not None
    assert state.acked_at is None
    assert state.details.acknowledgement is None


def test_mixed_replay_returns_delivery_counts_without_rewriting_ingestion_evidence(
    db: Database,
) -> None:
    from moneybin.connectors.sync_models import PlaidSource
    from tests.moneybin.test_services.test_simplefin_sync_service import _plaid  # pyright: ignore[reportPrivateUsage]  # synthetic investment fixture

    data = _envelope()
    data.sources.append(
        PlaidSource.model_validate({
            "provider": "plaid",
            "connection_id": str(UUID(int=300)),
            "credential_generation": 1,
            "outcomes": [],
            "batches": [
                {
                    "batch_id": str(UUID(int=301)),
                    "fetched_at": NOW,
                    "window": None,
                    "payload": _plaid().model_dump(mode="json"),
                }
            ],
        })
    )
    client = _client(data)
    service = _service(db, client)
    first = service.pull(refresh=False)
    before = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert before is not None
    replay = service.pull(refresh=False)
    after = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert after is not None
    count_fields = (
        "transactions_loaded",
        "accounts_loaded",
        "balances_loaded",
        "transactions_removed",
        "securities_loaded",
        "investment_transactions_loaded",
        "holdings_loaded",
        "holding_lots_loaded",
        "security_prices_loaded",
    )
    assert {name: getattr(before.details.result, name) for name in count_fields} == {
        name: getattr(after.details.result, name) for name in count_fields
    }
    assert replay.transactions_loaded < first.transactions_loaded
    assert replay.investment_transactions_loaded == 0
    assert replay.security_prices_loaded == 0
