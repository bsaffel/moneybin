"""Whole-job durability and local recovery across provider acknowledgement."""

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest
import yaml

from moneybin.connectors.sync_models import (
    PlaidSource,
    PlaidSyncDataResponse,
    SyncEnvelopeV2,
)
from moneybin.database import Database, _pin_cursor_to_moneybin  # pyright: ignore[reportPrivateUsage]  # established cursor catalog boundary
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.extractors.simplefin.extractor import SimpleFINExtractor
from moneybin.orchestration.refresh import RefreshResult
from moneybin.services import sync_service
from moneybin.services.sync_service import SyncService
from tests.moneybin.test_extractors.test_simplefin_extractor import source

JOB = UUID(int=800)
NOW = datetime(2026, 10, 1, 12, tzinfo=UTC)


def _envelope(*, status: str = "completed", empty: bool = False) -> SyncEnvelopeV2:
    data = source()
    if empty:
        data.batches = []
    return SyncEnvelopeV2.model_validate({
        "job_id": JOB,
        "contract_version": 2,
        "status": status,
        "sources": [data],
    })


def _client(data: SyncEnvelopeV2) -> MagicMock:
    client = MagicMock()
    client.consumer_id = "synthetic-profile"
    client.trigger_sync.return_value = SimpleNamespace(
        job_id=str(JOB),
        contract_version=2,
        status=data.status,
        transaction_count=1,
    )
    client.get_data.return_value = data
    results = [
        item.model_dump(mode="json", exclude={"batches"}) for item in data.sources
    ]
    client.get_status.return_value = SimpleNamespace(
        job_id=JOB,
        contract_version=2,
        status=data.status,
        started_at=NOW,
        completed_at=NOW,
        acked_at=None,
        results=results,
    )
    client.ack.return_value = SimpleNamespace(
        job_id=JOB,
        contract_version=2,
        status="acked",
        acked_at=NOW,
        results=results,
    )
    return client


def _service(db: Database, client: MagicMock) -> SyncService:
    return SyncService(client=client, db=db, loader=PlaidExtractor(db))


def test_ack_only_after_durable_job(db: Database) -> None:
    """A separate database connection sees Raw and recovery identity before ack."""
    client = _client(_envelope())
    observer = db.conn.cursor()
    _pin_cursor_to_moneybin(
        observer
    )  # New cursors otherwise select the memory catalog.
    ack_result = client.ack.return_value
    observed_errors: list[str] = []

    def ack(job_id: str) -> Any:
        assert job_id == str(JOB)
        try:
            assert observer.execute(
                "SELECT count(*) FROM raw.simplefin_transactions"
            ).fetchone() == (1,)
            assert observer.execute(
                "SELECT ack_state FROM app.sync_job_state WHERE job_id = ?", [str(JOB)]
            ).fetchone() == ("pending",)
        except Exception as exc:
            observed_errors.append(repr(exc))
            raise
        return ack_result

    client.ack.side_effect = ack
    try:
        result = _service(db, client).pull(refresh=False)
        assert result.transactions_loaded == 1
        assert getattr(result, "ingestion_state", None) == "committed"
        assert not observed_errors, observed_errors
        assert getattr(result, "ack_state", None) == "confirmed"
    finally:
        observer.close()


def test_partial_job_commits_valid_batches(db: Database) -> None:
    """Retrieval incompleteness does not discard accepted native observations."""
    client = _client(_envelope(status="partial"))
    result = _service(db, client).pull(refresh=False)
    assert result.transactions_loaded == 1
    assert getattr(result, "job_status", None) == "partial"
    assert db.execute("SELECT status FROM raw.simplefin_sync_receipts").fetchone() == (
        "partial",
    )
    assert getattr(result, "ingestion_state", None) == "committed"


def test_zero_batch_failure_is_durable_before_ack(db: Database) -> None:
    """An empty failed result retains job order and source outcome evidence."""
    client = _client(_envelope(status="failed", empty=True))
    result = _service(db, client).pull(refresh=False)
    assert result.transactions_loaded == 0
    assert db.execute(
        "SELECT status, batch_ids FROM raw.simplefin_sync_receipts"
    ).fetchone() == ("failed", "[]")
    assert getattr(result, "ack_state", None) == "confirmed"


def test_ack_failure_redelivery_is_safe(db: Database) -> None:
    """A lost ack response cannot duplicate observations or imply broker credit."""
    client = _client(_envelope())
    accepted = client.ack.return_value
    client.ack.side_effect = OSError("synthetic provider escape \x1b[31mprivate")
    first = _service(db, client).pull(refresh=False)
    assert getattr(first, "ingestion_state", None) == "committed"
    assert getattr(first, "ack_state", None) in {"pending", "unknown"}
    client.ack.side_effect = None
    client.ack.return_value = accepted
    second = _service(db, client).pull(refresh=False)
    assert second.transactions_loaded == 0
    assert getattr(second, "ack_state", None) == "confirmed"
    assert db.execute("SELECT count(*) FROM raw.simplefin_transactions").fetchone() == (
        1,
    )
    assert db.execute(
        "SELECT count(*) FROM raw.simplefin_sync_receipts"
    ).fetchone() == (1,)


def test_refresh_failure_retries_locally(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repair consumes durable job evidence without another upstream retrieval."""
    client = _client(_envelope())
    monkeypatch.setattr(
        sync_service,
        "_refresh",
        MagicMock(
            return_value=RefreshResult(
                applied=False, error="synthetic failure", duration_seconds=0
            )
        ),
    )
    service = _service(db, client)
    first = service.pull()
    assert getattr(first, "ingestion_state", None) == "committed"
    assert getattr(first, "refresh_state", None) == "failed"
    client.trigger_sync.side_effect = AssertionError("recovery must not fetch upstream")
    client.get_data.side_effect = AssertionError(
        "recovery must not require held payload"
    )
    monkeypatch.setattr(
        sync_service,
        "_refresh",
        MagicMock(return_value=RefreshResult(applied=True, duration_seconds=0)),
    )
    repaired = service.recover(str(JOB))
    assert getattr(repaired, "refresh_state", None) == "complete"
    assert db.execute(
        "SELECT count(*) FROM raw.simplefin_sync_receipts"
    ).fetchone() == (1,)


def test_mismatched_terminal_metadata_rolls_back_before_ack(db: Database) -> None:
    """The context must authenticate this envelope, not another completed job."""
    client = _client(_envelope())
    client.get_status.return_value.job_id = UUID(int=801)
    with pytest.raises(sync_service.SyncAPIError):
        _service(db, client).pull(refresh=False)
    assert db.execute(
        "SELECT count(*) FROM raw.simplefin_sync_receipts"
    ).fetchone() == (0,)
    client.ack.assert_not_called()


def _plaid() -> PlaidSyncDataResponse:
    fixture = (
        Path(__file__).parents[1]
        / "test_extractors/fixtures/plaid_investments_sync_response.yaml"
    )
    return PlaidSyncDataResponse.model_validate(yaml.safe_load(fixture.read_text()))


def test_mixed_provider_late_failure_rolls_back_plaid_receipts_and_deletes(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late SimpleFIN write failure cannot commit earlier Plaid investments."""
    data = _envelope()
    native = _plaid()
    native.removed_transactions = ["previous"]
    db.execute(
        "INSERT INTO raw.plaid_transactions (transaction_id, account_id, amount, transaction_date, source_file, source_origin) VALUES ('previous', 'account', 1, DATE '2026-09-01', 'prior', 'prior')"
    )
    plaid = PlaidSource.model_validate({
        "provider": "plaid",
        "connection_id": str(UUID(int=300)),
        "credential_generation": 1,
        "outcomes": [],
        "batches": [
            {
                "batch_id": str(UUID(int=301)),
                "fetched_at": NOW,
                "window": None,
                "payload": native.model_dump(mode="json"),
            }
        ],
    })
    data.sources.insert(0, plaid)
    client = _client(data)
    original = SimpleFINExtractor._write  # pyright: ignore[reportPrivateUsage]  # inject a late real-write failure

    def fail_last(self: SimpleFINExtractor, table: Any, rows: Any) -> int:
        if table.full_name == "raw.simplefin_transactions":
            raise ValueError("synthetic late write failure")
        return original(self, table, rows)

    monkeypatch.setattr(SimpleFINExtractor, "_write", fail_last)
    with pytest.raises(ValueError, match="synthetic late write failure"):
        _service(db, client).pull(refresh=False)
    assert db.execute(
        "SELECT transaction_id FROM raw.plaid_transactions"
    ).fetchall() == [("previous",)]
    assert db.execute(
        "SELECT count(*) FROM raw.plaid_investment_transaction_receipts"
    ).fetchone() == (0,)
    assert db.execute(
        "SELECT count(*) FROM raw.plaid_investment_holdings_snapshots"
    ).fetchone() == (0,)
    assert db.execute("SELECT count(*) FROM raw.simplefin_responses").fetchone() == (0,)
    client.ack.assert_not_called()


def test_setup_recovery_survives_reopen_without_held_payload(
    db: Database,
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed binding can be rebuilt from Raw after the broker payload expires."""
    client = _client(_envelope())
    original = sync_service.AccountResolver.resolve

    def fail_setup(*_args: Any, **_kwargs: Any) -> Any:
        raise ValueError("synthetic setup failure")

    monkeypatch.setattr(sync_service.AccountResolver, "resolve", fail_setup)
    first = _service(db, client).pull(refresh=False)
    assert getattr(first, "setup_state", None) == "failed"
    path = db._db_path  # pyright: ignore[reportPrivateUsage]  # reopen owned test database
    db.close()
    monkeypatch.setattr(sync_service.AccountResolver, "resolve", original)
    client.trigger_sync.side_effect = AssertionError(
        "no provider retrieval during repair"
    )
    client.get_data.side_effect = AssertionError("held payload has expired")
    with Database(
        path, read_only=False, secret_store=mock_secret_store, no_auto_upgrade=True
    ) as reopened:
        repaired = _service(reopened, client).recover(str(JOB), refresh=False)
        assert getattr(repaired, "setup_state", None) == "complete"
        assert reopened.execute(
            "SELECT count(*) FROM app.account_links WHERE source_type='simplefin'"
        ).fetchone() == (1,)
        assert reopened.execute(
            "SELECT count(*) FROM raw.simplefin_sync_receipts"
        ).fetchone() == (1,)


def test_plaid_batch_receipts_preserve_job_and_provider_order(db: Database) -> None:
    """Two immutable batches in one job cannot collapse investment receipts."""
    native = _plaid()
    loader = PlaidExtractor(db)
    first = UUID(int=301)
    second = UUID(int=302)
    original_synced_at = native.metadata.synced_at
    loader.load(native, "standalone-baseline")
    baseline = db.execute(
        "SELECT extracted_at, ingestion_sequence FROM raw.plaid_investment_transaction_receipts WHERE source_file = ? ORDER BY ingestion_sequence",
        ["sync_standalone-baseline"],
    ).fetchall()
    db.begin()
    try:
        loader.load(native, str(JOB), batch_id=first, in_outer_txn=True)
        loader.load(native, str(JOB), batch_id=second, in_outer_txn=True)
        db.commit()
    except BaseException:
        db.rollback()
        raise
    rows = db.execute(
        "SELECT DISTINCT source_file, extracted_at FROM raw.plaid_investment_transaction_receipts WHERE source_file != 'sync_standalone-baseline' ORDER BY source_file"
    ).fetchall()
    assert [row[0] for row in rows] == [f"sync_{JOB}_{first}", f"sync_{JOB}_{second}"]
    assert all(row[1] == baseline[0][0] for row in rows)
    assert native.metadata.synced_at == original_synced_at
    sequences = db.execute(
        "SELECT source_file, min(ingestion_sequence), max(ingestion_sequence) FROM raw.plaid_investment_transaction_receipts GROUP BY source_file ORDER BY min(ingestion_sequence)"
    ).fetchall()
    assert [row[0] for row in sequences] == [
        "sync_standalone-baseline",
        f"sync_{JOB}_{first}",
        f"sync_{JOB}_{second}",
    ]
    assert sequences[0][2] < sequences[1][1] <= sequences[1][2] < sequences[2][1]


def test_rolled_back_plaid_job_does_not_emit_durable_load_metrics(db: Database) -> None:
    """Transaction-owned metric buffering must discard receipts that never commit."""
    from moneybin.metrics.observations import MetricObservations
    from moneybin.metrics.registry import SYNC_INVESTMENTS_RECORDS_LOADED

    metric = SYNC_INVESTMENTS_RECORDS_LOADED.labels(
        table="plaid_investment_transactions"
    )
    before = metric._value.get()  # pyright: ignore[reportPrivateUsage]  # inspect the real counter
    observations = MetricObservations()
    loader = PlaidExtractor(db, observations=observations)
    db.begin()
    try:
        loader.load(_plaid(), str(JOB), batch_id=UUID(int=301), in_outer_txn=True)
    finally:
        db.rollback()
        observations.flush("rollback")
    assert metric._value.get() == before  # pyright: ignore[reportPrivateUsage]  # real rollback invariant


def test_initial_pull_retains_full_refresh_cascade(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ordinary pull keeps the existing refresh behavior, including configured gathers."""
    refresh = MagicMock(return_value=RefreshResult(applied=True, duration_seconds=0))
    monkeypatch.setattr(sync_service, "_refresh", refresh)
    result = _service(db, _client(_envelope())).pull()
    assert result.refresh_state == "complete"
    assert refresh.call_args.kwargs.get("steps") is None


def test_local_only_recovery_keeps_unknown_ack_without_broker_calls(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Offline repair fixes derived data without implying that the broker accepted ack."""
    client = _client(_envelope())
    client.ack.side_effect = OSError("synthetic lost response")
    service = _service(db, client)
    first = service.pull(refresh=False)
    assert first.ack_state == "unknown"
    client.reset_mock()
    for name in ("get_status", "ack", "get_data", "trigger_sync"):
        getattr(client, name).side_effect = AssertionError(
            "offline repair made a broker call"
        )
    refresh = MagicMock(return_value=RefreshResult(applied=True, duration_seconds=0))
    monkeypatch.setattr(sync_service, "_refresh", refresh)
    result = service.recover(str(JOB), local_only=True)
    assert result.ack_state == "unknown"
    assert result.refresh_state == "complete"
    assert set(refresh.call_args.kwargs["steps"]) == {
        "match",
        "investment_match",
        "transform",
        "categorize",
        "identity",
    }
    assert not client.mock_calls


def test_local_only_recovery_rejects_another_profile(db: Database) -> None:
    """Local repair cannot borrow another profile's committed job evidence."""
    client = _client(_envelope())
    service = _service(db, client)
    service.pull(refresh=False)
    client.consumer_id = "other-profile"
    client.reset_mock()
    with pytest.raises(Exception, match="profile"):
        service.recover(str(JOB), local_only=True)
    assert not client.mock_calls
