"""Client observability records durable ingestion and local refresh separately."""

from typing import Any

import pytest
from prometheus_client import CollectorRegistry, Counter

from moneybin.connectors.sync_models import ScopeOutcome
from moneybin.database import Database
from moneybin.orchestration.refresh import RefreshResult
from moneybin.services import sync_service
from tests.moneybin.test_services.test_simplefin_sync_service import (
    JOB,
    _client,  # pyright: ignore[reportPrivateUsage]
    _envelope,  # pyright: ignore[reportPrivateUsage]
    _service,  # pyright: ignore[reportPrivateUsage]
)


def _metrics(monkeypatch: pytest.MonkeyPatch) -> CollectorRegistry:
    registry = CollectorRegistry()
    for symbol, name in (
        ("SYNC_INGESTION_OUTCOMES_TOTAL", "moneybin_sync_ingestion_outcomes"),
        ("SYNC_REFRESH_OUTCOMES_TOTAL", "moneybin_sync_refresh_outcomes"),
        ("SYNC_PARTIAL_SCOPES_TOTAL", "moneybin_sync_partial_scopes"),
    ):
        metric = Counter(
            name, "Synthetic metric", ["provider", "status"], registry=registry
        )
        monkeypatch.setattr(sync_service, symbol, metric, raising=False)
    return registry


def test_durable_ingestion_metric_survives_ack_failure(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = _metrics(monkeypatch)
    client = _client(_envelope())
    client.ack.side_effect = OSError("synthetic lost acknowledgement")
    result = _service(db, client).pull(refresh=False)
    assert result.ingestion_state == "committed" and result.ack_state == "unknown"
    assert (
        registry.get_sample_value(
            "moneybin_sync_ingestion_outcomes_total",
            {"provider": "simplefin", "status": "committed"},
        )
        == 1
    )
    assert (
        registry.get_sample_value(
            "moneybin_sync_refresh_outcomes_total",
            {"provider": "simplefin", "status": "complete"},
        )
        is None
    )


def test_ingestion_rollback_never_counts_a_commit(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = _metrics(monkeypatch)

    def fail_commit() -> None:
        raise OSError("synthetic durable commit failure")

    monkeypatch.setattr(db, "commit", fail_commit)
    with pytest.raises(OSError, match="synthetic durable commit failure"):
        _service(db, _client(_envelope())).pull(refresh=False)
    assert db.execute(
        "SELECT count(*) FROM raw.simplefin_sync_receipts"
    ).fetchone() == (0,)
    assert (
        registry.get_sample_value(
            "moneybin_sync_ingestion_outcomes_total",
            {"provider": "simplefin", "status": "committed"},
        )
        is None
    )
    assert (
        registry.get_sample_value(
            "moneybin_sync_ingestion_outcomes_total",
            {"provider": "simplefin", "status": "failed"},
        )
        == 1
    )


def test_refresh_failure_is_distinct_from_committed_ingestion(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = _metrics(monkeypatch)

    def failed_refresh(*args: Any, **kwargs: Any) -> RefreshResult:
        return RefreshResult(
            applied=False, duration_seconds=None, error="synthetic projection failure"
        )

    monkeypatch.setattr(sync_service, "_refresh", failed_refresh)
    result = _service(db, _client(_envelope())).pull()
    assert result.ingestion_state == "committed" and result.refresh_state == "failed"
    assert (
        registry.get_sample_value(
            "moneybin_sync_ingestion_outcomes_total",
            {"provider": "simplefin", "status": "committed"},
        )
        == 1
    )
    assert (
        registry.get_sample_value(
            "moneybin_sync_refresh_outcomes_total",
            {"provider": "simplefin", "status": "failed"},
        )
        == 1
    )
    assert all(
        set(sample.labels) <= {"provider", "status"}
        for metric in registry.collect()
        for sample in metric.samples
    )


def test_replay_and_recovery_do_not_recount_durable_ingestion(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = _metrics(monkeypatch)
    data = _envelope(status="partial")
    data.sources[0].outcomes = [
        ScopeOutcome.model_validate({
            "account_id": "private-account-999999",
            "native_scope": {
                "kind": "simplefin_v2_connection",
                "conn_id": "private-connection",
            },
            "window": data.sources[0].batches[0].window,
            "status": "failed",
            "error_code": "https://private-host.invalid/private-token",
        })
    ]
    client = _client(data)
    service = _service(db, client)
    first = service.pull(refresh=False)
    assert first.ingestion_state == "committed"
    committed_labels = {"provider": "simplefin", "status": "committed"}
    partial_labels = {"provider": "simplefin", "status": "failed"}
    assert (
        registry.get_sample_value(
            "moneybin_sync_ingestion_outcomes_total", committed_labels
        )
        == 1
    )
    assert (
        registry.get_sample_value("moneybin_sync_partial_scopes_total", partial_labels)
        == 1
    )

    replayed = service.pull(refresh=False)
    assert replayed.ingestion_state == "committed"

    def refreshed(*args: Any, **kwargs: Any) -> RefreshResult:
        return RefreshResult(applied=True, duration_seconds=0)

    monkeypatch.setattr(sync_service, "_refresh", refreshed)
    recovered = service.recover(str(JOB), local_only=True)
    assert recovered.refresh_state == "complete"
    assert (
        registry.get_sample_value(
            "moneybin_sync_ingestion_outcomes_total", committed_labels
        )
        == 1
    )
    assert (
        registry.get_sample_value("moneybin_sync_partial_scopes_total", partial_labels)
        == 1
    )
    assert (
        registry.get_sample_value(
            "moneybin_sync_refresh_outcomes_total",
            {"provider": "simplefin", "status": "complete"},
        )
        == 1
    )
    assert all(
        set(sample.labels) <= {"provider", "status"}
        and not any("private" in value for value in sample.labels.values())
        for metric in registry.collect()
        for sample in metric.samples
    )
