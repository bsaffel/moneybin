"""Existing sync counters reflect durable writes and retrieval completeness."""

from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services import sync_service
from tests.moneybin.test_services.test_simplefin_sync_service import (
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
    _service,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
)  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixtures


@pytest.mark.parametrize("status", ["completed", "partial", "failed"])
def test_durable_provider_metrics_keep_retrieval_failure_distinct(
    status: str, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    outcomes, transactions, duration = MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setattr(sync_service, "SYNC_PULL_OUTCOMES_TOTAL", outcomes)
    monkeypatch.setattr(sync_service, "SYNC_PULL_TRANSACTIONS_LOADED", transactions)
    monkeypatch.setattr(sync_service, "SYNC_PULL_DURATION_SECONDS", duration)
    result = _service(db, _client(_envelope(status=status))).pull(refresh=False)
    outcomes.labels.assert_called_once_with(
        provider="simplefin", status="success" if status == "completed" else "failed"
    )
    transactions.labels.assert_called_once_with(provider="simplefin")
    transactions.labels.return_value.inc.assert_called_once_with(
        result.transactions_loaded
    )
    duration.labels.assert_called_once_with(provider="simplefin")
    assert duration.labels.return_value.observe.call_count == 1


def test_job_write_failure_counts_failure_without_durable_load(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    outcomes, transactions, duration = MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setattr(sync_service, "SYNC_PULL_OUTCOMES_TOTAL", outcomes)
    monkeypatch.setattr(sync_service, "SYNC_PULL_TRANSACTIONS_LOADED", transactions)
    monkeypatch.setattr(sync_service, "SYNC_PULL_DURATION_SECONDS", duration)
    monkeypatch.setattr(
        sync_service.SimpleFINExtractor,
        "load",
        MagicMock(side_effect=ValueError("synthetic write failure")),
    )
    with pytest.raises(ValueError, match="synthetic write failure"):
        _service(db, _client(_envelope())).pull(refresh=False)
    outcomes.labels.assert_called_once_with(provider="simplefin", status="failed")
    transactions.labels.assert_not_called()
    assert duration.labels.return_value.observe.call_count == 1


@pytest.mark.parametrize("rollback", [False, True])
def test_account_resolution_metric_waits_for_setup_commit(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    rollback: bool,
) -> None:
    counter = MagicMock()
    monkeypatch.setattr(
        sync_service, "ACCOUNT_LINK_OUTCOMES_TOTAL", counter, raising=False
    )
    commit = db.commit
    calls = 0

    def setup_commit() -> None:
        nonlocal calls
        calls += 1
        if rollback and calls == 2:
            raise OSError("synthetic local setup commit failure")
        commit()

    monkeypatch.setattr(db, "commit", setup_commit)
    result = _service(db, _client(_envelope())).pull(refresh=False)
    assert result.ingestion_state == "committed"
    if rollback:
        assert result.setup_state == "failed"
        counter.labels.assert_not_called()
    else:
        assert result.setup_state == "complete"
        counter.labels.assert_called_once_with(result="minted_new")
        counter.labels.return_value.inc.assert_called_once()
