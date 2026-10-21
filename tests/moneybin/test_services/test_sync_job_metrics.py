"""Existing sync counters reflect durable writes and retrieval completeness."""

from unittest.mock import MagicMock
from uuid import UUID

import pytest

from moneybin.connectors.sync_models import (
    PlaidItemScope,
    PlaidSource,
    ScopeOutcome,
    SimpleFINV2ConnectionScope,
)
from moneybin.database import Database
from moneybin.services import sync_service
from tests.moneybin.test_services.test_simplefin_sync_service import (
    _cash_plaid_envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
    _service,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
)  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixtures


@pytest.mark.parametrize("provider", ["plaid", "simplefin", None])
def test_empty_sources_preserve_failure_metrics(
    db: Database, monkeypatch: pytest.MonkeyPatch, provider: str | None
) -> None:
    outcomes, transactions, duration = MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setattr(sync_service, "SYNC_PULL_OUTCOMES_TOTAL", outcomes)
    monkeypatch.setattr(sync_service, "SYNC_PULL_TRANSACTIONS_LOADED", transactions)
    monkeypatch.setattr(sync_service, "SYNC_PULL_DURATION_SECONDS", duration)
    data = _envelope()
    data.sources = []

    result = _service(db, _client(data)).pull(provider=provider, refresh=False)

    label = provider or "all"
    assert not result.complete
    outcomes.labels.assert_called_once_with(provider=label, status="failed")
    outcomes.labels.return_value.inc.assert_called_once_with()
    transactions.labels.assert_called_once_with(provider=label)
    transactions.labels.return_value.inc.assert_called_once_with(0)
    duration.labels.assert_called_once_with(provider=label)
    duration.labels.return_value.observe.assert_called_once()
    assert duration.labels.return_value.observe.call_args.args[0] >= 0


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [("empty", "failed"), ("mixed", "failed"), ("explicit-no-accounts", "success")],
)
def test_provider_success_requires_outcomes_for_every_source(
    db: Database, monkeypatch: pytest.MonkeyPatch, evidence: str, expected: str
) -> None:
    counter = MagicMock()
    monkeypatch.setattr(sync_service, "SYNC_PULL_OUTCOMES_TOTAL", counter)
    data = _envelope()
    source = PlaidSource(
        provider="plaid",
        connection_id=UUID(int=401),
        credential_generation=1,
        batches=[],
        outcomes=[],
    )
    data.sources = [source]
    if evidence != "empty":
        complete = source.model_copy(deep=True)
        complete.connection_id = UUID(int=402)
        batch = _cash_plaid_envelope().sources[0].batches[0]
        batch.payload["accounts"] = []
        batch.payload["transactions"] = []
        batch.payload["balances"] = []
        complete.batches = [batch]
        complete.outcomes = [
            ScopeOutcome(
                account_id=None,
                native_scope=PlaidItemScope(
                    kind="plaid_item", provider_item_id="plaid-item-1"
                ),
                window=None,
                status="complete",
                error_code=None,
            )
        ]
        data.sources = (
            [complete] if evidence == "explicit-no-accounts" else [source, complete]
        )
    _service(db, _client(data)).pull(refresh=False)
    counter.labels.assert_called_once_with(provider="plaid", status=expected)


@pytest.mark.parametrize("status", ["completed", "partial", "failed"])
def test_durable_provider_metrics_keep_retrieval_failure_distinct(
    status: str, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    outcomes, transactions, duration = MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setattr(sync_service, "SYNC_PULL_OUTCOMES_TOTAL", outcomes)
    monkeypatch.setattr(sync_service, "SYNC_PULL_TRANSACTIONS_LOADED", transactions)
    monkeypatch.setattr(sync_service, "SYNC_PULL_DURATION_SECONDS", duration)
    data = _envelope(status=status)
    if status == "completed":
        source = data.sources[0]
        source.outcomes = [
            ScopeOutcome(
                account_id="acct",
                native_scope=SimpleFINV2ConnectionScope(
                    kind="simplefin_v2_connection", conn_id="native-connection"
                ),
                window=source.batches[0].window,
                status="complete",
                error_code=None,
            )
        ]
    result = _service(db, _client(data)).pull(refresh=False)
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
