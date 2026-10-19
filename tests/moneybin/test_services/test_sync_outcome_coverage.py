"""Recovery evaluates retrieval against acknowledged native account history."""

import json
from copy import deepcopy
from pathlib import Path
from uuid import UUID

import pytest

from moneybin.connectors.sync_models import (
    SimpleFINSourceResult,
    SyncAckV2Response,
    SyncEnvelopeV2,
    SyncJobDetails,
    SyncJobSource,
)
from moneybin.database import Database
from moneybin.extractors.simplefin.identity import source_origin
from moneybin.repositories.sync_job_state_repo import SyncJobStateRepo
from tests.moneybin.connectors.test_sync_pull_completeness import (
    _completed_pull,  # pyright: ignore[reportPrivateUsage]  # shared synthetic receipt
)
from tests.moneybin.db_helpers import create_core_tables
from tests.moneybin.test_services.test_simplefin_sync_service import (
    JOB,
    NOW,
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker
    _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker
    _service,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker
)


@pytest.mark.parametrize("missing_outcome", [False, True])
def test_recovery_checks_newly_acknowledged_native_account_scopes(
    db: Database, monkeypatch: pytest.MonkeyPatch, missing_outcome: bool
) -> None:
    result = _completed_pull("v2")
    source = result.source_results[0]
    assert isinstance(source, SimpleFINSourceResult)
    acknowledged = source.model_copy(deep=True)
    assert acknowledged.history is not None
    source.history = None
    if missing_outcome:
        source.outcomes = source.outcomes[:1]
    result.refresh_state = "deferred"
    result.sources = []
    create_core_tables(db)
    for index, account in enumerate(acknowledged.history.accounts):
        origin = source_origin(
            source.connection_id,
            source.protocol_version,
            account.native_scope.model_dump(by_alias=True),
        )
        db.execute(
            """INSERT INTO app.account_links
                (link_id, account_id, ref_kind, ref_value, source_type,
                 source_origin, status, decided_by, decided_at)
                VALUES (?, 'canonical-account', 'source_native', ?, 'simplefin',
                        ?, 'accepted', 'system', CURRENT_TIMESTAMP)""",
            [f"synthetic-link-{index}", account.account_id, origin],
        )
    broker = _client(_envelope())
    repo = SyncJobStateRepo(db)
    repo.ensure(
        result.job_id,
        broker.consumer_id,
        SyncJobDetails(
            sources=[SyncJobSource(source=source, batch_ids=[])],
            result=result,
            acknowledgement=SyncAckV2Response(
                job_id=UUID(result.job_id),
                contract_version=2,
                status="acked",
                acked_at=NOW,
                results=[acknowledged],
            ),
        ),
    )
    service = _service(db, broker)

    def no_investment_overlap() -> list[str]:
        return []

    monkeypatch.setattr(service, "_investment_source_overlap", no_investment_overlap)

    recovered = service.recover(result.job_id, refresh=False, local_only=True)

    assert recovered.complete is (not missing_outcome)
    assert recovered.sources[0].history is not None
    assert [
        account.account_id for account in recovered.sources[0].history.accounts
    ] == [
        "canonical-account",
        "canonical-account",
    ]
    recovered_source = recovered.source_results[0]
    assert isinstance(recovered_source, SimpleFINSourceResult)
    assert recovered_source.history == acknowledged.history
    assert len(recovered_source.outcomes) == (1 if missing_outcome else 2)
    stored = repo.get(result.job_id, broker.consumer_id)
    assert stored is not None
    stored_source = stored.details.result.source_results[0]
    assert isinstance(stored_source, SimpleFINSourceResult)
    assert stored_source.history is None


@pytest.mark.parametrize(
    "coverage", ["all", "none", "subset", "duplicate", "wrong_item", "partial"]
)
def test_plaid_completeness_requires_every_payload_account_scope(
    db: Database, coverage: str
) -> None:
    fixture = Path(__file__).parents[1] / "connectors/fixtures/sync_v2/plaid.json"
    envelope = json.loads(fixture.read_text())
    envelope["job_id"] = str(JOB)
    source = envelope["sources"][0]
    payload = source["batches"][0]["payload"]
    payload["metadata"]["institutions"] = [
        {
            "provider_item_id": "plaid-item-1",
            "institution_name": "Synthetic Bank",
            "status": "completed",
        }
    ]
    second_account = deepcopy(payload["accounts"][0])
    second_account["account_id"] = "plaid-account-2"
    payload["accounts"].append(second_account)
    second_outcome = deepcopy(source["outcomes"][0])
    second_outcome["account_id"] = "plaid-account-2"
    source["outcomes"].append(second_outcome)
    if coverage == "none":
        source["outcomes"] = []
    elif coverage == "subset":
        source["outcomes"] = source["outcomes"][:1]
    elif coverage == "duplicate":
        source["outcomes"][1] = deepcopy(source["outcomes"][0])
    elif coverage == "wrong_item":
        source["outcomes"][1]["native_scope"]["provider_item_id"] = "other-item"
    elif coverage == "partial":
        source["outcomes"][1]["status"] = "partial"
        source["outcomes"][1]["error_code"] = "provider_error"
    data = SyncEnvelopeV2.model_validate(envelope)
    original_outcomes = deepcopy(data.sources[0].outcomes)
    client = _client(data)
    service = _service(db, client)

    result = service.pull(refresh=False)

    assert result.complete is (coverage == "all")
    assert result.ingestion_state == "committed"
    assert result.ack_state == "confirmed"
    assert result.accounts_loaded == 2
    assert result.transactions_loaded == 1
    stored = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert stored is not None
    assert stored.details.sources[0].source.outcomes == original_outcomes
    assert stored.details.acknowledgement is not None
    assert stored.details.acknowledgement.results[0].outcomes == original_outcomes
    assert data.model_dump(mode="json", exclude_unset=True) == envelope

    recovered = service.recover(str(JOB), refresh=False, local_only=True)

    assert recovered.complete is (coverage == "all")
    assert recovered.source_results[0].outcomes == result.source_results[0].outcomes


@pytest.mark.parametrize("delivery", ["all", "empty", "subset", "split", "wrong_item"])
def test_plaid_completeness_requires_delivered_native_accounts(
    db: Database, delivery: str
) -> None:
    fixture = Path(__file__).parents[1] / "connectors/fixtures/sync_v2/plaid.json"
    envelope = json.loads(fixture.read_text())
    envelope["job_id"] = str(JOB)
    source = envelope["sources"][0]
    payload = source["batches"][0]["payload"]
    payload["metadata"]["institutions"] = [
        {
            "provider_item_id": "plaid-item-1",
            "institution_name": "Synthetic Bank",
            "status": "completed",
        }
    ]
    payload["transactions"] = []
    payload["balances"] = []
    second_account = deepcopy(payload["accounts"][0])
    second_account["account_id"] = "plaid-account-2"
    payload["accounts"].append(second_account)
    second_outcome = deepcopy(source["outcomes"][0])
    second_outcome["account_id"] = "plaid-account-2"
    source["outcomes"].append(second_outcome)
    if delivery == "empty":
        payload["accounts"] = []
    elif delivery == "subset":
        payload["accounts"] = payload["accounts"][:1]
    elif delivery == "wrong_item":
        payload["metadata"]["institutions"][0]["provider_item_id"] = "other-item"
    elif delivery == "split":
        second_batch = deepcopy(source["batches"][0])
        second_batch["batch_id"] = str(UUID(int=901))
        second_batch["payload"]["accounts"] = [second_account]
        source["batches"].append(second_batch)
        payload["accounts"] = payload["accounts"][:1]
    data = SyncEnvelopeV2.model_validate(envelope)
    original_outcomes = deepcopy(data.sources[0].outcomes)
    client = _client(data)
    service = _service(db, client)

    result = service.pull(refresh=False)

    complete = delivery in {"all", "split"}
    assert result.complete is complete
    assert result.ingestion_state == "committed"
    assert result.ack_state == "confirmed"
    assert result.transactions_loaded == 0
    stored = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert stored is not None
    assert stored.details.sources[0].source.outcomes == original_outcomes
    assert stored.details.acknowledgement is not None
    assert stored.details.acknowledgement.results[0].outcomes == original_outcomes
    assert data.model_dump(mode="json", exclude_unset=True) == envelope
    if not complete:
        missing = {"plaid-account-2"}
        if delivery != "subset":
            missing.add("plaid-account-1")
        local_outcomes = result.source_results[0].outcomes[len(original_outcomes) :]
        for outcome in original_outcomes:
            if outcome.account_id in missing:
                assert (
                    outcome.model_copy(
                        update={"status": "partial", "error_code": "provider_error"}
                    )
                    in local_outcomes
                )
        assert any(
            outcome.status == "partial" for outcome in result.sources[0].outcomes
        )

    recovered = service.recover(str(JOB), refresh=False, local_only=True)

    assert recovered.complete is complete
    assert recovered.source_results[0].outcomes == result.source_results[0].outcomes


@pytest.mark.parametrize(
    "evidence", ["no_sources", "no_batches", "no_outcomes", "account", "item"]
)
def test_zero_transaction_pull_requires_retrieval_evidence(
    db: Database, evidence: str
) -> None:
    fixture = Path(__file__).parents[1] / "connectors/fixtures/sync_v2/plaid.json"
    envelope = json.loads(fixture.read_text())
    envelope["job_id"] = str(JOB)
    source = envelope["sources"][0]
    payload = source["batches"][0]["payload"]
    payload["metadata"]["institutions"] = [
        {
            "provider_item_id": "plaid-item-1",
            "institution_name": "Synthetic Bank",
            "status": "completed",
        }
    ]
    payload["transactions"] = []
    payload["balances"] = []
    if evidence != "account":
        payload["accounts"] = []
    if evidence in {"no_batches", "no_outcomes"}:
        source["outcomes"] = []
    if evidence == "no_sources":
        envelope["sources"] = []
    elif evidence == "no_batches":
        source["batches"] = []
    elif evidence == "item":
        source["outcomes"][0]["account_id"] = None
    data = SyncEnvelopeV2.model_validate(envelope)
    client = _client(data)
    service = _service(db, client)

    result = service.pull(refresh=False)

    assert result.ingestion_state == "committed"
    assert result.ack_state == "confirmed"
    assert result.setup_state == "complete"
    assert result.refresh_state == ("deferred" if evidence == "account" else "complete")
    assert result.transactions_loaded == 0
    assert result.complete is (evidence in {"account", "item"})

    recovered = service.recover(str(JOB), refresh=False, local_only=True)

    assert recovered.complete is (evidence in {"account", "item"})
