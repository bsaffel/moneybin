"""Native delivery evidence bounds broker completeness and balance freshness."""

import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    SimpleFINSource,
    SimpleFINSourceResult,
    SimpleFINV1OrgScope,
    SyncEnvelopeV2,
)
from moneybin.database import Database
from moneybin.extractors.simplefin.identity import source_origin
from moneybin.repositories.sync_job_state_repo import SyncJobStateRepo
from moneybin.services.sync_completeness import SyncCompletenessService
from tests.moneybin.connectors.test_sync_pull_completeness import (
    _completed_pull,  # pyright: ignore[reportPrivateUsage]  # shared synthetic history
)
from tests.moneybin.db_helpers import create_core_tables
from tests.moneybin.test_services.test_simplefin_sync_service import (
    JOB,
    NOW,
    _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker
    _service,  # pyright: ignore[reportPrivateUsage]  # real ingestion service
)


def _delivery(db: Database, protocol: str) -> SyncEnvelopeV2:
    fixture = Path(__file__).parents[1] / "connectors/fixtures/sync_v2"
    raw = json.loads((fixture / f"simplefin_{protocol}.json").read_text())
    raw["job_id"] = str(JOB)
    native = raw["sources"][0]
    for account in native["batches"][0]["payload"]["accounts"]:
        account["balance"] = "100.00"
        account["transactions"] = []
    completed = _completed_pull(protocol).source_results[0]
    assert isinstance(completed, SimpleFINSourceResult)
    native["history"] = completed.history
    data = SyncEnvelopeV2.model_validate(raw)
    source = data.sources[0]
    assert isinstance(source, SimpleFINSource)
    assert source.history is not None
    create_core_tables(db)
    for index, account in enumerate(source.history.accounts):
        scope = account.native_scope
        origin = source_origin(
            source.connection_id,
            source.protocol_version,
            scope.org.model_dump(by_alias=True, exclude_none=True)
            if isinstance(scope, SimpleFINV1OrgScope)
            else scope.model_dump(by_alias=True),
        )
        db.execute(
            """INSERT INTO app.account_links
                (link_id, account_id, ref_kind, ref_value, source_type,
                 source_origin, status, decided_by, decided_at)
                VALUES (?, ?, 'source_native', ?, 'simplefin', ?,
                        'accepted', 'system', CURRENT_TIMESTAMP)""",
            [f"link-{index}", f"canonical-{index}", account.account_id, origin],
        )
        db.execute(
            """INSERT INTO app.account_settings
                (account_id, account_type, account_subtype, include_in_net_worth)
                VALUES (?, 'depository', 'checking', TRUE)""",
            [f"canonical-{index}"],
        )
    return data


@pytest.mark.parametrize("protocol", ["v1", "v2"])
@pytest.mark.parametrize("delivery", ["all", "empty", "subset", "split", "wrong_scope"])
def test_simplefin_completeness_requires_delivered_native_accounts(
    db: Database, protocol: str, delivery: str
) -> None:
    data = _delivery(db, protocol)
    full_delivery = data.model_copy(deep=True)
    source = data.sources[0]
    assert isinstance(source, SimpleFINSource)
    payload = source.batches[0].payload
    accounts = cast(list[dict[str, object]], payload["accounts"])
    if delivery == "empty":
        payload["accounts"] = []
    elif delivery == "subset":
        payload["accounts"] = accounts[:1]
    elif delivery == "wrong_scope":
        accounts[1] = deepcopy(accounts[0])
    elif delivery == "split":
        second = source.batches[0].model_copy(deep=True)
        second.batch_id = UUID(int=901)
        second_accounts = cast(list[dict[str, object]], second.payload["accounts"])
        second.payload["accounts"] = second_accounts[1:]
        source.batches.append(second)
        payload["accounts"] = accounts[:1]
    if protocol == "v1":
        for batch in source.batches:
            for account in cast(list[dict[str, object]], batch.payload["accounts"]):
                organization = cast(dict[str, str], account["org"])
                organization["name"] = "Renamed synthetic institution"
    original = data.model_dump(mode="json")
    client = _client(data)
    service = _service(db, client)

    result = service.pull(refresh=False)

    complete = delivery in {"all", "split"}
    assert SyncCompletenessService(db).assess().complete is complete
    assert result.complete is complete
    assert result.ingestion_state == "committed"
    assert result.ack_state == "confirmed"
    stored = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert stored is not None
    assert stored.details.acknowledgement is not None
    assert stored.details.sources[0].source.outcomes == source.outcomes
    assert stored.details.acknowledgement.results[0].outcomes == source.outcomes
    raw = db.execute(
        "SELECT source_metadata, local_outcomes FROM raw.simplefin_sync_receipts"
    ).fetchone()
    assert raw is not None
    assert json.loads(raw[0]) == source.model_dump(mode="json", exclude={"batches"})
    if not complete:
        assert json.loads(raw[1])
    assert data.model_dump(mode="json") == original
    assert (
        service.recover(str(JOB), refresh=False, local_only=True).complete is complete
    )

    if not complete:
        repaired = full_delivery
        repaired_source = repaired.sources[0]
        assert isinstance(repaired_source, SimpleFINSource)
        repaired_source.batches[0].batch_id = UUID(int=902)
        repaired.job_id = UUID(int=903)
        newer = _client(repaired)
        newer.trigger_sync.return_value.job_id = str(repaired.job_id)
        newer.get_status.return_value.job_id = repaired.job_id
        newer.get_status.return_value.started_at = datetime(2026, 10, 2, tzinfo=UTC)
        newer.get_status.return_value.completed_at = datetime(2026, 10, 2, tzinfo=UTC)
        newer.ack.return_value.job_id = repaired.job_id
        _service(db, newer).pull(refresh=False)
        assert SyncCompletenessService(db).assess().complete


@pytest.mark.parametrize("missing_first", [False, True])
@pytest.mark.parametrize("observation", ["missing", "invalid", "older", "newer"])
def test_connection_balance_age_requires_every_history_account(
    db: Database, missing_first: bool, observation: str
) -> None:
    data = _delivery(db, "v2")
    source = data.sources[0]
    assert isinstance(source, SimpleFINSource)
    accounts = cast(list[dict[str, object]], source.batches[0].payload["accounts"])
    recent = int(datetime(2026, 9, 29, tzinfo=UTC).timestamp())
    older = int(datetime(2026, 9, 28, tzinfo=UTC).timestamp())
    accounts[0]["balance-date"] = recent
    if observation == "missing":
        del accounts[1]
    else:
        accounts[1]["balance-date"] = older if observation == "older" else recent
        if observation == "invalid":
            accounts[1]["balance-date"] = "not-a-timestamp"
    assert source.history is not None
    if missing_first:
        source.history.accounts.reverse()
    client = _client(data)
    service = _service(db, client)
    service.pull(refresh=False)
    client.list_institutions.return_value = [
        ConnectedInstitution(
            id=str(source.connection_id),
            provider="simplefin",
            provider_item_id=None,
            institution_name=None,
            status="active",
            created_at=NOW,
            credential_generation=source.credential_generation,
            protocol_version=source.protocol_version,
            accounts=[],
            history=source.history,
        )
    ]

    result = service.list_connections()[0]

    expected = None
    if observation in {"older", "newer"}:
        expected = datetime.fromtimestamp(
            older if observation == "older" else recent, UTC
        )
    assert result.provider_balance_at == expected
