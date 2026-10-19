"""Native delivery evidence bounds broker completeness and balance freshness."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
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
@pytest.mark.parametrize(
    "delivery",
    [
        "all",
        "empty",
        "subset",
        "split",
        "wrong_scope",
        "older_window",
        "narrower_window",
        "shorter_window",
        "wrong_scope_window",
        "incidental_older_window",
    ],
)
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
    elif delivery in {
        "older_window",
        "narrower_window",
        "shorter_window",
        "wrong_scope_window",
        "incidental_older_window",
    }:
        older = source.batches[0].model_copy(deep=True)
        older.batch_id = UUID(int=904)
        assert older.window is not None
        if delivery == "narrower_window":
            older.window.start += timedelta(days=1)
        elif delivery == "shorter_window":
            older.window.end -= timedelta(days=1)
        else:
            older.window.start -= timedelta(days=30)
            older.window.end -= timedelta(days=30)
        source.batches.append(older)
        if delivery in {"older_window", "narrower_window", "shorter_window"}:
            source.batches.pop(0)
        elif delivery == "wrong_scope_window":
            payload["accounts"] = accounts[:1]
        source.outcomes.extend([
            outcome.model_copy(update={"window": older.window}, deep=True)
            for outcome in source.outcomes
        ])
    if protocol == "v1":
        for batch in source.batches:
            for account in cast(list[dict[str, object]], batch.payload["accounts"]):
                organization = cast(dict[str, str], account["org"])
                organization["name"] = "Renamed synthetic institution"
    original = data.model_dump(mode="json")
    client = _client(data)
    service = _service(db, client)

    result = service.pull(refresh=False)

    complete = delivery in {"all", "split", "incidental_older_window"}
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


@pytest.mark.parametrize("protocol", ["v1", "v2"])
@pytest.mark.parametrize(
    "evidence",
    [
        "all",
        "split",
        "missing_scope",
        "missing_account",
        "missing_start",
        "missing_end",
        "split_missing",
        "partial",
        "failed",
        "deferred",
    ],
)
def test_simplefin_delivered_accounts_require_exact_outcome_coverage(
    db: Database, protocol: str, evidence: str
) -> None:
    data = _delivery(db, protocol)
    source = data.sources[0]
    assert isinstance(source, SimpleFINSource)
    assert source.history is not None
    batch = source.batches[0]
    accounts = cast(list[dict[str, object]], batch.payload["accounts"])
    held = source.outcomes[1].model_copy(deep=True)
    missing = evidence.startswith("missing_") or evidence == "split_missing"
    if evidence == "missing_account":
        accounts[1]["id"] = "unreported-account"
        native_field = "org" if protocol == "v1" else "conn_id"
        accounts[1][native_field] = deepcopy(accounts[0][native_field])
        held.account_id = "unreported-account"
        assert source.outcomes[0].native_scope is not None
        held.native_scope = source.outcomes[0].native_scope.model_copy(deep=True)
    if evidence in {"missing_scope", "missing_account", "split_missing"}:
        source.outcomes.pop(1)
        source.history.accounts.pop(1)
    if evidence in {"split", "split_missing", "missing_start", "missing_end"}:
        additional = batch.model_copy(deep=True)
        additional.batch_id = UUID(int=905)
        additional.payload["accounts"] = deepcopy(accounts[1:])
        source.batches.append(additional)
        if evidence in {"split", "split_missing"}:
            batch.payload["accounts"] = accounts[:1]
        else:
            assert additional.window is not None
            if evidence == "missing_start":
                additional.window.start -= timedelta(days=1)
            else:
                additional.window.end += timedelta(days=1)
            held.window = additional.window.model_copy()
    if evidence in {"partial", "failed", "deferred"}:
        source.outcomes[1] = source.outcomes[1].model_copy(
            update={"status": evidence, "error_code": "provider_error"}
        )
    if protocol == "v1":
        for delivered in source.batches:
            for account in cast(list[dict[str, object]], delivered.payload["accounts"]):
                cast(dict[str, str], account["org"])["name"] = "Changed display name"
    original = data.model_dump(mode="json")
    client = _client(data)
    service = _service(db, client)

    result = service.pull(refresh=False)

    receipt = db.execute(
        "SELECT source_metadata, local_outcomes FROM raw.simplefin_sync_receipts"
    ).fetchone()
    assert receipt is not None
    expected_local = (
        [
            held.model_copy(
                update={"status": "partial", "error_code": "provider_error"}
            ).model_dump(mode="json")
        ]
        if missing
        else []
    )
    local = json.loads(receipt[1])
    # V1 display labels can change without changing the native identity.
    if protocol == "v1" and missing:
        expected_local[0]["native_scope"]["org"]["name"] = "Changed display name"
    assert local == expected_local
    complete = evidence in {"all", "split"}
    assert result.complete is complete
    assert result.transactions_loaded == 0
    assert result.ingestion_state == "committed"
    assert result.ack_state == "confirmed"
    assert SyncCompletenessService(db).assess().complete is complete
    assert (
        service.recover(str(JOB), refresh=False, local_only=True).complete is complete
    )
    assert json.loads(receipt[0]) == source.model_dump(mode="json", exclude={"batches"})
    stored = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert stored is not None
    assert stored.details.acknowledgement is not None
    assert stored.details.sources[0].source.outcomes == source.outcomes
    assert stored.details.acknowledgement.results[0].outcomes == source.outcomes
    assert data.model_dump(mode="json") == original


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
