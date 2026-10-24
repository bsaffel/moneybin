"""New Plaid jobs cannot order competing observations of the same native item."""

import json
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from unittest.mock import patch
from uuid import UUID

import pytest

from moneybin.connectors.sync_errors import SyncAPIError
from moneybin.connectors.sync_models import (
    PlaidSourceResult,
    PullResult,
    SyncEnvelopeV2,
    SyncJobDetails,
    SyncJobSource,
)
from moneybin.database import Database
from moneybin.extractors.simplefin.extractor import SimpleFINExtractor
from moneybin.repositories.sync_job_state_repo import SyncJobStateRepo
from tests.moneybin.test_services.test_simplefin_sync_service import (
    NOW,
    _cash_plaid_envelope,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
    _client,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
    _envelope,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
    _service,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
)


def _batches(kind: str, *, separate_sources: bool = False) -> SyncEnvelopeV2:
    wire = _cash_plaid_envelope().model_dump(mode="json")
    source = wire["sources"][0]
    newer = source["batches"][0]
    newer["fetched_at"] = NOW.isoformat()
    newer["payload"]["metadata"]["synced_at"] = NOW.isoformat()
    older = deepcopy(newer)
    older["batch_id"] = str(UUID(int=802))
    older["fetched_at"] = (NOW - timedelta(days=1)).isoformat()
    older["payload"]["metadata"]["synced_at"] = older["fetched_at"]
    older["payload"]["transactions"][0]["amount"] = "1.00"
    if kind in {"remove", "resurrect"}:
        removal = older if kind == "remove" else newer
        removal["payload"]["transactions"] = []
        removal["payload"]["removed_transactions"] = ["plaid-transaction-1"]
    elif kind == "empty_accounts":
        for batch in (newer, older):
            for field in ("accounts", "transactions", "balances"):
                batch["payload"][field] = []
    if separate_sources:
        second = deepcopy(source)
        second["connection_id"] = str(UUID(int=803))
        second["batches"] = [older]
        wire["sources"].append(second)
    else:
        source["batches"].append(older)
    return SyncEnvelopeV2.model_validate(wire)


def _seed_current_transaction(db: Database) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_transactions
            (transaction_id, account_id, amount, transaction_date,
             source_file, source_origin)
        VALUES ('plaid-transaction-1', 'plaid-account-1', 99,
                DATE '2026-09-01', 'newer-job', 'plaid-item-1')
        """
    )


@pytest.mark.parametrize("kind", ["update", "remove", "resurrect", "empty_accounts"])
@pytest.mark.parametrize("separate_sources", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_new_overlapping_plaid_items_refused_before_any_mutation(
    db: Database, kind: str, separate_sources: bool, reverse: bool
) -> None:
    data = _batches(kind, separate_sources=separate_sources)
    if reverse:
        if separate_sources:
            data.sources.reverse()
        else:
            data.sources[0].batches.reverse()
    data.sources.insert(0, _envelope().sources[0])
    _seed_current_transaction(db)
    client = _client(data)
    service = _service(db, client)
    with (
        patch.object(
            SimpleFINExtractor, "load", wraps=SimpleFINExtractor(db).load
        ) as simplefin_load,
        patch.object(service.loader, "load", wraps=service.loader.load) as plaid_load,
        patch.object(
            service.loader,
            "handle_removed_transactions",
            wraps=service.loader.handle_removed_transactions,
        ) as remove,
        pytest.raises(SyncAPIError, match="overlap"),
    ):
        service.pull(refresh=False)

    simplefin_load.assert_not_called()
    plaid_load.assert_not_called()
    remove.assert_not_called()
    client.ack.assert_not_called()
    assert db.execute("SELECT amount FROM raw.plaid_transactions").fetchall() == [
        (Decimal("99.00"),)
    ]
    for table in (
        "raw.simplefin_responses",
        "raw.plaid_accounts",
        "app.sync_job_state",
    ):
        assert db.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)  # noqa: S608  # fixed test table names


@pytest.mark.parametrize("separate_sources", [False, True])
def test_disjoint_plaid_item_batches_are_accepted(
    db: Database, separate_sources: bool
) -> None:
    wire = _batches("update", separate_sources=separate_sources).model_dump(mode="json")
    second_source = wire["sources"][-1]
    second = second_source["batches"][-1]
    second["payload"]["metadata"]["institutions"][0]["provider_item_id"] = (
        "plaid-item-2"
    )
    outcome = deepcopy(second_source["outcomes"][0])
    outcome["native_scope"]["provider_item_id"] = "plaid-item-2"
    if separate_sources:
        second_source["outcomes"] = [outcome]
    else:
        second_source["outcomes"].append(outcome)
    data = SyncEnvelopeV2.model_validate(wire)

    result = _service(db, _client(data)).pull(refresh=False)

    assert result.transactions_loaded == 2
    assert result.ack_state == "confirmed"
    assert db.execute(
        "SELECT source_origin, amount FROM raw.plaid_transactions ORDER BY source_origin"
    ).fetchall() == [
        ("plaid-item-1", Decimal("12.34")),
        ("plaid-item-2", Decimal("1.00")),
    ]


@pytest.mark.parametrize("changed_digest", [False, True])
def test_retained_overlapping_job_requires_identical_evidence_and_never_reapplies(
    db: Database, changed_digest: bool
) -> None:
    data = _batches("remove")
    source = data.sources[0]
    source_result = PlaidSourceResult.model_validate(
        source.model_dump(exclude={"batches"})
    )
    # Seed evidence committed by an older client without asking new ingestion to accept it.
    digests = {
        batch.batch_id: sha256(
            json.dumps(
                {
                    "job_id": str(data.job_id),
                    "connection_id": str(source.connection_id),
                    "credential_generation": source.credential_generation,
                    "batch": batch.model_dump(mode="json"),
                },
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode()
        ).hexdigest()
        for batch in source.batches
    }
    client = _client(data)
    SyncJobStateRepo(db).ensure(
        str(data.job_id),
        client.consumer_id,
        SyncJobDetails(
            sources=[
                SyncJobSource(
                    source=source_result,
                    batch_ids=[batch.batch_id for batch in source.batches],
                    plaid_item_ids=["plaid-item-1"],
                    batch_digests=digests,
                )
            ],
            result=PullResult(
                job_id=str(data.job_id),
                source_results=[source_result],
                transactions_loaded=1,
                accounts_loaded=2,
                balances_loaded=2,
                transactions_removed=1,
                institutions=[],
                setup_state="complete",
                refresh_state="complete",
            ),
        ),
    )
    _seed_current_transaction(db)
    if changed_digest:
        source.batches[-1].payload["future_native_field"] = "changed"
    service = _service(db, client)
    with (
        patch.object(service.loader, "load", wraps=service.loader.load) as load,
        patch.object(
            service.loader,
            "handle_removed_transactions",
            wraps=service.loader.handle_removed_transactions,
        ) as remove,
    ):
        if changed_digest:
            with pytest.raises(SyncAPIError, match="immutable Plaid batch"):
                service.pull(refresh=False)
            client.ack.assert_not_called()
        else:
            result = service.pull(refresh=False)
            assert result.transactions_loaded == result.transactions_removed == 0
            assert result.ack_state == "confirmed"
            client.ack.assert_called_once()
    load.assert_not_called()
    remove.assert_not_called()
    assert db.execute("SELECT amount FROM raw.plaid_transactions").fetchall() == [
        (Decimal("99.00"),)
    ]
