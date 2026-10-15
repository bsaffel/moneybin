"""Recovery evaluates retrieval against acknowledged native account history."""

from uuid import UUID

import pytest

from moneybin.connectors.sync_models import (
    SimpleFINSourceResult,
    SyncAckV2Response,
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
