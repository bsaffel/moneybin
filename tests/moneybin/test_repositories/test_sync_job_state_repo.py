"""Retained source evidence cannot be forged by ingestion or recovery writes."""

import pytest

from moneybin.database import Database
from moneybin.repositories.sync_job_state_repo import SyncJobStateRepo
from tests.moneybin.test_services.test_simplefin_sync_service import (
    JOB,
    _cash_plaid_envelope,  # pyright: ignore[reportPrivateUsage]  # synthetic fixture
    _client,  # pyright: ignore[reportPrivateUsage]  # synthetic fixture
    _service,  # pyright: ignore[reportPrivateUsage]  # synthetic fixture
)


def test_new_plaid_job_requires_batch_digest_evidence(db: Database) -> None:
    client = _client(_cash_plaid_envelope())
    _service(db, client).pull(refresh=False)
    repo = SyncJobStateRepo(db)
    state = repo.get(str(JOB), client.consumer_id)
    assert state is not None
    details = state.details.model_dump()
    for source in details["sources"]:
        source.pop("batch_digests", None)
    db.execute("DELETE FROM app.sync_job_state")
    with pytest.raises(ValueError, match="Plaid batch digest"):
        repo.ensure(
            str(JOB), client.consumer_id, type(state.details).model_validate(details)
        )
    assert repo.get(str(JOB), client.consumer_id) is None


def test_recovery_cannot_replace_immutable_source_evidence(db: Database) -> None:
    client = _client(_cash_plaid_envelope())
    _service(db, client).pull(refresh=False)
    repo = SyncJobStateRepo(db)
    state = repo.get(str(JOB), client.consumer_id)
    assert state is not None
    before = state.details.sources[0].source.credential_generation
    state.details.sources[0].source.credential_generation += 1
    with pytest.raises(ValueError, match="immutable sync job source evidence"):
        repo.save(state)
    retained = repo.get(str(JOB), client.consumer_id)
    assert retained is not None
    assert retained.details.sources[0].source.credential_generation == before
