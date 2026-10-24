"""Final public phase projections preserve committed evidence and broker lifecycle."""

from contextlib import nullcontext
from datetime import date
from unittest.mock import MagicMock

import httpx
import pytest
import respx
from typer.testing import CliRunner

from moneybin.cli.commands import sync as cli_sync
from moneybin.cli.main import app
from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_models import (
    LinkStatusResponse,
    PullResult,
    SyncConnectionView,
    SyncHistoryAccountView,
    SyncHistoryView,
    SyncSourceView,
    UTCWindow,
)
from moneybin.database import Database
from moneybin.orchestration.refresh import RefreshResult
from moneybin.services import sync_service
from tests.moneybin.connectors import test_sync_client_v2 as synthetic_client
from tests.moneybin.test_services.test_simplefin_sync_service import (
    JOB,
    NOW,
    _client,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
    _envelope,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
    _service,  # pyright: ignore[reportPrivateUsage]  # synthetic broker fixture
)
from tests.moneybin.test_services.test_sync_grant_surfaces import CONNECTION

client = synthetic_client.client


@pytest.mark.parametrize("method", ["_public_sources", "_investment_source_overlap"])
def test_post_commit_inspection_failure_retains_real_completed_phases(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
) -> None:
    broker = _client(_envelope())
    service = _service(db, broker)
    monkeypatch.setattr(
        sync_service,
        "_refresh",
        MagicMock(return_value=RefreshResult(applied=True, duration_seconds=0)),
    )
    monkeypatch.setattr(
        service,
        method,
        MagicMock(side_effect=RuntimeError("synthetic private inspection error")),
    )
    result = service.pull()
    assert result.ingestion_state == "committed"
    assert result.ack_state == "confirmed" and result.setup_state == "complete"
    assert result.refresh_state == "complete"
    assert result.inspection_error_code == "local_inspection_failed"
    assert not result.complete
    assert db.execute(
        "SELECT count(*) FROM raw.simplefin_sync_receipts"
    ).fetchone() == (1,)
    broker.ack.assert_called_once()


def test_unresolved_history_cannot_claim_attempt_complete(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _service(db, _client(_envelope()))
    monkeypatch.setattr(
        sync_service,
        "_refresh",
        MagicMock(return_value=RefreshResult(applied=True, duration_seconds=0)),
    )
    result = service.pull()
    result.sources = [
        SyncSourceView(
            provider="simplefin",
            connection_id=CONNECTION,
            credential_generation=1,
            protocol_version="2.0.0-draft",
            history_phase="post_ack",
            history=SyncHistoryView(
                observed_at=NOW,
                replay_required=False,
                discovery_history_target=None,
                accounts=[
                    SyncHistoryAccountView(
                        account_id=None,
                        display_name="Unresolved account",
                        acknowledged_intervals=[
                            UTCWindow.model_validate({
                                "start": "2026-09-01T00:00:00Z",
                                "end": "2026-10-01T00:00:00Z",
                            })
                        ],
                        history_target=None,
                    )
                ],
            ),
        )
    ]
    assert result.ack_state == "confirmed" and result.refresh_state == "complete"
    assert not result.complete


def test_human_status_distinguishes_history_provider_balance_and_local_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.database_mocks import with_reporting_timezone

    service = MagicMock()
    service.db = with_reporting_timezone(MagicMock())
    service.list_connections.return_value = [
        SyncConnectionView(
            id=CONNECTION,
            provider="simplefin",
            provider_item_id=None,
            institution_name="Synthetic Bank",
            status="active",
            created_at=NOW,
            last_sync=NOW,
            provider_balance_at=None,
            setup_state="failed",
            refresh_state="deferred",
            history_phase="current",
            safe_history=SyncHistoryView(
                observed_at=NOW,
                replay_required=True,
                discovery_history_target=date(2026, 7, 1),
                accounts=[],
            ),
        )
    ]
    monkeypatch.setattr(
        cli_sync, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    result = CliRunner().invoke(app, ["sync", "status"])
    assert result.exit_code == 0, result.output
    for label in (
        "Acknowledged sync",
        "Provider balance",
        "History",
        "Local setup",
        "Refresh",
        "2026-07-01",
        "failed",
        "deferred",
    ):
        assert label in result.output


@pytest.mark.parametrize(
    ("state", "action"),
    [
        ("claiming", "resume_setup"),
        ("claim_outcome_unknown", "disable_token_and_relink"),
    ],
)
@respx.mock
def test_actual_simplefin_link_status_preserves_claim_state_and_action(
    client: SyncClient, state: str, action: str
) -> None:
    respx.get("https://test.api/sync/link/status").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": "synthetic",
                "status": state,
                "provider": "simplefin",
                "connection_id": None,
                "provider_item_id": None,
                "institution_name": None,
                "error": None,
                "action": action,
                "expiration": "2026-10-01T12:00:00Z",
            },
        )
    )
    result = client.get_link_status("synthetic")
    assert result.status == state and result.action == action
    assert result.connection_id is None


def test_unknown_simplefin_claim_never_auto_pulls(db: Database) -> None:
    broker = _client(_envelope())
    broker.list_institutions.return_value = []
    broker.poll_link_status.return_value = LinkStatusResponse.model_validate({
        "session_id": "synthetic",
        "status": "claim_outcome_unknown",
        "provider": "simplefin",
        "action": "disable_token_and_relink",
        "expiration": NOW,
    })
    service = _service(db, broker)
    service.pull = MagicMock(
        return_value=PullResult(
            job_id=str(JOB),
            transactions_loaded=0,
            accounts_loaded=0,
            balances_loaded=0,
            transactions_removed=0,
            institutions=[],
        )
    )
    result = service.link(provider="simplefin")
    service.pull.assert_not_called()
    assert result.status == "claim_outcome_unknown"
    assert result.action == "disable_token_and_relink"


def test_validated_ack_survives_postcommit_local_state_read_failure(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.database import _pin_cursor_to_moneybin  # pyright: ignore[reportPrivateUsage]  # established independent observer boundary
    from moneybin.repositories.sync_job_state_repo import SyncJobState, SyncJobStateRepo

    observer = db.conn.cursor()
    _pin_cursor_to_moneybin(observer)
    original = SyncJobStateRepo.get

    def read(
        repo: SyncJobStateRepo, job_id: str, consumer_id: str
    ) -> SyncJobState | None:
        if observer.execute(
            "SELECT count(*) FROM raw.simplefin_sync_receipts"
        ).fetchone() == (1,):
            raise OSError("synthetic postcommit metadata read unavailable")
        return original(repo, job_id, consumer_id)

    monkeypatch.setattr(SyncJobStateRepo, "get", read)
    broker = _client(_envelope())
    try:
        result = _service(db, broker).pull(refresh=False)
        assert result.ingestion_state == "committed"
        assert result.ack_state == "confirmed"
        assert observer.execute(
            "SELECT count(*) FROM raw.simplefin_sync_receipts"
        ).fetchone() == (1,)
        broker.ack.assert_called_once()
    finally:
        observer.close()


async def test_status_discovers_older_unfinished_job_after_reopen_without_current_grant(
    db: Database,
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A newer completed job and removed grant cannot hide durable repair identity."""
    from uuid import UUID

    from moneybin.mcp.tools import sync as mcp_sync
    from moneybin.repositories.sync_job_state_repo import SyncJobStateRepo

    broker = _client(_envelope())
    broker.ack.side_effect = OSError("synthetic lost ack")
    _service(db, broker).pull(refresh=False)
    repo = SyncJobStateRepo(db)
    older = repo.get(str(JOB), "synthetic-profile")
    assert older is not None
    newer_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    newer = older.details.model_copy(deep=True)
    newer.result.job_id = str(newer_id)
    newer.result.ack_state = "confirmed"
    newer.result.setup_state = "complete"
    newer.result.refresh_state = "complete"
    newer.result.inspection_error_code = None
    repo.ensure(str(newer_id), "synthetic-profile", newer)
    path = db._db_path  # pyright: ignore[reportPrivateUsage]  # reopen owned test database
    db.close()
    broker.list_institutions.return_value = []
    broker.trigger_sync.reset_mock()
    broker.get_data.reset_mock()
    broker.trigger_sync.side_effect = AssertionError("no provider retrieval")
    broker.get_data.side_effect = AssertionError("no held payload")
    monkeypatch.setattr(
        sync_service,
        "_refresh",
        MagicMock(return_value=RefreshResult(applied=True, duration_seconds=0)),
    )
    with Database(
        path, read_only=False, secret_store=mock_secret_store, no_auto_upgrade=True
    ) as reopened:
        service = _service(reopened, broker)
        monkeypatch.setattr(
            mcp_sync,
            "_build_sync_service",
            MagicMock(return_value=nullcontext(service)),
        )
        response = await mcp_sync.sync_status_coarse()
        payload = response.to_dict()
        jobs = payload["data"]["recovery_jobs"]
        assert [job["job_id"] for job in jobs] == [str(JOB)]
        assert jobs[0]["ack_state"] == "unknown"
        assert str(JOB) in " ".join(payload["actions"])
        assert "local_only" in " ".join(payload["actions"])
        repaired = service.recover(jobs[0]["job_id"], local_only=True)
        assert repaired.ack_state == "unknown" and repaired.setup_state == "complete"
        broker.trigger_sync.assert_not_called()
        broker.get_data.assert_not_called()
