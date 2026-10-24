"""Recovery actions and exact grant journeys preserve durable obligations."""

import json
from contextlib import nullcontext
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands import sync as cli_sync
from moneybin.cli.main import app
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    LinkStatusResponse,
    PullResult,
    SyncSourceView,
)
from moneybin.database import Database
from moneybin.mcp.tools import sync as mcp_sync
from moneybin.orchestration.refresh import RefreshResult
from moneybin.repositories.sync_job_state_repo import SyncJobStateRepo
from moneybin.services import sync_service
from tests.moneybin.test_services.test_simplefin_sync_service import (
    JOB,
    NOW,
    _client,  # pyright: ignore[reportPrivateUsage]  # synthetic job builder
    _envelope,  # pyright: ignore[reportPrivateUsage]  # synthetic job builder
    _service,  # pyright: ignore[reportPrivateUsage]  # synthetic job builder
)
from tests.moneybin.test_services.test_sync_grant_surfaces import (
    CONNECTION,
    _grant,  # pyright: ignore[reportPrivateUsage]  # synthetic grant builder
)


@pytest.mark.parametrize("surface", ["cli", "mcp"])
@pytest.mark.parametrize("obligation", ["unavailable", "partial", "local"])
async def test_actions_distinguish_terminal_retrieval_from_local_repair(
    surface: str, obligation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = PullResult(
        job_id=str(JOB),
        transactions_loaded=1,
        accounts_loaded=0,
        balances_loaded=0,
        transactions_removed=0,
        institutions=[],
        job_status="partial" if obligation == "partial" else "completed",
        ack_state="unavailable" if obligation == "unavailable" else "confirmed",
        setup_state="failed" if obligation == "local" else "complete",
        refresh_state="complete",
        sources=[
            SyncSourceView.model_validate({
                "provider": "simplefin",
                "connection_id": CONNECTION,
                "credential_generation": 2,
                "protocol_version": "2.0.0-draft",
                "history_phase": "unknown",
                "history": None,
                "outcomes": [],
            })
        ],
    )
    service = MagicMock()
    service.recover.return_value = result
    module = cli_sync if surface == "cli" else mcp_sync
    monkeypatch.setattr(
        module, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    if surface == "cli":
        response = CliRunner().invoke(
            app, ["sync", "pull", "--job-id", str(JOB), "--output", "json"]
        )
        assert response.exit_code == 1, response.output
        body = json.loads(response.stdout)
    else:
        body = (await mcp_sync.sync_pull_coarse(job_id=str(JOB))).to_dict()
    actions = " ".join(body["actions"])
    assert body["data"]["ingestion_state"] == "committed"
    if obligation == "local":
        assert ("--local-only" if surface == "cli" else "local_only=true") in actions
        assert str(JOB) in actions
    else:
        assert "cannot" in actions.lower() and "original job" in actions.lower()
        assert CONNECTION in actions
        assert ("--connection-id" if surface == "cli" else "connection_id=") in actions
    service.pull.assert_not_called()


def test_v1_history_uses_the_delivered_organization_identity(db: Database) -> None:
    client = _client(_envelope())
    client.list_institutions.return_value = [
        ConnectedInstitution.model_validate({
            "id": CONNECTION,
            "provider": "simplefin",
            "provider_item_id": None,
            "institution_name": None,
            "status": "active",
            "created_at": NOW,
            "credential_generation": 1,
            "protocol_version": "1.0.7",
            "history": {
                "observed_at": NOW,
                "replay_required": False,
                "discovery_history_target": None,
                "accounts": [
                    {
                        "account_id": "private-native",
                        "native_scope": {
                            "kind": "simplefin_v1_org",
                            "org": {
                                "sfin-url": "https://synthetic.invalid",
                                "name": "Synthetic Bank",
                            },
                        },
                        "acknowledged_intervals": [],
                        "history_target": None,
                    }
                ],
            },
        })
    ]
    result = _service(db, client).list_connections()
    assert result[0].safe_history is not None
    assert result[0].safe_history.accounts[0].display_name == "Unresolved account"


def test_completed_refresh_is_invalidated_by_repaired_native_binding(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _client(_envelope())
    service = _service(db, client)
    refresh = MagicMock(return_value=RefreshResult(applied=True, duration_seconds=0))
    monkeypatch.setattr(sync_service, "_refresh", refresh)
    service.pull()
    state = SyncJobStateRepo(db).get(str(JOB), client.consumer_id)
    assert state is not None
    assert state.details.result.refresh_state == "complete"
    state.details.result.setup_state = "failed"
    SyncJobStateRepo(db).save(state)
    db.execute(
        "DELETE FROM app.account_links WHERE source_type='simplefin' AND ref_kind='source_native'"
    )
    before = db.execute("SELECT count(*) FROM raw.simplefin_transactions").fetchone()
    refresh.reset_mock()
    client.reset_mock()
    result = service.recover(str(JOB), local_only=True)
    assert result.setup_state == "complete"
    assert result.refresh_state == "complete"
    refresh.assert_called_once()
    assert (
        db.execute("SELECT count(*) FROM raw.simplefin_transactions").fetchone()
        == before
    )
    client.trigger_sync.assert_not_called()
    client.get_data.assert_not_called()
    client.get_status.assert_not_called()


def test_simplefin_completed_link_auto_pull_targets_the_returned_grant(
    db: Database,
) -> None:
    client = _client(_envelope())
    client.list_institutions.return_value = []
    client.poll_link_status.return_value = LinkStatusResponse.model_validate({
        "session_id": "synthetic-session",
        "status": "linked",
        "provider": "simplefin",
        "connection_id": CONNECTION,
        "credential_generation": 2,
        "expiration": NOW,
    })
    service = _service(db, client)
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
    service.link(provider="simplefin")
    service.pull.assert_called_once_with(provider="simplefin", connection_id=CONNECTION)


def test_interactive_simplefin_disconnect_verifies_the_exact_confirmed_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.cli.output import OutputFormat
    from moneybin.connectors.sync_models import SyncDisconnectResult

    service = MagicMock()
    service.plan_disconnect.return_value = _grant()
    service.disconnect.return_value = SyncDisconnectResult(
        **_grant().model_dump(), remote_revocation="user_action_required"
    )
    service.disconnect_confirmed.return_value = SyncDisconnectResult(
        **_grant().model_dump(), remote_revocation="user_action_required"
    )
    monkeypatch.setattr(
        cli_sync, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    monkeypatch.setattr(
        cli_sync,
        "get_terminal_policy",
        MagicMock(return_value=MagicMock(interactive=True)),
    )
    monkeypatch.setattr(cli_sync, "_emit_sync_receipt", MagicMock())
    prompt = MagicMock(return_value=True)
    monkeypatch.setattr(cli_sync.typer, "confirm", prompt)
    cli_sync.sync_disconnect(
        institution=None,
        provider_item_id=None,
        connection_id=CONNECTION,
        purge=False,
        yes=False,
        output=OutputFormat.TEXT,
    )
    service.disconnect.assert_not_called()
    assert service.disconnect_confirmed.call_args.kwargs["connection_id"] == CONNECTION
    message = prompt.call_args.args[0]
    assert "Synthetic Checking" in message and "Synthetic Savings" in message
    changed = _grant().model_copy(update={"credential_generation": 3})
    with pytest.raises(Exception, match="changed"):
        service.disconnect_confirmed.call_args.kwargs["verify"](changed)
