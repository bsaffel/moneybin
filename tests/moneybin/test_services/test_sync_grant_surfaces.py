"""SimpleFIN grant management is reachable through the existing public workflow."""

import json
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands import sync as cli_sync
from moneybin.cli.main import app
from moneybin.connectors.sync_models import (
    ConnectedInstitution,
    LinkInitiateResponse,
    LinkResult,
    SyncAccountScope,
    SyncConnectionView,
    SyncDisconnectResponse,
    SyncDisconnectResult,
)
from moneybin.database import Database
from moneybin.mcp.tools import sync as mcp_sync

CONNECTION = str(UUID(int=100))


def _grant() -> ConnectedInstitution:
    return ConnectedInstitution(
        id=CONNECTION,
        provider="simplefin",
        provider_item_id=None,
        institution_name="Synthetic Bank",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        status="active",
        credential_generation=2,
        accounts=[
            SyncAccountScope.model_validate({
                "account_id": "private-native-123456",
                "native_scope": {
                    "kind": "simplefin_v2_connection",
                    "conn_id": "private-scope",
                },
                "name": "Synthetic Checking",
            }),
            SyncAccountScope.model_validate({
                "account_id": "private-native-987654",
                "native_scope": {
                    "kind": "simplefin_v2_connection",
                    "conn_id": "private-scope",
                },
                "name": "Synthetic Savings",
            }),
        ],
    )


@pytest.mark.parametrize("surface", ["cli", "mcp"])
async def test_simplefin_link_selector_uses_browser_flow_without_token_argument(
    surface: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = MagicMock()
    service.initiate_link.return_value = LinkInitiateResponse(
        session_id=str(UUID(int=500)),
        link_url="https://broker.invalid/claim",
        link_type="widget_flow",
        expiration=datetime(2026, 10, 1, tzinfo=UTC),
    )
    module = cli_sync if surface == "cli" else mcp_sync
    monkeypatch.setattr(
        module, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    if surface == "cli":
        result = CliRunner().invoke(
            app,
            [
                "sync",
                "link",
                "--provider",
                "simplefin",
                "--connection-id",
                CONNECTION,
                "--output",
                "json",
            ],
        )
        assert result.exit_code == 0, result.output
        body = json.loads(result.stdout)
    else:
        body = (
            await mcp_sync.sync_link_coarse(
                provider="simplefin", connection_id=CONNECTION
            )
        ).to_dict()
    assert body["data"]["link_type"] == "widget_flow"
    assert service.initiate_link.call_args.kwargs["provider"] == "simplefin"
    assert service.initiate_link.call_args.kwargs["connection_id"] == CONNECTION
    assert "setup_token" not in body and "consumer_id" not in json.dumps(body)


def test_disconnect_confirmation_binds_generation_and_all_affected_accounts() -> None:
    grant = _grant()
    binding = mcp_sync._sync_disconnect_binding(grant)  # pyright: ignore[reportPrivateUsage]  # public confirmation semantics
    changed = grant.model_copy(deep=True)
    changed.credential_generation += 1
    second = mcp_sync._sync_disconnect_binding(changed)  # pyright: ignore[reportPrivateUsage]  # confirmation must stale on replacement
    assert binding != second
    assert binding.blast_radius["accounts"] == 2
    message = mcp_sync._sync_disconnect_message(grant)  # pyright: ignore[reportPrivateUsage]  # actual elicitation text
    assert "Synthetic Checking" in message and "Synthetic Savings" in message
    assert "private-native" not in message and "private-scope" not in message


@pytest.mark.parametrize(
    ("output", "confirmed_interactively"),
    [("text", False), ("json", False), ("text", True)],
)
@pytest.mark.parametrize("institution_name", [None, "Synthetic Bank"])
def test_cli_automatic_relink_preserves_selected_connection(
    output: str,
    institution_name: str | None,
    confirmed_interactively: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = SyncConnectionView(
        **_grant()
        .model_copy(update={"status": "error", "institution_name": institution_name})
        .model_dump(),
        guidance=None,
    )
    service = MagicMock()
    service.list_connections.return_value = [
        selected,
        selected.model_copy(update={"id": str(UUID(int=101)), "status": "active"}),
    ]
    service.link.return_value = LinkResult(
        provider_item_id="", institution_name=institution_name
    )
    service.initiate_link.return_value = LinkInitiateResponse(
        session_id=str(UUID(int=500)),
        link_url="https://broker.invalid/claim",
        link_type="widget_flow",
        expiration=datetime(2026, 10, 1, tzinfo=UTC),
    )
    monkeypatch.setattr(
        cli_sync, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    terminal = replace(cli_sync.get_terminal_policy(), interactive=True)
    monkeypatch.setattr(cli_sync, "get_terminal_policy", lambda: terminal)
    confirm = MagicMock(return_value=True)
    monkeypatch.setattr(cli_sync.typer, "confirm", confirm)
    arguments = ["sync", "link", "--no-pull", "--output", output]
    # JSON never prompts, so it always needs the explicit confirmation flag.
    if not confirmed_interactively or output == "json":
        arguments.append("--yes")
    result = CliRunner().invoke(app, arguments)
    assert result.exit_code == 0, result.output
    called = service.initiate_link if output == "json" else service.link
    assert called.call_args.kwargs["connection_id"] == selected.id
    assert called.call_args.kwargs["provider"] == "simplefin"
    assert called.call_args.kwargs["institution"] is None
    if confirmed_interactively and output == "text":
        assert selected.id in confirm.call_args.args[0]


@pytest.mark.parametrize(
    "change", ["order", "labels", "generation", "scope", "account"]
)
def test_mcp_disconnect_confirmation_uses_native_identity_set(change: str) -> None:
    grant = _grant()
    live = grant.model_copy(deep=True)
    if change == "order":
        live.accounts.reverse()
    elif change == "labels":
        live.accounts[0].name = "Renamed checking"
    elif change == "generation":
        live.credential_generation += 1
    elif change == "scope":
        live.accounts[0] = SyncAccountScope.model_validate({
            **live.accounts[0].model_dump(),
            "native_scope": {"kind": "simplefin_v2_connection", "conn_id": "other"},
        })
    else:
        live.accounts.pop()
    before = mcp_sync._sync_disconnect_binding(grant)  # pyright: ignore[reportPrivateUsage]  # confirmation contract regression
    after = mcp_sync._sync_disconnect_binding(live)  # pyright: ignore[reportPrivateUsage]  # confirmation contract regression
    assert (before == after) is (change in {"order", "labels"})


@pytest.mark.parametrize(
    "change",
    [
        "duplicate_added",
        "duplicate_removed",
        "account_added",
        "account_removed",
        "scope",
        "generation",
    ],
)
async def test_mcp_disconnect_token_binds_distinct_native_accounts(
    db: Database, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from tests.moneybin.test_services.test_simplefin_sync_service import (
        _client,  # pyright: ignore[reportPrivateUsage]  # synthetic broker boundary
        _envelope,  # pyright: ignore[reportPrivateUsage]  # synthetic broker boundary
        _service,  # pyright: ignore[reportPrivateUsage]  # real sync service
    )

    preview = _grant()
    live = preview.model_copy(deep=True)
    if change == "duplicate_added":
        live.accounts.append(live.accounts[0].model_copy(deep=True))
    elif change == "duplicate_removed":
        preview.accounts.append(preview.accounts[0].model_copy(deep=True))
    elif change == "account_added":
        live.accounts.append(
            live.accounts[0].model_copy(update={"account_id": "new-native-account"})
        )
    elif change == "account_removed":
        live.accounts.pop()
    elif change == "scope":
        live.accounts[0] = SyncAccountScope.model_validate({
            **live.accounts[0].model_dump(),
            "native_scope": {"kind": "simplefin_v2_connection", "conn_id": "other"},
        })
    else:
        live.credential_generation += 1
    client = _client(_envelope())
    client.list_institutions.side_effect = [[preview], [live]]
    client.disconnect.return_value = SyncDisconnectResponse(
        connection_id=UUID(CONNECTION),
        credential_generation=live.credential_generation,
        remote_revocation="user_action_required",
        accounts=live.accounts,
    )
    service = _service(db, client)
    monkeypatch.setattr(mcp_sync, "_build_sync_service", lambda: nullcontext(service))

    required = await mcp_sync.sync_disconnect(connection_id=CONNECTION)
    assert required.error is not None
    assert required.error.code == "mutation_confirmation_required"
    token = str(required.error.details["confirmation_token"])
    response = await mcp_sync.sync_disconnect(
        connection_id=CONNECTION, confirmation_token=token
    )

    if change in {"duplicate_added", "duplicate_removed"}:
        assert response.error is None, response.to_dict()
        assert response.data.status == "disconnected"
        client.disconnect.assert_called_once_with(
            CONNECTION, credential_generation=2, accounts=live.accounts
        )
    else:
        assert response.error is not None
        assert response.error.code == "mutation_confirmation_mismatch"
        client.disconnect.assert_not_called()


@pytest.mark.parametrize(
    "change", ["order", "labels", "generation", "scope", "account"]
)
def test_cli_disconnect_confirmation_uses_native_identity_set(
    change: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    grant = _grant()
    live = grant.model_copy(deep=True)
    if change == "order":
        live.accounts.reverse()
    elif change == "labels":
        live.accounts[0].name = "Renamed checking"
    elif change == "generation":
        live.credential_generation += 1
    elif change == "scope":
        live.accounts[0] = SyncAccountScope.model_validate({
            **live.accounts[0].model_dump(),
            "native_scope": {"kind": "simplefin_v2_connection", "conn_id": "other"},
        })
    else:
        live.accounts.pop()
    service = MagicMock()
    service.plan_disconnect.return_value = grant
    deleted = False

    def confirmed(
        *, connection_id: str, verify: Callable[[ConnectedInstitution], None]
    ) -> SyncDisconnectResult:
        nonlocal deleted
        assert connection_id == grant.id
        verify(live)
        deleted = True
        return SyncDisconnectResult(
            **live.model_dump(), remote_revocation="user_action_required"
        )

    service.disconnect_confirmed.side_effect = confirmed
    monkeypatch.setattr(
        cli_sync, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    terminal = replace(cli_sync.get_terminal_policy(), interactive=True)
    monkeypatch.setattr(cli_sync, "get_terminal_policy", lambda: terminal)
    monkeypatch.setattr(cli_sync.typer, "confirm", MagicMock(return_value=True))
    result = CliRunner().invoke(
        app, ["sync", "disconnect", "--connection-id", grant.id]
    )
    allowed = change in {"order", "labels"}
    assert result.exit_code == (0 if allowed else 1), result.output
    assert deleted is allowed
    if not allowed:
        assert "Connection changed after confirmation" in result.output


def test_cli_grant_disconnect_reports_user_action_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.connectors.sync_models import SyncDisconnectResult

    service = MagicMock()
    service.disconnect.return_value = SyncDisconnectResult(
        **_grant().model_dump(), remote_revocation="user_action_required"
    )
    monkeypatch.setattr(
        cli_sync, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    result = CliRunner().invoke(
        app,
        [
            "sync",
            "disconnect",
            "--connection-id",
            CONNECTION,
            "--yes",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)
    assert body["data"]["remote_revocation"] == "user_action_required"
    assert body["data"]["connection_id"] == CONNECTION
    assert any("Bridge" in action for action in body["actions"])
    service.disconnect.assert_called_once_with(
        institution=None, provider_item_id=None, connection_id=CONNECTION
    )


def test_profile_deletion_explains_surviving_remote_grants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.cli.commands import profile

    service = MagicMock()
    monkeypatch.setattr(profile, "ProfileService", MagicMock(return_value=service))
    result = CliRunner().invoke(profile.app, ["delete", "synthetic", "--yes"])
    assert result.exit_code == 0, result.output
    assert "remote" in result.output.lower() and "Bridge" in result.output
    service.delete.assert_called_once_with("synthetic")


@pytest.mark.parametrize("surface", ["cli", "mcp"])
async def test_failed_plaid_remote_revocation_never_directs_to_simplefin_bridge(
    surface: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import AsyncMock

    from moneybin.connectors.sync_models import SyncDisconnectResult

    grant = _grant().model_copy(
        update={"provider": "plaid", "provider_item_id": "synthetic-plaid"}
    )
    result = SyncDisconnectResult(
        **grant.model_dump(), remote_revocation="user_action_required"
    )
    service = MagicMock()
    service.disconnect.return_value = result
    service.disconnect_confirmed.return_value = result
    service.plan_disconnect.return_value = grant
    module = cli_sync if surface == "cli" else mcp_sync
    monkeypatch.setattr(
        module, "_build_sync_service", MagicMock(return_value=nullcontext(service))
    )
    if surface == "cli":
        response = CliRunner().invoke(
            app,
            [
                "sync",
                "disconnect",
                "--connection-id",
                CONNECTION,
                "--yes",
                "--output",
                "json",
            ],
        )
        assert response.exit_code == 0, response.output
        body = json.loads(response.stdout)
    else:
        monkeypatch.setattr(
            mcp_sync, "grant_confirmation_or_raise", AsyncMock(return_value=MagicMock())
        )
        body = (await mcp_sync.sync_disconnect(connection_id=CONNECTION)).to_dict()
    assert body["data"]["remote_revocation"] == "user_action_required"
    guidance = " ".join(body["actions"])
    assert "Bridge" not in guidance and "SimpleFIN" not in guidance
    assert "Plaid" in guidance or "institution" in guidance.lower()


def test_disconnect_projects_actual_confirmed_response_accounts(db: Database) -> None:
    from moneybin.connectors.sync_models import SyncDisconnectResponse
    from tests.moneybin.test_services.test_simplefin_sync_service import (
        _client,  # pyright: ignore[reportPrivateUsage]  # shared synthetic broker fixture
        _envelope,  # pyright: ignore[reportPrivateUsage]  # shared synthetic job fixture
        _service,  # pyright: ignore[reportPrivateUsage]  # shared synthetic service fixture
    )

    grant = _grant()
    returned = [
        account.model_copy(update={"name": "Current safe label"})
        for account in grant.accounts
    ]
    broker = _client(_envelope())
    broker.list_institutions.return_value = [grant]
    broker.disconnect.return_value = SyncDisconnectResponse.model_validate({
        "connection_id": CONNECTION,
        "credential_generation": 2,
        "accounts": [account.model_dump() for account in returned],
        "remote_revocation": "user_action_required",
    })
    result = _service(db, broker).disconnect(connection_id=CONNECTION)
    assert result.accounts == returned
    assert broker.disconnect.call_args.kwargs["credential_generation"] == 2
    assert broker.disconnect.call_args.kwargs["accounts"] == grant.accounts
