"""Tests for CLI `moneybin sync login`, `moneybin sync logout`, `moneybin sync pull`, and connect."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.connectors.sync_models import (
    ConnectResult,
    InstitutionResult,
    PullResult,
    SyncConnectionView,
)

runner = CliRunner()


def _fake_pull_result() -> PullResult:
    return PullResult(
        job_id="job-xyz",
        transactions_loaded=10,
        accounts_loaded=2,
        balances_loaded=2,
        transactions_removed=0,
        institutions=[
            InstitutionResult(
                provider_item_id="item_chase",
                institution_name="Chase",
                status="completed",
                transaction_count=10,
            )
        ],
    )


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_client")
def test_sync_login_invokes_client_login(mock_build: MagicMock) -> None:
    mock_client = MagicMock()
    mock_build.return_value = mock_client
    result = runner.invoke(app, ["sync", "login", "--no-browser"])
    assert result.exit_code == 0, result.output
    mock_client.login.assert_called_once_with(open_browser=False)


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_client")
def test_sync_login_default_opens_browser(mock_build: MagicMock) -> None:
    mock_client = MagicMock()
    mock_build.return_value = mock_client
    result = runner.invoke(app, ["sync", "login"])
    assert result.exit_code == 0, result.output
    mock_client.login.assert_called_once_with(open_browser=True)


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_client")
def test_sync_logout_clears_tokens(mock_build: MagicMock) -> None:
    mock_client = MagicMock()
    mock_build.return_value = mock_client
    result = runner.invoke(app, ["sync", "logout"])
    assert result.exit_code == 0, result.output
    mock_client.logout.assert_called_once()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_text_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.pull.return_value = _fake_pull_result()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull"])
    assert result.exit_code == 0, result.output
    assert "Chase" in result.stdout
    assert "10" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_json_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.pull.return_value = _fake_pull_result()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull", "--output", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["job_id"] == "job-xyz"
    assert data["transactions_loaded"] == 10
    assert data["institutions"][0]["institution_name"] == "Chase"


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_with_institution_and_force(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.pull.return_value = _fake_pull_result()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull", "--institution", "Chase", "--force"])
    assert result.exit_code == 0, result.output
    service.pull.assert_called_once_with(institution="Chase", force=True)


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_connect_new_institution(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = []
    service.connect.return_value = ConnectResult(
        provider_item_id="item_new",
        institution_name="Chase",
        pull_result=_fake_pull_result(),
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "connect"])
    assert result.exit_code == 0, result.output
    service.connect.assert_called_once()
    # auto_pull defaults to True
    assert service.connect.call_args.kwargs.get("auto_pull", True) is True


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_connect_no_pull(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = []
    service.connect.return_value = ConnectResult(
        provider_item_id="item_new",
        institution_name="Chase",
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "connect", "--no-pull"])
    assert result.exit_code == 0, result.output
    service.connect.assert_called_once()
    assert service.connect.call_args.kwargs["auto_pull"] is False


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_connect_explicit_institution(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.connect.return_value = ConnectResult(
        provider_item_id="item_x",
        institution_name="Schwab",
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app, ["sync", "connect", "--institution", "Schwab", "--no-pull"]
    )
    assert result.exit_code == 0, result.output
    service.connect.assert_called_once()
    assert service.connect.call_args.kwargs["institution"] == "Schwab"


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_connect_status_command(mock_build: MagicMock) -> None:
    from moneybin.connectors.sync_client import SyncClient

    client = MagicMock(spec=SyncClient)
    client.poll_connect_status.return_value = MagicMock(
        session_id="sess_x",
        status="connected",
        provider_item_id="item_new",
        institution_name="Chase",
        model_dump_json=lambda **k: '{"status": "connected"}',  # type: ignore[misc]
    )
    # connect-status uses the client directly, not the service
    with patch("moneybin.cli.commands.sync._build_sync_client", return_value=client):
        result = runner.invoke(
            app,
            ["sync", "connect-status", "--session-id", "sess_x", "--output", "json"],
        )
    assert result.exit_code == 0, result.output
    assert "connected" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_disconnect_requires_yes_or_confirm(mock_build: MagicMock) -> None:
    service = MagicMock()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app, ["sync", "disconnect", "--institution", "Chase", "--yes"]
    )
    assert result.exit_code == 0, result.output
    service.disconnect.assert_called_once_with(institution="Chase")


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_status_text_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="active",
            last_sync=datetime(2026, 4, 7, 14, 30, tzinfo=UTC),
            guidance=None,
        ),
        SyncConnectionView(
            id="u2",
            provider_item_id="item_b",
            institution_name="Schwab",
            provider="plaid",
            status="error",
            last_sync=None,
            guidance="Schwab needs re-authentication — run `moneybin sync connect --institution Schwab`",
        ),
    ]
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "status"])
    assert result.exit_code == 0, result.output
    assert "Chase" in result.stdout
    assert "Schwab" in result.stdout
    assert "needs re-authentication" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_status_json_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="active",
            last_sync=datetime(2026, 4, 7, 14, 30, tzinfo=UTC),
            guidance=None,
        ),
    ]
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "status", "--output", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert isinstance(data, list)
    assert data[0]["institution_name"] == "Chase"
    assert data[0]["status"] == "active"
