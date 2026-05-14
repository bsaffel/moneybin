"""Tests for CLI `moneybin sync login`, `moneybin sync logout`, and `moneybin sync pull`."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.connectors.sync_models import InstitutionResult, PullResult

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
