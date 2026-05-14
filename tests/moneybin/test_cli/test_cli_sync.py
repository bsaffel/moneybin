"""Tests for CLI `moneybin sync login` and `moneybin sync logout`."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


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
