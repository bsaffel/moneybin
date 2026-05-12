"""Tests for the `system status` CLI command."""

import json
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


def test_system_status_help() -> None:
    result = runner.invoke(app, ["system", "status", "--help"])
    assert result.exit_code == 0
    assert "--output" in result.output
    assert "--quiet" in result.output


@patch("moneybin.cli.commands.system.get_database")
def test_system_status_text_output(mock_get_db: MagicMock) -> None:
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    # SystemService runs 3 queries + 2 service-delegated queries; mock all to 0
    mock_db.execute.return_value.fetchone.return_value = (0, None, None)

    result = runner.invoke(app, ["system", "status"])
    assert result.exit_code == 0
    out = result.output.lower()
    assert "accounts" in out
    assert "transactions" in out
    assert "matches pending" in out
    assert "uncategorized" in out


@patch("moneybin.cli.commands.system.get_database")
def test_system_status_json_output(mock_get_db: MagicMock) -> None:
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_db.execute.return_value.fetchone.return_value = (0, None, None)

    result = runner.invoke(app, ["system", "status", "--output", "json"])
    assert result.exit_code == 0
    envelope = json.loads(result.stdout)
    payload = envelope["data"]
    assert "accounts_count" in payload
    assert "transactions_count" in payload
    assert "matches_pending" in payload
    assert "categorize_pending" in payload
