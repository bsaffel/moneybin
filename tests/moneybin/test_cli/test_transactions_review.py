"""Tests for the unified `transactions review` command."""

import json
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


def test_review_help_lists_options() -> None:
    result = runner.invoke(app, ["transactions", "review", "--help"])
    assert result.exit_code == 0
    out = result.output
    assert "--type" in out
    assert "--status" in out
    assert "--confirm" in out
    assert "--reject" in out


def test_review_type_invalid() -> None:
    result = runner.invoke(
        app, ["transactions", "review", "--type", "bogus", "--status"]
    )
    assert result.exit_code != 0


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_flag(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--status returns counts of both queues without entering interactive mode."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    # count_pending: 0 rows from app.match_decisions; count_uncategorized: 0 rows
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(app, ["transactions", "review", "--status"])
    assert result.exit_code == 0
    out = result.output.lower()
    assert "match" in out or "matches" in out
    assert "categori" in out


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_matches(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type matches limits output to match queue count."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--type", "matches", "--status"]
    )
    assert result.exit_code == 0
    out = result.output.lower()
    assert "match" in out
    assert "categori" not in out


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_categorize(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type categorize limits output to categorize queue count."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--type", "categorize", "--status"]
    )
    assert result.exit_code == 0
    out = result.output.lower()
    assert "categori" in out
    assert "match" not in out


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_json_output(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--status --output json emits a structured envelope."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--status", "--output", "json"]
    )
    assert result.exit_code == 0
    envelope = json.loads(result.stdout)
    payload = envelope["status"]
    assert payload == {
        "matches_pending": 0,
        "categorize_pending": 0,
        "total": 0,
    }


@patch("moneybin.cli.utils.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_json_type_filter(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type matches --output json includes only the matches key."""
    mock_db = MagicMock()
    mock_get_db.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app,
        ["transactions", "review", "--type", "matches", "--status", "--output", "json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)["status"]
    assert "matches_pending" in payload
    assert "categorize_pending" not in payload
    assert "total" not in payload
