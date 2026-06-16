"""Tests for the top-level `moneybin review` command and deprecated `transactions review` alias."""

import json
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_flag(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """`moneybin review --status` returns counts including account_links_pending."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(app, ["review", "--status"])
    assert result.exit_code == 0
    out = result.output.lower()
    # All three queues should appear in text output
    assert "match" in out or "matches" in out
    assert "categori" in out
    assert "account" in out or "link" in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_json_output(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """`moneybin review --status --output json` emits a three-way envelope."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(app, ["review", "--status", "--output", "json"])
    assert result.exit_code == 0
    envelope = json.loads(result.stdout)
    payload = envelope["data"]
    assert "matches_pending" in payload
    assert "categorize_pending" in payload
    assert "account_links_pending" in payload
    assert "total" in payload
    assert payload["total"] == (
        payload["matches_pending"]
        + payload["categorize_pending"]
        + payload["account_links_pending"]
    )


def test_review_help_includes_standard_flags() -> None:
    """`moneybin review --help` shows the same flags as `transactions review`."""
    result = runner.invoke(app, ["review", "--help"])
    assert result.exit_code == 0
    out = result.output
    assert "--status" in out
    assert "--type" in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_transactions_review_alias_warns_deprecated(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """`moneybin transactions review --status` emits a deprecation warning to stderr."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    # CliRunner mixes stdout + stderr into result.output
    result = runner.invoke(app, ["transactions", "review", "--status"])
    assert result.exit_code == 0
    combined = result.output.lower()
    assert "deprecated" in combined or "moneybin review" in combined


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_transactions_review_alias_still_works(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """`moneybin transactions review --status` still runs and exits 0 (backward compat)."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(app, ["transactions", "review", "--status"])
    assert result.exit_code == 0
