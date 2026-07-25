"""Tests for the unified `transactions review` command."""

import json
import re
import shlex
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.matching_service import PENDING_MATCHES_HINT

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


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_bare_review_answers_its_own_help_text(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """`moneybin review` with no flags prints the counts its help advertises."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(app, ["review"])
    assert result.exit_code == 0
    out = result.output.lower()
    assert "not yet implemented" not in out
    assert "matches pending" in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_without_status_prints_the_count(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """`--type matches` alone selects the count, not the unbuilt walk."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(app, ["review", "--type", "matches"])
    assert result.exit_code == 0
    out = result.output.lower()
    assert "not yet implemented" not in out
    assert "matches pending" in out


def test_interactive_is_the_only_stubbed_path() -> None:
    """The unbuilt walk is reachable only by asking for it by name."""
    result = runner.invoke(app, ["review", "--interactive"])
    assert result.exit_code == 0
    assert "not yet implemented" in result.output.lower()


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.services.matching_service.MatchingService.set_status")
def test_pending_matches_hint_is_runnable(
    mock_set_status: MagicMock, mock_get_db: MagicMock
) -> None:
    """Run the command the hint prints; a usage error means we published a dead end.

    Extracts the invocation from `PENDING_MATCHES_HINT` rather than restating
    it, so editing the constant to drop `--type matches` fails here instead of
    in a user's terminal — which is how the flag went missing the first time.
    """
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    invocation = re.search(r"'moneybin (review [^']+)'", PENDING_MATCHES_HINT)
    assert invocation, f"no `moneybin review` command found in {PENDING_MATCHES_HINT!r}"
    args = [
        arg.replace("<match-id>", "deadbeefcafe")
        for arg in shlex.split(invocation.group(1))
    ]

    result = runner.invoke(app, args)
    assert result.exit_code != 2, (
        f"the hint prints `moneybin {invocation.group(1)}`, which exits 2: "
        f"{result.output}"
    )
    mock_set_status.assert_called_once()


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param(["--status", "--interactive"], id="status-and-interactive"),
        pytest.param(
            ["--status", "--type", "matches", "--confirm", "abc123"],
            id="status-and-confirm",
        ),
        pytest.param(
            ["--interactive", "--type", "matches", "--confirm-all"],
            id="interactive-and-confirm-all",
        ),
    ],
)
def test_review_modes_are_mutually_exclusive(extra: list[str]) -> None:
    """Two modes at once is a usage error, not a silent pick between them."""
    result = runner.invoke(app, ["review", *extra])
    assert result.exit_code == 2
    assert "pass only one" in result.output


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_flag(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--status returns counts of both queues without entering interactive mode."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    # count_pending: 0 rows from app.match_decisions; count_uncategorized: 0 rows
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(app, ["transactions", "review", "--status"])
    assert result.exit_code == 0
    out = result.output.lower()
    assert "match" in out or "matches" in out
    assert "categori" in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_matches(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type matches limits output to match queue count."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--type", "matches", "--status"]
    )
    assert result.exit_code == 0
    out = result.output.lower()
    assert "match" in out
    assert "categori" not in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_categorize(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type categorize limits output to categorize queue count."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--type", "categorize", "--status"]
    )
    assert result.exit_code == 0
    out = result.output.lower()
    assert "categori" in out
    assert "match" not in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_merchant_links(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type merchant-links limits output to the merchant-link queue count."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--type", "merchant-links", "--status"]
    )
    assert result.exit_code == 0
    out = result.output.lower()
    assert "merchant-link" in out
    assert "matches pending" not in out
    assert "uncategorized" not in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_type_filter_security_links(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type security-links limits output to the security-link queue count."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--type", "security-links", "--status"]
    )
    assert result.exit_code == 0
    out = result.output.lower()
    assert "security-link" in out
    assert "matches pending" not in out
    assert "uncategorized" not in out


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_json_type_filter_merchant_links(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type merchant-links --output json includes only the merchant_links key."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app,
        [
            "transactions",
            "review",
            "--type",
            "merchant-links",
            "--status",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)["data"]
    assert "merchant_links_pending" in payload
    assert "account_links_pending" not in payload
    assert "total" not in payload


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_json_output(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--status --output json emits a structured envelope."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app, ["transactions", "review", "--status", "--output", "json"]
    )
    assert result.exit_code == 0
    envelope = json.loads(result.stdout)
    payload = envelope["data"]
    assert payload == {
        "matches_pending": 0,
        "categorize_pending": 0,
        "account_links_pending": 0,
        "merchant_links_pending": 0,
        "security_links_pending": 0,
        "total": 0,
    }


@patch("moneybin.cli.commands.transactions.review.get_database")
@patch("moneybin.config.get_settings")
def test_review_status_json_type_filter(
    mock_get_settings: MagicMock, mock_get_db: MagicMock
) -> None:
    """--type matches --output json includes only the matches key."""
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_get_settings.return_value = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = (0,)

    result = runner.invoke(
        app,
        ["transactions", "review", "--type", "matches", "--status", "--output", "json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)["data"]
    assert "matches_pending" in payload
    assert "categorize_pending" not in payload
    assert "total" not in payload
