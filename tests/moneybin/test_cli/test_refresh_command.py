"""Unit tests for the `moneybin refresh` CLI command."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.mcp.adapters.refresh_adapters import REFRESH_CATEGORIZE_FOLLOWUP_HINT


@pytest.fixture
def runner() -> CliRunner:
    """Return a Typer/Click CliRunner with split streams."""
    return CliRunner()


def test_refresh_json_success(runner: CliRunner) -> None:
    """JSON output on success emits envelope with applied=true and no actions."""
    fake_result = MagicMock(applied=True, duration_seconds=4.2, error=None)
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--output", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["applied"] is True
    assert payload["data"]["duration_seconds"] == 4.2
    assert payload["data"].get("error") is None
    assert payload["actions"] == []


def test_refresh_json_failure_includes_action_hint(runner: CliRunner) -> None:
    """JSON output on apply failure must mirror the MCP tool's recovery hint."""
    fake_result = MagicMock(applied=False, duration_seconds=1.1, error="model boom")
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--output", "json"])

    assert result.exit_code == 1, "apply failure must exit non-zero"
    payload = json.loads(result.stdout)
    assert payload["data"]["applied"] is False
    assert payload["data"]["error"] == "model boom"
    assert payload["actions"], "apply failure must emit a recovery hint"
    assert any("transform_plan" in a for a in payload["actions"])
    assert all("moneybin_discover" not in a for a in payload["actions"])


def test_refresh_quiet_failure_exits_nonzero(runner: CliRunner) -> None:
    """Quiet mode must still exit non-zero on apply failure."""
    fake_result = MagicMock(applied=False, duration_seconds=0.5, error="boom")
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--quiet"])

    assert result.exit_code == 1


def test_refresh_text_failure_exits_nonzero(runner: CliRunner) -> None:
    """Text mode logs the error and exits non-zero on apply failure."""
    fake_result = MagicMock(applied=False, duration_seconds=0.5, error="model boom")
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh"])

    assert result.exit_code == 1


def test_refresh_step_transform_only(runner: CliRunner) -> None:
    """``--step transform`` runs only the transform step."""
    fake_result = MagicMock(applied=True, duration_seconds=0.5, error=None)
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result) as svc,
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--step", "transform"])

    assert result.exit_code == 0
    assert svc.call_args.kwargs == {"steps": ["transform"]}


def test_refresh_step_repeatable(runner: CliRunner) -> None:
    """``--step match --step categorize`` collects into a list."""
    fake_result = MagicMock(applied=False, duration_seconds=None, error=None)
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result) as svc,
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app, ["refresh", "--step", "match", "--step", "categorize"]
        )

    # applied=False with error=None (transform deliberately skipped) → exit 0.
    # The user got what they asked for; only genuine errors fail the command.
    assert result.exit_code == 0
    assert svc.call_args.kwargs == {"steps": ["match", "categorize"]}


def test_refresh_step_json_partial_cascade(runner: CliRunner) -> None:
    """``--step transform --output json`` returns the same envelope MCP returns."""
    fake_result = MagicMock(applied=True, duration_seconds=0.7, error=None)
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(
            app, ["refresh", "--step", "transform", "--output", "json"]
        )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["applied"] is True
    assert payload["data"]["duration_seconds"] == 0.7
    # No follow-up hint because transform was requested without match
    # (the hint fires only on match-without-categorize).
    assert REFRESH_CATEGORIZE_FOLLOWUP_HINT not in payload["actions"]


def test_refresh_step_match_without_categorize_emits_followup_hint(
    runner: CliRunner,
) -> None:
    """``--step match --output json`` emits the categorize follow-up hint."""
    fake_result = MagicMock(applied=False, duration_seconds=None, error=None)
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--step", "match", "--output", "json"])

    # match-only → no transform but no error → exit 0.
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert REFRESH_CATEGORIZE_FOLLOWUP_HINT in payload["actions"]


def test_refresh_unknown_step_raises_user_error(runner: CliRunner) -> None:
    """Unknown step name surfaces as a CLI error with non-zero exit."""
    with (
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--step", "bogus"])

    assert result.exit_code != 0
    assert "UNKNOWN_REFRESH_STEP" in result.output or "bogus" in result.output
