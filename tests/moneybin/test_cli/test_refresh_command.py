"""Unit tests for the `moneybin refresh` CLI command."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.mcp.adapters.refresh_adapters import REFRESH_CATEGORIZE_FOLLOWUP_HINT
from moneybin.services.refresh import RefreshResult


@pytest.fixture
def runner() -> CliRunner:
    """Return a Typer/Click CliRunner with split streams."""
    return CliRunner()


def test_refresh_json_success(runner: CliRunner) -> None:
    """JSON output on success emits envelope with applied=true and no actions."""
    fake_result = RefreshResult(applied=True, duration_seconds=4.2, error=None)
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
    # self_heal_actions is always emitted (empty until the safelist lands) so
    # agents see a stable key — guard that "always-present" contract.
    assert payload["data"]["self_heal_actions"] == []
    assert payload["actions"] == []


def test_refresh_json_failure_includes_action_hint(runner: CliRunner) -> None:
    """JSON output on apply failure must mirror the MCP tool's recovery hint."""
    fake_result = RefreshResult(applied=False, duration_seconds=1.1, error="model boom")
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
    assert any("moneybin transform plan" in a for a in payload["actions"])
    assert all("moneybin_discover" not in a for a in payload["actions"])


def test_refresh_quiet_failure_exits_nonzero(runner: CliRunner) -> None:
    """Quiet mode must still exit non-zero on apply failure."""
    fake_result = RefreshResult(applied=False, duration_seconds=0.5, error="boom")
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--quiet"])

    assert result.exit_code == 1


def test_refresh_text_failure_exits_nonzero(runner: CliRunner) -> None:
    """Text mode logs the error and exits non-zero on apply failure."""
    fake_result = RefreshResult(applied=False, duration_seconds=0.5, error="model boom")
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh"])

    assert result.exit_code == 1


def test_refresh_step_transform_only(runner: CliRunner) -> None:
    """``--step transform`` runs only the transform step."""
    fake_result = RefreshResult(applied=True, duration_seconds=0.5, error=None)
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
    fake_result = RefreshResult(applied=False, duration_seconds=None, error=None)
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
    fake_result = RefreshResult(applied=True, duration_seconds=0.7, error=None)
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
    fake_result = RefreshResult(applied=False, duration_seconds=None, error=None)
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


def test_refresh_matcher_crash_surfaced_in_json(runner: CliRunner) -> None:
    """A matcher crash (best-effort) surfaces in JSON without failing the command."""
    fake_result = RefreshResult(
        applied=True, duration_seconds=2.0, matching_error="matcher boom"
    )
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--output", "json"])

    assert result.exit_code == 0  # best-effort crash doesn't fail the command
    payload = json.loads(result.stdout)
    assert payload["data"]["matching_error"] == "matcher boom"
    tools = [ra["tool"] for ra in payload["recovery_actions"]]
    assert "refresh_run" in tools
    assert "system_doctor" in tools


def test_refresh_matcher_crash_warns_in_text(runner: CliRunner) -> None:
    """A matcher crash emits a ⚠️ warning in human output, exit 0."""
    fake_result = RefreshResult(
        applied=True, duration_seconds=2.0, matching_error="matcher boom"
    )
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh"])

    assert result.exit_code == 0
    assert "Matching step failed" in result.output


def test_refresh_matcher_crash_warns_even_in_quiet(runner: CliRunner) -> None:
    """--quiet suppresses ✅/status but NOT a best-effort step-crash warning."""
    fake_result = RefreshResult(
        applied=True, duration_seconds=2.0, matching_error="matcher boom"
    )
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh", "--quiet"])

    assert result.exit_code == 0  # best-effort crash doesn't fail the command
    assert "Matching step failed" in result.output  # warning still surfaced


def test_refresh_clean_success_keeps_check_banner(runner: CliRunner) -> None:
    """A clean run still prints the ✅ success banner (no contradictory output)."""
    fake_result = RefreshResult(applied=True, duration_seconds=2.0)
    with (
        patch("moneybin.services.refresh.refresh", return_value=fake_result),
        patch("moneybin.database.get_database") as get_db,
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        result = runner.invoke(app, ["refresh"])

    assert result.exit_code == 0
    assert "✅ Refresh complete" in result.output


def test_refresh_unknown_step_rejected_at_parse_time(runner: CliRunner) -> None:
    """Unknown step name is rejected by Typer before the service runs (exit 2)."""
    result = runner.invoke(app, ["refresh", "--step", "bogus"])
    assert result.exit_code == 2, result.output
    assert "bogus" in result.output  # Typer prints the bad value
