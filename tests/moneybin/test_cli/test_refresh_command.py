"""Unit tests for the `moneybin refresh` CLI command."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app


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
