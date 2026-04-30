"""Tests for the moneybin stats CLI command."""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.commands.stats import (
    stats_command,
)
from moneybin.cli.main import app as root_app

# Build a single-command Typer app for in-place tests of the leaf function.
# This lets the legacy filter-logic tests keep invoking through CliRunner
# without depending on the root app or its profile bootstrap.
stats_app = typer.Typer()
stats_app.command()(stats_command)


@pytest.fixture()
def runner() -> CliRunner:
    """Return a Typer CLI test runner."""
    return CliRunner()


class TestStatsLeafShape:
    """Stats is a leaf command — no `show` subcommand, bare invocation runs it."""

    @pytest.mark.unit
    def test_bare_invocation_runs_command(
        self,
        runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        # Provision a real on-disk profile dir so the root callback's
        # existence check passes; mock the DB and observability setup so
        # the test stays fast and side-effect free.
        profile_dir = tmp_path / "profiles" / "test"
        profile_dir.mkdir(parents=True)
        monkeypatch.setenv("MONEYBIN_PROFILE", "test")

        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []
        with (
            patch("moneybin.cli.utils.get_database", return_value=mock_db),
            patch("moneybin.cli.main.ensure_default_profile", return_value="test"),
            patch("moneybin.cli.main.set_current_profile"),
            patch("moneybin.cli.main.setup_observability"),
            patch("moneybin.config.get_base_dir", return_value=tmp_path),
        ):
            result = runner.invoke(root_app, ["stats"])

        assert result.exit_code == 0, result.output
        assert "No metrics" in result.stdout

    @pytest.mark.unit
    def test_show_subcommand_no_longer_exists(
        self, runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MONEYBIN_PROFILE", "test")
        result = runner.invoke(root_app, ["stats", "show"])
        assert result.exit_code != 0
        assert (
            "no such option" in result.stdout.lower()
            or "extra argument" in result.stdout.lower()
            or "unknown" in result.stdout.lower()
            or "got unexpected" in result.stdout.lower()
            or "no such option" in result.output.lower()
            or "extra argument" in result.output.lower()
            or "unknown" in result.output.lower()
            or "got unexpected" in result.output.lower()
        )


class TestStatsCommand:
    """Tests for the stats leaf command's filter and output logic."""

    @pytest.mark.unit
    def test_show_no_metrics_prints_hint(self, runner: CliRunner) -> None:
        """Should display a hint when no metrics exist."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        with patch("moneybin.cli.utils.get_database", return_value=mock_db):
            result = runner.invoke(stats_app, [])

        assert result.exit_code == 0
        assert "No metrics" in result.output

    @pytest.mark.unit
    def test_show_with_rows_displays_metrics(self, runner: CliRunner) -> None:
        """Should display formatted metric values when rows exist."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = [
            (
                "moneybin_import_records",
                "counter",
                "{}",
                42.0,
                5,
                datetime.now(),
            ),
        ]

        with patch("moneybin.cli.utils.get_database", return_value=mock_db):
            result = runner.invoke(stats_app, [])

        assert result.exit_code == 0
        assert "Import Records" in result.output

    @pytest.mark.unit
    def test_show_json_output(self, runner: CliRunner) -> None:
        """--output json should return valid JSON."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        with patch("moneybin.cli.utils.get_database", return_value=mock_db):
            result = runner.invoke(stats_app, ["--output", "json"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, dict)

    @pytest.mark.unit
    def test_show_with_since_filter(self, runner: CliRunner) -> None:
        """--since should filter metrics by time window."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        with patch("moneybin.cli.utils.get_database", return_value=mock_db):
            result = runner.invoke(stats_app, ["--since", "7d"])

        assert result.exit_code == 0

    @pytest.mark.unit
    def test_show_locked_database_exits_1(self, runner: CliRunner) -> None:
        """Should exit 1 when database key is missing."""
        from moneybin.database import DatabaseKeyError

        with patch(
            "moneybin.cli.utils.get_database",
            side_effect=DatabaseKeyError("locked"),
        ):
            result = runner.invoke(stats_app, [])

        assert result.exit_code == 1
