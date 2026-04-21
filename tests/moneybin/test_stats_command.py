"""Tests for the moneybin stats CLI command."""

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.stats import app as stats_app


@pytest.fixture()
def runner() -> CliRunner:
    """Return a Typer CLI test runner."""
    return CliRunner()


class TestStatsShow:
    """Tests for the stats show command."""

    @pytest.mark.unit
    def test_show_no_metrics_prints_hint(self, runner: CliRunner) -> None:
        """Should display a hint when no metrics exist."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        with patch("moneybin.cli.commands.stats.get_database", return_value=mock_db):
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

        with patch("moneybin.cli.commands.stats.get_database", return_value=mock_db):
            result = runner.invoke(stats_app, [])

        assert result.exit_code == 0
        assert "Import Records" in result.output

    @pytest.mark.unit
    def test_show_json_output(self, runner: CliRunner) -> None:
        """--output json should return valid JSON."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        with patch("moneybin.cli.commands.stats.get_database", return_value=mock_db):
            result = runner.invoke(stats_app, ["--output", "json"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, dict)

    @pytest.mark.unit
    def test_show_with_since_filter(self, runner: CliRunner) -> None:
        """--since should filter metrics by time window."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        with patch("moneybin.cli.commands.stats.get_database", return_value=mock_db):
            result = runner.invoke(stats_app, ["--since", "7d"])

        assert result.exit_code == 0

    @pytest.mark.unit
    def test_show_locked_database_exits_1(self, runner: CliRunner) -> None:
        """Should exit 1 and display unlock hint when database is locked."""
        from moneybin.database import DatabaseKeyError

        with patch(
            "moneybin.cli.commands.stats.get_database",
            side_effect=DatabaseKeyError("locked"),
        ):
            result = runner.invoke(stats_app, [])

        assert result.exit_code == 1
        assert "unlock" in result.output.lower()
