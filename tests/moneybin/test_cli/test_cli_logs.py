"""Tests for logs CLI commands."""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.logs import _parse_duration, app

runner = CliRunner()


class TestParseDuration:
    """Tests for the _parse_duration helper function."""

    def test_parse_days(self) -> None:
        """_parse_duration parses day strings correctly."""
        assert _parse_duration("30d") == timedelta(days=30)
        assert _parse_duration("7d") == timedelta(days=7)
        assert _parse_duration("1d") == timedelta(days=1)

    def test_parse_hours(self) -> None:
        """_parse_duration parses hour strings correctly."""
        assert _parse_duration("24h") == timedelta(hours=24)
        assert _parse_duration("1h") == timedelta(hours=1)

    def test_parse_minutes(self) -> None:
        """_parse_duration parses minute strings correctly."""
        assert _parse_duration("60m") == timedelta(minutes=60)
        assert _parse_duration("5m") == timedelta(minutes=5)

    def test_parse_invalid_format_raises(self) -> None:
        """_parse_duration raises ValueError for invalid formats."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            _parse_duration("30")

    def test_parse_invalid_unit_raises(self) -> None:
        """_parse_duration raises ValueError for unknown units."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            _parse_duration("30s")

    def test_parse_empty_raises(self) -> None:
        """_parse_duration raises ValueError for empty string."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            _parse_duration("")


class TestLogsPath:
    """Tests for the logs path command."""

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_path_prints_log_dir(self, mock_settings) -> None:
        """Logs path prints the log directory."""
        mock_settings.return_value.logging.log_file_path = Path(
            "/fake/profiles/alice/logs/moneybin.log"
        )
        result = runner.invoke(app, ["path"])
        assert result.exit_code == 0
        assert "/fake/profiles/alice/logs" in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_path_does_not_include_filename(self, mock_settings) -> None:
        """Logs path prints the directory, not the log file itself."""
        mock_settings.return_value.logging.log_file_path = Path(
            "/fake/profiles/alice/logs/moneybin.log"
        )
        result = runner.invoke(app, ["path"])
        assert result.exit_code == 0
        assert "moneybin.log" not in result.output


class TestLogsClean:
    """Tests for the logs clean command."""

    def test_clean_with_dry_run(self, tmp_path: Path) -> None:
        """Logs clean --dry-run shows what would be deleted."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "old.log").write_text("old log")

        with patch("moneybin.cli.commands.logs.get_settings") as mock:
            mock.return_value.logging.log_file_path = log_dir / "moneybin.log"
            result = runner.invoke(app, ["clean", "--older-than", "30d", "--dry-run"])
            assert result.exit_code == 0

    def test_clean_deletes_old_files(self, tmp_path: Path) -> None:
        """Logs clean deletes files older than the cutoff."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        old_log = log_dir / "old.log"
        old_log.write_text("old log content")

        # Set mtime to 60 days ago
        old_time = (datetime.now() - timedelta(days=60)).timestamp()
        import os

        os.utime(old_log, (old_time, old_time))

        with patch("moneybin.cli.commands.logs.get_settings") as mock:
            mock.return_value.logging.log_file_path = log_dir / "moneybin.log"
            result = runner.invoke(app, ["clean", "--older-than", "30d"])
            assert result.exit_code == 0
            assert not old_log.exists()

    def test_clean_keeps_recent_files(self, tmp_path: Path) -> None:
        """Logs clean keeps files newer than the cutoff."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        recent_log = log_dir / "recent.log"
        recent_log.write_text("recent log content")

        with patch("moneybin.cli.commands.logs.get_settings") as mock:
            mock.return_value.logging.log_file_path = log_dir / "moneybin.log"
            result = runner.invoke(app, ["clean", "--older-than", "30d"])
            assert result.exit_code == 0
            assert recent_log.exists()

    def test_clean_no_log_dir(self, tmp_path: Path) -> None:
        """Logs clean handles missing log directory gracefully."""
        log_dir = tmp_path / "nonexistent_logs"

        with patch("moneybin.cli.commands.logs.get_settings") as mock:
            mock.return_value.logging.log_file_path = log_dir / "moneybin.log"
            result = runner.invoke(app, ["clean", "--older-than", "7d"])
            assert result.exit_code == 0

    def test_clean_invalid_duration(self) -> None:
        """Logs clean exits with code 1 for invalid duration."""
        result = runner.invoke(app, ["clean", "--older-than", "invalid"])
        assert result.exit_code == 1

    def test_clean_missing_older_than(self) -> None:
        """Logs clean requires --older-than option."""
        result = runner.invoke(app, ["clean"])
        assert result.exit_code != 0


class TestLogsTail:
    """Tests for the logs tail command."""

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_tail_shows_last_n_lines(self, mock_settings, tmp_path: Path) -> None:
        """Logs tail shows the last N lines of the log file."""
        log_file = tmp_path / "moneybin.log"
        lines = [f"line {i}\n" for i in range(50)]
        log_file.write_text("".join(lines))
        mock_settings.return_value.logging.log_file_path = log_file

        result = runner.invoke(app, ["tail", "--lines", "5"])
        assert result.exit_code == 0
        output_lines = [ln for ln in result.output.strip().split("\n") if ln]
        assert len(output_lines) == 5
        assert "line 49" in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_tail_default_lines(self, mock_settings, tmp_path: Path) -> None:
        """Logs tail defaults to 20 lines."""
        log_file = tmp_path / "moneybin.log"
        lines = [f"line {i}\n" for i in range(30)]
        log_file.write_text("".join(lines))
        mock_settings.return_value.logging.log_file_path = log_file

        result = runner.invoke(app, ["tail"])
        assert result.exit_code == 0
        output_lines = [ln for ln in result.output.strip().split("\n") if ln]
        assert len(output_lines) == 20

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_tail_stream_filter(self, mock_settings, tmp_path: Path) -> None:
        """Logs tail --stream filters lines by stream name."""
        log_file = tmp_path / "moneybin.log"
        log_file.write_text(
            "INFO mcp server started\n"
            "INFO sqlmesh transform done\n"
            "INFO mcp tool called\n"
            "INFO general info\n"
        )
        mock_settings.return_value.logging.log_file_path = log_file

        result = runner.invoke(app, ["tail", "--stream", "mcp"])
        assert result.exit_code == 0
        assert "mcp server started" in result.output
        assert "mcp tool called" in result.output
        assert "sqlmesh transform done" not in result.output
        assert "general info" not in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_tail_missing_log_file(self, mock_settings, tmp_path: Path) -> None:
        """Logs tail handles missing log file gracefully."""
        mock_settings.return_value.logging.log_file_path = tmp_path / "nonexistent.log"
        result = runner.invoke(app, ["tail"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_tail_fewer_lines_than_requested(
        self, mock_settings, tmp_path: Path
    ) -> None:
        """Logs tail shows all lines when file has fewer than requested."""
        log_file = tmp_path / "moneybin.log"
        log_file.write_text("line 1\nline 2\nline 3\n")
        mock_settings.return_value.logging.log_file_path = log_file

        result = runner.invoke(app, ["tail", "--lines", "20"])
        assert result.exit_code == 0
        assert "line 1" in result.output
        assert "line 2" in result.output
        assert "line 3" in result.output
