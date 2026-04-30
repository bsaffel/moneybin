"""Tests for the leaf logs CLI command.

These tests cover the leaf logs command. The previous group structure
(tail/clean/path) is gone — see test_cli_logs_leaf.py for shape tests.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.logs import logs_command_app
from moneybin.utils.parsing import parse_duration

runner = CliRunner()


class TestParseDuration:
    """Tests for the parse_duration helper function."""

    def test_parse_days(self) -> None:
        """parse_duration parses day strings correctly."""
        assert parse_duration("30d") == timedelta(days=30)
        assert parse_duration("7d") == timedelta(days=7)
        assert parse_duration("1d") == timedelta(days=1)

    def test_parse_hours(self) -> None:
        """parse_duration parses hour strings correctly."""
        assert parse_duration("24h") == timedelta(hours=24)
        assert parse_duration("1h") == timedelta(hours=1)

    def test_parse_minutes(self) -> None:
        """parse_duration parses minute strings correctly."""
        assert parse_duration("60m") == timedelta(minutes=60)
        assert parse_duration("5m") == timedelta(minutes=5)

    def test_parse_invalid_format_raises(self) -> None:
        """parse_duration raises ValueError for invalid formats."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("30")

    def test_parse_invalid_unit_raises(self) -> None:
        """parse_duration raises ValueError for unknown units."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("30s")

    def test_parse_empty_raises(self) -> None:
        """parse_duration raises ValueError for empty string."""
        with pytest.raises(ValueError, match="Invalid duration format"):
            parse_duration("")


class TestLogsPrintPath:
    """Tests for the --print-path flag."""

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_print_path_prints_log_dir(self, mock_settings: MagicMock) -> None:
        """`logs --print-path` prints the log directory."""
        mock_settings.return_value.logging.log_file_path = Path(
            "/fake/profiles/alice/logs/moneybin.log"
        )
        result = runner.invoke(logs_command_app, ["--print-path"])
        assert result.exit_code == 0
        assert "/fake/profiles/alice/logs" in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_print_path_does_not_include_filename(
        self, mock_settings: MagicMock
    ) -> None:
        """`logs --print-path` prints the directory, not the log file itself."""
        mock_settings.return_value.logging.log_file_path = Path(
            "/fake/profiles/alice/logs/moneybin.log"
        )
        result = runner.invoke(logs_command_app, ["--print-path"])
        assert result.exit_code == 0
        assert "moneybin.log" not in result.output


class TestLogsPrune:
    """Tests for the --prune flag."""

    def test_prune_with_dry_run(self, tmp_path: Path) -> None:
        """`logs --prune --dry-run` shows what would be deleted."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "old.log").write_text("old log")

        with patch("moneybin.cli.commands.logs.get_settings") as mock:
            mock.return_value.logging.log_file_path = log_dir / "moneybin.log"
            result = runner.invoke(
                logs_command_app, ["--prune", "--older-than", "30d", "--dry-run"]
            )
            assert result.exit_code == 0

    def test_prune_deletes_old_files(self, tmp_path: Path) -> None:
        """`logs --prune` deletes files older than the cutoff."""
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
            result = runner.invoke(logs_command_app, ["--prune", "--older-than", "30d"])
            assert result.exit_code == 0
            assert not old_log.exists()

    def test_prune_keeps_recent_files(self, tmp_path: Path) -> None:
        """`logs --prune` keeps files newer than the cutoff."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        recent_log = log_dir / "recent.log"
        recent_log.write_text("recent log content")

        with patch("moneybin.cli.commands.logs.get_settings") as mock:
            mock.return_value.logging.log_file_path = log_dir / "moneybin.log"
            result = runner.invoke(logs_command_app, ["--prune", "--older-than", "30d"])
            assert result.exit_code == 0
            assert recent_log.exists()

    def test_prune_no_log_dir(self, tmp_path: Path) -> None:
        """`logs --prune` handles missing log directory gracefully."""
        log_dir = tmp_path / "nonexistent_logs"

        with patch("moneybin.cli.commands.logs.get_settings") as mock:
            mock.return_value.logging.log_file_path = log_dir / "moneybin.log"
            result = runner.invoke(logs_command_app, ["--prune", "--older-than", "7d"])
            assert result.exit_code == 0

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_prune_invalid_duration(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """`logs --prune` exits non-zero for invalid duration."""
        mock_settings.return_value.logging.log_file_path = (
            tmp_path / "logs" / "moneybin.log"
        )
        result = runner.invoke(logs_command_app, ["--prune", "--older-than", "invalid"])
        assert result.exit_code != 0

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_prune_missing_older_than(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """`logs --prune` requires --older-than."""
        mock_settings.return_value.logging.log_file_path = (
            tmp_path / "logs" / "moneybin.log"
        )
        result = runner.invoke(logs_command_app, ["--prune"])
        assert result.exit_code != 0


class TestLogsView:
    """Tests for `logs <stream>` viewing."""

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_view_shows_last_n_lines(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """`logs cli --lines 5` shows the last 5 lines of the most recent file."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        log_file = log_dir / "cli_2026-04-21.log"
        lines = [f"line {i}\n" for i in range(50)]
        log_file.write_text("".join(lines))
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(logs_command_app, ["cli", "--lines", "5"])
        assert result.exit_code == 0
        output_lines = [ln for ln in result.output.strip().split("\n") if ln]
        assert len(output_lines) == 5
        assert "line 49" in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_view_default_lines(self, mock_settings: MagicMock, tmp_path: Path) -> None:
        """`logs cli` defaults to 20 lines."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        log_file = log_dir / "cli_2026-04-21.log"
        lines = [f"line {i}\n" for i in range(30)]
        log_file.write_text("".join(lines))
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(logs_command_app, ["cli"])
        assert result.exit_code == 0
        output_lines = [ln for ln in result.output.strip().split("\n") if ln]
        assert len(output_lines) == 20

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_view_stream_selects_correct_files(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """`logs <stream>` selects files matching the stream prefix."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "mcp_2026-04-21.log").write_text(
            "INFO mcp server started\nINFO mcp tool called\n"
        )
        (log_dir / "cli_2026-04-21.log").write_text("INFO general info\n")
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(logs_command_app, ["mcp"])
        assert result.exit_code == 0
        assert "mcp server started" in result.output
        assert "mcp tool called" in result.output
        assert "general info" not in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_view_missing_log_dir(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """`logs <stream>` handles missing log directory gracefully."""
        mock_settings.return_value.logging.log_file_path = (
            tmp_path / "nonexistent" / "moneybin.log"
        )
        result = runner.invoke(logs_command_app, ["cli"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_view_fewer_lines_than_requested(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """`logs <stream>` shows all lines when file has fewer than requested."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "cli_2026-04-21.log").write_text("line 1\nline 2\nline 3\n")
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(logs_command_app, ["cli", "--lines", "20"])
        assert result.exit_code == 0
        assert "line 1" in result.output
        assert "line 2" in result.output
        assert "line 3" in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_single_stream_excludes_others(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """`logs cli` excludes mcp and sqlmesh entries."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        (log_dir / "cli_2026-04-21.log").write_text(
            "2026-04-21 14:00:00,000 - cli - INFO - from cli\n"
        )
        (log_dir / "mcp_2026-04-21.log").write_text(
            "2026-04-21 14:00:01,000 - mcp - INFO - from mcp\n"
        )
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(
            logs_command_app, ["cli", "--output", "json", "-n", "50"]
        )
        assert result.exit_code == 0
        entries: list[dict[str, str]] = json.loads(result.output)
        messages = [e["message"] for e in entries]
        assert "from cli" in messages
        assert "from mcp" not in messages


def _make_structured_log(tmp_path: Path) -> Path:
    """Create a log file with structured log lines for filter tests."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "cli_2026-04-21.log"
    log_file.write_text(
        "2026-04-21 14:00:00,000 - moneybin.loader - INFO - Loaded 50 records\n"
        "2026-04-21 14:00:01,000 - moneybin.loader - DEBUG - Cache hit for key abc\n"
        "2026-04-21 14:00:02,000 - moneybin.loader - WARNING - ⚠️  Duplicate record\n"
        "2026-04-21 14:00:03,000 - moneybin.loader - ERROR - ❌ File not found: x.csv\n"
        "Traceback (most recent call last):\n"
        '  File "loader.py", line 42, in load\n'
        "FileNotFoundError: x.csv\n"
        "2026-04-21 14:00:04,000 - moneybin.db - INFO - Query completed in 0.5s\n"
        "2026-04-21 14:00:05,000 - moneybin.db - CRITICAL - Database corrupted\n"
    )
    return log_dir


class TestLogsViewFilters:
    """Tests for view-mode filtering (--level, --since, --grep, --output)."""

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_level_filter_error(self, mock_settings: MagicMock, tmp_path: Path) -> None:
        """--level ERROR shows only ERROR and CRITICAL entries."""
        log_dir = _make_structured_log(tmp_path)
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(
            logs_command_app, ["cli", "--level", "ERROR", "-n", "50"]
        )
        assert result.exit_code == 0
        assert "File not found" in result.output
        assert "Database corrupted" in result.output
        assert "Loaded 50 records" not in result.output
        assert "Duplicate record" not in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_level_filter_includes_traceback(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """--level ERROR includes traceback continuation lines."""
        log_dir = _make_structured_log(tmp_path)
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(
            logs_command_app, ["cli", "--level", "ERROR", "-n", "50"]
        )
        assert result.exit_code == 0
        assert "Traceback" in result.output
        assert "FileNotFoundError" in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_level_filter_warning(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """--level WARNING shows WARNING, ERROR, and CRITICAL."""
        log_dir = _make_structured_log(tmp_path)
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(
            logs_command_app, ["cli", "--level", "WARNING", "-n", "50"]
        )
        assert result.exit_code == 0
        assert "Duplicate record" in result.output
        assert "File not found" in result.output
        assert "Database corrupted" in result.output
        assert "Loaded 50 records" not in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_grep_filter(self, mock_settings: MagicMock, tmp_path: Path) -> None:
        """--grep filters messages by regex pattern."""
        log_dir = _make_structured_log(tmp_path)
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(
            logs_command_app, ["cli", "--grep", "records|Query", "-n", "50"]
        )
        assert result.exit_code == 0
        assert "Loaded 50 records" in result.output
        assert "Query completed" in result.output
        assert "Duplicate record" not in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_grep_matches_traceback(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """--grep matches against traceback lines too."""
        log_dir = _make_structured_log(tmp_path)
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(
            logs_command_app, ["cli", "--grep", "FileNotFoundError", "-n", "50"]
        )
        assert result.exit_code == 0
        assert "File not found" in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_output_json(self, mock_settings: MagicMock, tmp_path: Path) -> None:
        """--output json returns structured JSON array."""
        log_dir = _make_structured_log(tmp_path)
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(
            logs_command_app, ["cli", "--output", "json", "-n", "50"]
        )
        assert result.exit_code == 0
        entries: list[dict[str, str]] = json.loads(result.output)
        assert isinstance(entries, list)
        assert len(entries) == 6
        assert entries[0]["level"] == "INFO"
        assert entries[0]["logger"] == "moneybin.loader"
        assert "timestamp" in entries[0]
        assert "message" in entries[0]

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_output_json_with_traceback(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """--output json includes traceback field for error entries."""
        log_dir = _make_structured_log(tmp_path)
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(
            logs_command_app,
            ["cli", "--output", "json", "--level", "ERROR", "-n", "50"],
        )
        assert result.exit_code == 0
        entries = json.loads(result.output)
        error_entry = entries[0]
        assert error_entry["level"] == "ERROR"
        assert "traceback" in error_entry
        assert "FileNotFoundError" in error_entry["traceback"]

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_combined_filters(self, mock_settings: MagicMock, tmp_path: Path) -> None:
        """--level and --grep can be combined."""
        log_dir = _make_structured_log(tmp_path)
        mock_settings.return_value.logging.log_file_path = log_dir / "moneybin.log"

        result = runner.invoke(
            logs_command_app,
            ["cli", "--level", "ERROR", "--grep", "corrupted", "-n", "50"],
        )
        assert result.exit_code == 0
        assert "Database corrupted" in result.output
        assert "File not found" not in result.output

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_invalid_level_exits_non_zero(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """--level with an invalid value exits non-zero."""
        mock_settings.return_value.logging.log_file_path = (
            tmp_path / "logs" / "moneybin.log"
        )
        result = runner.invoke(logs_command_app, ["cli", "--level", "BOGUS"])
        assert result.exit_code != 0

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_invalid_grep_regex_exits_non_zero(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """--grep with invalid regex exits non-zero."""
        mock_settings.return_value.logging.log_file_path = (
            tmp_path / "logs" / "moneybin.log"
        )
        result = runner.invoke(logs_command_app, ["cli", "--grep", "[invalid"])
        assert result.exit_code != 0

    @patch("moneybin.cli.commands.logs.get_settings")
    def test_invalid_since_exits_non_zero(
        self, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """--since with invalid value exits non-zero."""
        mock_settings.return_value.logging.log_file_path = (
            tmp_path / "logs" / "moneybin.log"
        )
        result = runner.invoke(logs_command_app, ["cli", "--since", "bogus"])
        assert result.exit_code != 0


class TestParseLogLines:
    """Tests for _parse_log_lines and _filter_entries helpers."""

    def test_parse_structured_lines(self) -> None:
        """Parses standard log format into entries."""
        from moneybin.cli.commands.logs import (
            _parse_log_lines,  # pyright: ignore[reportPrivateUsage]
        )

        lines = [
            "2026-04-21 14:00:00,000 - app - INFO - hello",
            "2026-04-21 14:00:01,000 - app - ERROR - boom",
            "Traceback line 1",
            "Traceback line 2",
        ]
        entries = _parse_log_lines(lines)
        assert len(entries) == 2
        assert entries[0].level == "INFO"
        assert entries[1].level == "ERROR"
        assert entries[1].extra_lines == ["Traceback line 1", "Traceback line 2"]

    def test_parse_empty_lines(self) -> None:
        """Returns empty list for no lines."""
        from moneybin.cli.commands.logs import (
            _parse_log_lines,  # pyright: ignore[reportPrivateUsage]
        )

        assert _parse_log_lines([]) == []


class TestTailFile:
    """Unit tests for the _tail_file helper."""

    def test_empty_file(self, tmp_path: Path) -> None:
        """_tail_file returns empty list for an empty file."""
        from moneybin.cli.commands.logs import (
            _tail_file,  # pyright: ignore[reportPrivateUsage] — testing internal helper
        )

        f = tmp_path / "empty.log"
        f.write_text("")
        assert _tail_file(f, 10) == []

    def test_fewer_lines_than_requested(self, tmp_path: Path) -> None:
        """_tail_file returns all lines when file has fewer than n."""
        from moneybin.cli.commands.logs import (
            _tail_file,  # pyright: ignore[reportPrivateUsage] — testing internal helper
        )

        f = tmp_path / "short.log"
        f.write_text("a\nb\nc\n")
        result = _tail_file(f, 10)
        assert result == ["a", "b", "c"]

    def test_exact_n_lines(self, tmp_path: Path) -> None:
        """_tail_file returns last n lines from a longer file."""
        from moneybin.cli.commands.logs import (
            _tail_file,  # pyright: ignore[reportPrivateUsage] — testing internal helper
        )

        f = tmp_path / "long.log"
        lines = [f"line {i}" for i in range(100)]
        f.write_text("\n".join(lines) + "\n")
        result = _tail_file(f, 5)
        assert result == ["line 95", "line 96", "line 97", "line 98", "line 99"]

    def test_small_block_size(self, tmp_path: Path) -> None:
        """_tail_file works correctly with a small block size."""
        from moneybin.cli.commands.logs import (
            _tail_file,  # pyright: ignore[reportPrivateUsage] — testing internal helper
        )

        f = tmp_path / "small_block.log"
        f.write_text("alpha\nbeta\ngamma\ndelta\n")
        result = _tail_file(f, 2, block_size=4)
        assert result == ["gamma", "delta"]
