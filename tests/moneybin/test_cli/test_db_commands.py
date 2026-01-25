# ruff: noqa: S101,S106
"""Tests for database exploration CLI commands.

Tests CLI-specific functionality: argument parsing, exit codes, error handling,
and subprocess command building for DuckDB CLI wrapper commands.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.db import app


class TestDatabaseCommands:
    """Test CLI-specific functionality for database commands."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.fixture
    def mock_get_database_path(self, mocker: Any, tmp_path: Path) -> MagicMock:
        """Mock get_database_path to return a test database path."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()  # Create empty file
        return mocker.patch(
            "moneybin.cli.commands.db.get_database_path",
            return_value=test_db,
        )

    @pytest.fixture
    def mock_subprocess_run(self, mocker: Any) -> MagicMock:
        """Mock subprocess.run for testing."""
        return mocker.patch("moneybin.cli.commands.db.subprocess.run")

    @pytest.fixture
    def mock_duckdb_cli(self, mocker: Any) -> MagicMock:
        """Mock shutil.which to indicate DuckDB CLI is available."""
        return mocker.patch(
            "moneybin.cli.commands.db.shutil.which",
            return_value="/usr/local/bin/duckdb",
        )

    @pytest.fixture
    def mock_no_duckdb_cli(self, mocker: Any) -> MagicMock:
        """Mock shutil.which to indicate DuckDB CLI is not available."""
        return mocker.patch("moneybin.cli.commands.db.shutil.which", return_value=None)

    def test_ui_command_uses_config_database(
        self,
        runner: CliRunner,
        mock_get_database_path: MagicMock,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
    ) -> None:
        """Test that UI command uses database from config by default."""
        result = runner.invoke(app, ["ui"])
        assert result.exit_code == 0

        # Verify it used the mocked database path
        mock_get_database_path.assert_called_once()
        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "duckdb"
        assert "-ui" in call_args

    def test_ui_command_with_custom_database(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Test UI command with custom database path."""
        custom_db = tmp_path / "custom.duckdb"
        custom_db.touch()

        result = runner.invoke(app, ["ui", "--database", str(custom_db)])
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert str(custom_db) in call_args
        assert "-ui" in call_args

    def test_ui_command_database_not_found(
        self,
        runner: CliRunner,
        mock_duckdb_cli: MagicMock,
    ) -> None:
        """Test UI command fails gracefully when database doesn't exist."""
        result = runner.invoke(
            app, ["ui", "--database", "/nonexistent/database.duckdb"]
        )
        assert result.exit_code == 1

    def test_ui_command_duckdb_cli_not_installed(
        self,
        runner: CliRunner,
        mock_get_database_path: MagicMock,
        mock_no_duckdb_cli: MagicMock,
    ) -> None:
        """Test UI command fails gracefully when DuckDB CLI is not installed."""
        result = runner.invoke(app, ["ui"])
        assert result.exit_code == 1

    def test_ui_command_handles_keyboard_interrupt(
        self,
        runner: CliRunner,
        mock_get_database_path: MagicMock,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
    ) -> None:
        """Test UI command handles Ctrl+C gracefully."""
        mock_subprocess_run.side_effect = KeyboardInterrupt()

        result = runner.invoke(app, ["ui"])
        # Typer converts KeyboardInterrupt to exit code 0
        assert result.exit_code == 0

    def test_query_command_argument_parsing(
        self,
        runner: CliRunner,
        mock_get_database_path: MagicMock,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
    ) -> None:
        """Test query command builds correct DuckDB command."""
        result = runner.invoke(app, ["query", "SELECT 1"])
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "duckdb"
        assert "-c" in call_args
        assert "SELECT 1" in call_args

    def test_query_command_with_format_options(
        self,
        runner: CliRunner,
        mock_get_database_path: MagicMock,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
    ) -> None:
        """Test query command with different output formats."""
        formats = {
            "csv": "-csv",
            "json": "-json",
            "markdown": "-markdown",
            "box": "-box",
        }

        for format_name, format_flag in formats.items():
            mock_subprocess_run.reset_mock()
            result = runner.invoke(app, ["query", "SELECT 1", "--format", format_name])
            assert result.exit_code == 0

            call_args = mock_subprocess_run.call_args[0][0]
            assert format_flag in call_args

    def test_query_command_with_custom_database(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Test query command with custom database path."""
        custom_db = tmp_path / "custom.duckdb"
        custom_db.touch()

        result = runner.invoke(
            app,
            ["query", "SELECT 1", "--database", str(custom_db)],
        )
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert str(custom_db) in call_args

    def test_query_command_database_not_found(
        self,
        runner: CliRunner,
        mock_duckdb_cli: MagicMock,
    ) -> None:
        """Test query command fails when database doesn't exist."""
        result = runner.invoke(
            app,
            ["query", "SELECT 1", "--database", "/nonexistent/database.duckdb"],
        )
        assert result.exit_code == 1

    def test_query_command_duckdb_cli_not_installed(
        self,
        runner: CliRunner,
        mock_get_database_path: MagicMock,
        mock_no_duckdb_cli: MagicMock,
    ) -> None:
        """Test query command fails when DuckDB CLI is not installed."""
        result = runner.invoke(app, ["query", "SELECT 1"])
        assert result.exit_code == 1

    def test_shell_command_argument_parsing(
        self,
        runner: CliRunner,
        mock_get_database_path: MagicMock,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
    ) -> None:
        """Test shell command builds correct DuckDB command."""
        result = runner.invoke(app, ["shell"])
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "duckdb"
        # Shell mode should NOT have -c or -ui flags
        assert "-c" not in call_args
        assert "-ui" not in call_args

    def test_shell_command_with_custom_database(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Test shell command with custom database path."""
        custom_db = tmp_path / "custom.duckdb"
        custom_db.touch()

        result = runner.invoke(app, ["shell", "--database", str(custom_db)])
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert str(custom_db) in call_args

    def test_shell_command_database_not_found(
        self,
        runner: CliRunner,
        mock_duckdb_cli: MagicMock,
    ) -> None:
        """Test shell command fails when database doesn't exist."""
        result = runner.invoke(
            app, ["shell", "--database", "/nonexistent/database.duckdb"]
        )
        assert result.exit_code == 1

    def test_shell_command_duckdb_cli_not_installed(
        self,
        runner: CliRunner,
        mock_get_database_path: MagicMock,
        mock_no_duckdb_cli: MagicMock,
    ) -> None:
        """Test shell command fails when DuckDB CLI is not installed."""
        result = runner.invoke(app, ["shell"])
        assert result.exit_code == 1

    def test_shell_command_handles_keyboard_interrupt(
        self,
        runner: CliRunner,
        mock_get_database_path: MagicMock,
        mock_subprocess_run: MagicMock,
        mock_duckdb_cli: MagicMock,
    ) -> None:
        """Test shell command handles Ctrl+C gracefully."""
        mock_subprocess_run.side_effect = KeyboardInterrupt()

        result = runner.invoke(app, ["shell"])
        assert result.exit_code == 0


class TestDatabaseCommandsIntegration:
    """Integration tests for database CLI commands."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.mark.integration
    def test_commands_handle_subprocess_errors(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test that commands handle subprocess errors gracefully."""
        test_db = tmp_path / "test.duckdb"
        test_db.touch()

        # Test with a database that exists but may cause DuckDB errors
        # Should fail gracefully with proper exit code, not crash
        result = runner.invoke(
            app, ["query", "INVALID SQL", "--database", str(test_db)]
        )

        # Should fail with exit code 1, not crash with unhandled exception
        # The exact behavior depends on whether DuckDB CLI is installed
        assert result.exit_code in [0, 1]
        assert "Traceback" not in result.output
