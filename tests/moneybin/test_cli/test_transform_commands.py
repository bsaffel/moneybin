# ruff: noqa: S101,S106
"""Tests for transform CLI commands.

Tests CLI-specific functionality: argument parsing, exit codes, error handling,
and subprocess command building. Business logic is handled by dbt directly.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transform import app


class TestTransformCommands:
    """Test CLI-specific functionality for transform commands."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.fixture
    def mock_subprocess_run(self, mocker: Any) -> MagicMock:
        """Mock subprocess.run for testing."""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "dbt run completed successfully"
        mock_result.stderr = ""
        return mocker.patch(
            "moneybin.cli.commands.transform.subprocess.run",
            return_value=mock_result,
        )

    @pytest.fixture
    def mock_subprocess_popen(self, mocker: Any) -> MagicMock:
        """Mock subprocess.Popen for testing verbose mode."""
        mock_process = MagicMock()
        mock_process.returncode = 0
        # Mock stdout as a file-like object with readline method
        mock_stdout = MagicMock()
        mock_stdout.readline.side_effect = ["dbt run completed successfully\n", ""]
        mock_process.stdout = mock_stdout
        mock_process.wait.return_value = None
        return mocker.patch(
            "moneybin.cli.commands.transform.subprocess.Popen",
            return_value=mock_process,
        )

    @pytest.fixture
    def temp_dbt_project(self, tmp_path: Path) -> Path:
        """Create a temporary dbt project directory."""
        dbt_dir = tmp_path / "dbt"
        dbt_dir.mkdir()
        return dbt_dir

    def test_run_command_argument_parsing(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        temp_dbt_project: Path,
    ) -> None:
        """Test CLI argument parsing for run command."""
        # Test default arguments
        result = runner.invoke(app, ["run", "--profiles-dir", str(temp_dbt_project)])
        assert result.exit_code == 0

        # Verify dbt command was built correctly
        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "uv"
        assert call_args[1] == "run"
        assert call_args[2] == "dbt"
        assert call_args[3] == "run"
        assert "--profiles-dir" in call_args
        assert str(temp_dbt_project) in call_args
        assert "--target" in call_args
        assert "--log-path" in call_args

        # Test with select argument (non-verbose to use subprocess.run)
        mock_subprocess_run.reset_mock()
        result = runner.invoke(
            app,
            [
                "run",
                "--profiles-dir",
                str(temp_dbt_project),
                "--select",
                "tag:ofx",
                "--full-refresh",
            ],
        )
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert "--select" in call_args
        assert "tag:ofx" in call_args
        assert "--full-refresh" in call_args

    def test_run_command_input_validation(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
    ) -> None:
        """Test input validation for run command."""
        # Test invalid select parameter (shell injection prevention)
        result = runner.invoke(
            app,
            ["run", "--select", "staging; rm -rf /"],
        )
        assert result.exit_code == 1

        # Test nonexistent profiles directory
        result = runner.invoke(
            app,
            ["run", "--profiles-dir", "/nonexistent/path"],
        )
        assert result.exit_code == 1

    def test_run_command_exit_codes(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        temp_dbt_project: Path,
    ) -> None:
        """Test CLI exit codes for run command."""
        # Success case
        mock_subprocess_run.return_value.returncode = 0
        result = runner.invoke(app, ["run", "--profiles-dir", str(temp_dbt_project)])
        assert result.exit_code == 0

        # dbt failure case
        mock_subprocess_run.return_value.returncode = 1
        result = runner.invoke(app, ["run", "--profiles-dir", str(temp_dbt_project)])
        assert result.exit_code == 1

        # FileNotFoundError case (dbt/uv not installed)
        mock_subprocess_run.reset_mock()
        mock_subprocess_run.side_effect = FileNotFoundError("uv not found")
        result = runner.invoke(app, ["run", "--profiles-dir", str(temp_dbt_project)])
        assert result.exit_code == 1

    def test_test_command_argument_parsing(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        temp_dbt_project: Path,
    ) -> None:
        """Test CLI argument parsing for test command."""
        result = runner.invoke(
            app,
            [
                "test",
                "--project-dir",
                str(temp_dbt_project),
                "--models",
                "staging",
                "--verbose",
            ],
        )
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "dbt"
        assert call_args[1] == "test"
        assert "--models" in call_args
        assert "staging" in call_args
        assert "--debug" in call_args

    def test_docs_command_argument_parsing(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        temp_dbt_project: Path,
    ) -> None:
        """Test CLI argument parsing for docs command."""
        # Test docs generate only
        result = runner.invoke(app, ["docs", "--project-dir", str(temp_dbt_project)])
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "dbt"
        assert call_args[1] == "docs"
        assert call_args[2] == "generate"

        # Test docs generate and serve
        mock_subprocess_run.reset_mock()
        # Mock both generate and serve calls
        mock_subprocess_run.side_effect = [
            MagicMock(returncode=0),  # generate
            MagicMock(returncode=0),  # serve
        ]

        result = runner.invoke(
            app,
            [
                "docs",
                "--project-dir",
                str(temp_dbt_project),
                "--serve",
                "--port",
                "8081",
            ],
        )
        assert result.exit_code == 0
        assert mock_subprocess_run.call_count == 2

    def test_compile_command_argument_parsing(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        temp_dbt_project: Path,
    ) -> None:
        """Test CLI argument parsing for compile command."""
        result = runner.invoke(
            app,
            [
                "compile",
                "--project-dir",
                str(temp_dbt_project),
                "--models",
                "marts",
            ],
        )
        assert result.exit_code == 0

        call_args = mock_subprocess_run.call_args[0][0]
        assert call_args[0] == "dbt"
        assert call_args[1] == "compile"
        assert "--models" in call_args
        assert "marts" in call_args

    def test_verbose_mode_uses_popen(
        self,
        runner: CliRunner,
        mock_subprocess_run: MagicMock,
        mock_subprocess_popen: MagicMock,
        temp_dbt_project: Path,
    ) -> None:
        """Test that verbose mode uses Popen for real-time output."""
        result = runner.invoke(
            app,
            [
                "run",
                "--profiles-dir",
                str(temp_dbt_project),
                "--verbose",
            ],
        )
        assert result.exit_code == 0

        # Verbose mode should use Popen, not subprocess.run
        mock_subprocess_popen.assert_called_once()
        # subprocess.run should not be called for verbose mode
        assert not mock_subprocess_run.called


class TestTransformCommandsIntegration:
    """Integration tests for transform CLI commands."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.mark.integration
    def test_command_error_handling(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test that CLI properly handles dbt command errors."""
        # Create minimal dbt project directory
        dbt_dir = tmp_path / "dbt"
        dbt_dir.mkdir()

        # Test compile command - should fail gracefully with proper exit code
        result = runner.invoke(
            app,
            ["compile", "--project-dir", str(dbt_dir)],
        )

        # Should fail with exit code 1 (our error handling), not crash
        # The exact exit code depends on whether dbt is installed and how it fails
        assert result.exit_code in [
            1,
            2,
        ]  # 1 = our error handling, 2 = dbt config error

        # Should not crash with unhandled exceptions
        assert "Traceback" not in result.output
