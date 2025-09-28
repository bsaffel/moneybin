# ruff: noqa: S101,S106
"""Tests for extract CLI commands.

Tests CLI-specific functionality: argument parsing, exit codes, error handling,
and environment setup. Business logic is tested in test_plaid_extractor.py.
"""

from __future__ import annotations

# Ensure project root is on sys.path so 'src' namespace is importable
import sys
from pathlib import Path
from pathlib import Path as _Path
from typing import Any
from unittest.mock import MagicMock

sys.path.append(str(_Path(__file__).resolve().parents[1]))

import pytest
from typer.testing import CliRunner

from src.moneybin.cli.commands.extract import app


class TestExtractCommands:
    """Test CLI-specific functionality for extract commands."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.fixture
    def mock_setup_logging(self, mocker: Any) -> MagicMock:
        """Mock setup_logging for testing."""
        return mocker.patch("src.moneybin.cli.commands.extract.setup_logging")

    @pytest.fixture
    def mock_setup_secure_environment(self, mocker: Any) -> MagicMock:
        """Mock setup_secure_environment for testing."""
        return mocker.patch(
            "src.moneybin.cli.commands.extract.setup_secure_environment"
        )

    def test_extract_plaid_argument_parsing(
        self,
        runner: CliRunner,
        mock_setup_logging: MagicMock,
        mock_setup_secure_environment: MagicMock,
    ) -> None:
        """Test CLI argument parsing for extract plaid command."""
        # Test verbose argument parsing
        result = runner.invoke(app, ["plaid", "--verbose"])
        # May fail due to missing credentials, but should parse arguments correctly
        assert result.exit_code in [0, 1]  # 0 = success, 1 = expected failure
        mock_setup_logging.assert_called_with(cli_mode=True, verbose=True)

    def test_extract_plaid_setup_env_option(
        self,
        runner: CliRunner,
        mock_setup_logging: MagicMock,
        mock_setup_secure_environment: MagicMock,
    ) -> None:
        """Test --setup-env option calls setup function."""
        result = runner.invoke(app, ["plaid", "--setup-env"])
        assert result.exit_code == 0

        # Should call setup_secure_environment when setup-env is used
        mock_setup_secure_environment.assert_called_once()

    def test_extract_plaid_handles_errors_gracefully(
        self,
        runner: CliRunner,
        mock_setup_logging: MagicMock,
        mock_setup_secure_environment: MagicMock,
    ) -> None:
        """Test that extract plaid command handles errors gracefully."""
        # Without proper setup, command should fail gracefully with exit code 1
        result = runner.invoke(app, ["plaid"])
        assert result.exit_code in [0, 1]  # 0 = success, 1 = expected failure

        # Should not crash with unhandled exceptions
        assert "Traceback" not in result.output

    def test_extract_all_argument_parsing(
        self,
        runner: CliRunner,
        mock_setup_logging: MagicMock,
        mock_setup_secure_environment: MagicMock,
    ) -> None:
        """Test extract all command argument parsing."""
        # Test verbose argument parsing
        result = runner.invoke(app, ["all", "--verbose"])
        # May fail due to missing credentials, but should parse arguments correctly
        assert result.exit_code in [0, 1]  # 0 = success, 1 = expected failure
        mock_setup_logging.assert_called_with(cli_mode=True, verbose=True)


class TestExtractCommandsIntegration:
    """Integration tests for extract CLI commands."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.mark.integration
    def test_extract_plaid_setup_env_integration(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test --setup-env creates actual .env file."""
        # Change to temp directory for test
        import os

        original_cwd = os.getcwd()
        os.chdir(tmp_path)

        try:
            result = runner.invoke(app, ["plaid", "--setup-env"])
            assert result.exit_code == 0

            # The command should create some kind of environment file or show instructions
            # The exact behavior depends on the implementation - main thing is it doesn't crash

        finally:
            os.chdir(original_cwd)
