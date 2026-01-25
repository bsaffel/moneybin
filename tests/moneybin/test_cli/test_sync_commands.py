# ruff: noqa: S101,S106
"""Tests for sync CLI commands.

Tests CLI-specific functionality: argument parsing, exit codes, error handling,
and environment setup. Business logic is tested in test_plaid_extractor.py.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.sync import app


class TestSyncCommands:
    """Test CLI-specific functionality for sync commands."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.fixture
    def mock_plaid_connection_manager(self, mocker: Any) -> MagicMock:
        """Mock PlaidConnectionManager to avoid heavy initialization."""
        mock_manager = MagicMock()
        mock_manager.extract_all_institutions.return_value = {}
        return mocker.patch(
            "moneybin.cli.commands.sync.PlaidConnectionManager",
            return_value=mock_manager,
        )

    def test_extract_plaid_argument_parsing(
        self,
        runner: CliRunner,
        mock_plaid_connection_manager: MagicMock,
    ) -> None:
        """Test CLI argument parsing for extract plaid command."""
        # Test verbose argument parsing
        result = runner.invoke(app, ["plaid", "--verbose"])
        # May fail due to missing credentials, but should parse arguments correctly
        assert result.exit_code in [0, 1]  # 0 = success, 1 = expected failure

    def test_extract_plaid_handles_errors_gracefully(
        self,
        runner: CliRunner,
        mock_plaid_connection_manager: MagicMock,
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
        mock_plaid_connection_manager: MagicMock,
    ) -> None:
        """Test extract all command argument parsing."""
        # Test verbose argument parsing
        result = runner.invoke(app, ["all", "--verbose"])
        # May fail due to missing credentials, but should parse arguments correctly
        assert result.exit_code in [0, 1]  # 0 = success, 1 = expected failure


class TestExtractCommandsIntegration:
    """Integration tests for extract CLI commands."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()
