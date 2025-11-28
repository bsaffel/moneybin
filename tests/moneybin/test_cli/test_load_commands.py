# ruff: noqa: S101,S106
"""Tests for load CLI commands.

Tests CLI-specific functionality: argument parsing, exit codes, error handling,
and user experience. Business logic is tested in test_parquet_loader.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.load import app


class TestLoadCommands:
    """Test CLI-specific functionality: argument parsing, exit codes, error handling."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.fixture
    def mock_parquet_loader(self, mocker: Any) -> MagicMock:
        """Mock ParquetLoader for testing CLI commands."""
        mock_loader_class = mocker.patch("moneybin.cli.commands.load.ParquetLoader")
        mock_loader = MagicMock()
        mock_loader_class.return_value = mock_loader
        return mock_loader

    @pytest.fixture
    def mock_loading_config(self, mocker: Any) -> MagicMock:
        """Mock LoadingConfig for testing CLI commands."""
        return mocker.patch("moneybin.cli.commands.load.LoadingConfig")

    @pytest.fixture
    def mock_setup_logging(self, mocker: Any) -> MagicMock:
        """Mock setup_logging for testing."""
        return mocker.patch("moneybin.cli.commands.load.setup_logging")

    @pytest.fixture
    def mock_config_functions(self, mocker: Any) -> None:
        """Mock configuration functions to return test values."""
        mocker.patch(
            "moneybin.cli.commands.load.get_raw_data_path",
            return_value=Path("data/raw"),
        )
        mocker.patch(
            "moneybin.cli.commands.load.get_database_path",
            return_value=Path("data/duckdb/testbin.duckdb"),
        )

    def test_parquet_command_argument_parsing(
        self,
        runner: CliRunner,
        mock_parquet_loader: MagicMock,
        mock_loading_config: MagicMock,
        mock_setup_logging: MagicMock,
        mock_config_functions: None,
    ) -> None:
        """Test CLI argument parsing for parquet command."""
        mock_parquet_loader.load_all_parquet_files.return_value = {}

        # Test default arguments
        result = runner.invoke(app, ["parquet"])
        assert result.exit_code == 0
        mock_loading_config.assert_called_with(
            source_path=Path("data/raw"),
            database_path=Path("data/duckdb/testbin.duckdb"),
            incremental=True,
        )

        # Test custom arguments
        mock_loading_config.reset_mock()
        result = runner.invoke(
            app,
            [
                "parquet",
                "--source",
                "custom/raw",
                "--database",
                "custom/db.duckdb",
                "--full-refresh",
                "--verbose",
            ],
        )
        assert result.exit_code == 0
        mock_loading_config.assert_called_with(
            source_path=Path("custom/raw"),
            database_path=Path("custom/db.duckdb"),
            incremental=False,
        )
        mock_setup_logging.assert_called_with(cli_mode=True, verbose=True)

    def test_parquet_command_exit_codes(
        self,
        runner: CliRunner,
        mock_parquet_loader: MagicMock,
        mock_loading_config: MagicMock,
        mock_setup_logging: MagicMock,
        mock_config_functions: None,
    ) -> None:
        """Test CLI exit codes for different error conditions."""
        # Success case
        mock_parquet_loader.load_all_parquet_files.return_value = {"table": 10}
        result = runner.invoke(app, ["parquet"])
        assert result.exit_code == 0

        # FileNotFoundError case
        mock_parquet_loader.load_all_parquet_files.side_effect = FileNotFoundError(
            "Source path does not exist"
        )
        result = runner.invoke(app, ["parquet"])
        assert result.exit_code == 1

        # General exception case
        mock_parquet_loader.load_all_parquet_files.side_effect = Exception(
            "Database connection failed"
        )
        result = runner.invoke(app, ["parquet"])
        assert result.exit_code == 1

    def test_status_command_argument_parsing(
        self,
        runner: CliRunner,
        mock_parquet_loader: MagicMock,
        mock_loading_config: MagicMock,
        mock_setup_logging: MagicMock,
        mock_config_functions: None,
    ) -> None:
        """Test CLI argument parsing for status command."""
        mock_parquet_loader.get_database_status.return_value = {}

        # Test default database path
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0
        mock_loading_config.assert_called_with(
            database_path=Path("data/duckdb/testbin.duckdb")
        )

        # Test custom database path
        mock_loading_config.reset_mock()
        result = runner.invoke(app, ["status", "--database", "custom/test.duckdb"])
        assert result.exit_code == 0
        mock_loading_config.assert_called_with(database_path=Path("custom/test.duckdb"))

    def test_status_command_exit_codes(
        self,
        runner: CliRunner,
        mock_parquet_loader: MagicMock,
        mock_loading_config: MagicMock,
        mock_setup_logging: MagicMock,
        mock_config_functions: None,
    ) -> None:
        """Test CLI exit codes for status command error conditions."""
        # Success case - must include both row_count and estimated_size
        mock_parquet_loader.get_database_status.return_value = {
            "table": {"row_count": 10, "estimated_size": 1024}
        }
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0

        # FileNotFoundError case
        mock_parquet_loader.get_database_status.side_effect = FileNotFoundError(
            "Database file does not exist"
        )
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 1

        # General exception case
        mock_parquet_loader.get_database_status.side_effect = Exception(
            "Database connection failed"
        )
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 1


class TestLoadCommandsIntegration:
    """Integration tests for CLI end-to-end workflows."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a CLI runner for testing."""
        return CliRunner()

    @pytest.mark.integration
    def test_cli_workflow_end_to_end(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test complete CLI workflow: load data then check status."""
        # Create test directory structure and data
        source_dir = tmp_path / "raw" / "plaid"
        source_dir.mkdir(parents=True)

        # Create test data
        import polars as pl

        accounts_data = pl.DataFrame({
            "account_id": ["acc1", "acc2"],
            "name": ["Account 1", "Account 2"],
        })

        parquet_file = source_dir / "accounts_20240101.parquet"
        accounts_data.write_parquet(parquet_file)

        db_path = tmp_path / "test.duckdb"

        # Test load command
        load_result = runner.invoke(
            app,
            [
                "parquet",
                "--source",
                str(tmp_path / "raw"),
                "--database",
                str(db_path),
            ],
        )
        assert load_result.exit_code == 0
        assert db_path.exists()

        # Test status command
        status_result = runner.invoke(
            app,
            ["status", "--database", str(db_path)],
        )
        assert status_result.exit_code == 0
