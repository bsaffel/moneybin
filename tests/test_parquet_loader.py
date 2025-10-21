# ruff: noqa: S101,S106
"""Tests for ParquetLoader.

Comprehensive unit tests for the ParquetLoader class, including configuration,
data loading operations, database status checking, and error handling.
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

from src.moneybin.loaders.parquet_loader import LoadingConfig, ParquetLoader


class TestLoadingConfig:
    """Test cases for LoadingConfig dataclass."""

    def test_default_config(self, mocker: Any) -> None:
        """Test default configuration values."""
        # Mock configuration functions to return test values
        mocker.patch(
            "src.moneybin.loaders.parquet_loader.get_raw_data_path",
            return_value=Path("data/raw"),
        )
        mocker.patch(
            "src.moneybin.loaders.parquet_loader.get_database_path",
            return_value=Path("data/duckdb/testbin.duckdb"),
        )

        config = LoadingConfig()

        assert config.source_path == Path("data/raw")
        assert config.database_path == Path("data/duckdb/testbin.duckdb")
        assert config.incremental is True
        assert config.create_database_dir is True

    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = LoadingConfig(
            source_path=Path("custom/raw"),
            database_path=Path("custom/db.duckdb"),
            incremental=False,
            create_database_dir=False,
        )

        assert config.source_path == Path("custom/raw")
        assert config.database_path == Path("custom/db.duckdb")
        assert config.incremental is False
        assert config.create_database_dir is False


class TestParquetLoader:
    """Test cases for ParquetLoader class."""

    @pytest.fixture
    def temp_config(self, tmp_path: Path) -> LoadingConfig:
        """Create a temporary configuration for testing."""
        return LoadingConfig(
            source_path=tmp_path / "raw",
            database_path=tmp_path / "test.duckdb",
            incremental=True,
            create_database_dir=True,
        )

    @pytest.fixture
    def mock_duckdb_connection(self, mocker: Any) -> MagicMock:
        """Mock DuckDB connection for testing."""
        mock_conn = MagicMock()
        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = mock_conn
        mock_context_manager.__exit__.return_value = None

        mocker.patch("duckdb.connect", return_value=mock_context_manager)
        return mock_conn

    @pytest.fixture
    def mock_config_functions(self, mocker: Any) -> None:
        """Mock configuration functions to return test values."""
        mocker.patch(
            "src.moneybin.loaders.parquet_loader.get_raw_data_path",
            return_value=Path("data/raw"),
        )
        mocker.patch(
            "src.moneybin.loaders.parquet_loader.get_database_path",
            return_value=Path("data/duckdb/testbin.duckdb"),
        )

    def test_init_with_default_config(self, mock_config_functions: None) -> None:
        """Test ParquetLoader initialization with default config."""
        loader = ParquetLoader()

        assert loader.config.source_path == Path("data/raw")
        assert loader.config.database_path == Path("data/duckdb/testbin.duckdb")
        assert loader.config.incremental is True
        assert loader.config.create_database_dir is True

    def test_init_with_custom_config(self, temp_config: LoadingConfig) -> None:
        """Test ParquetLoader initialization with custom config."""
        loader = ParquetLoader(temp_config)

        assert loader.config == temp_config

    def test_init_creates_database_directory(self, tmp_path: Path) -> None:
        """Test that database directory is created when create_database_dir is True."""
        db_path = tmp_path / "nested" / "db" / "test.duckdb"
        config = LoadingConfig(
            database_path=db_path,
            create_database_dir=True,
        )

        ParquetLoader(config)

        assert db_path.parent.exists()

    def test_init_skips_directory_creation(self, tmp_path: Path) -> None:
        """Test that database directory is not created when create_database_dir is False."""
        db_path = tmp_path / "nested" / "db" / "test.duckdb"
        config = LoadingConfig(
            database_path=db_path,
            create_database_dir=False,
        )

        ParquetLoader(config)

        assert not db_path.parent.exists()

    def test_load_all_parquet_files_source_not_exists(
        self, temp_config: LoadingConfig
    ) -> None:
        """Test load_all_parquet_files raises FileNotFoundError when source doesn't exist."""
        loader = ParquetLoader(temp_config)

        with pytest.raises(FileNotFoundError, match="Source path does not exist"):
            loader.load_all_parquet_files()

    def test_load_all_parquet_files_no_plaid_data(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test load_all_parquet_files with no Plaid data available."""
        # Create source directory but no plaid subdirectory
        temp_config.resolved_source_path.mkdir(parents=True)

        loader = ParquetLoader(temp_config)
        result = loader.load_all_parquet_files()

        assert result == {}
        mock_duckdb_connection.sql.assert_not_called()

    def test_load_all_parquet_files_with_plaid_data(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock, mocker: Any
    ) -> None:
        """Test load_all_parquet_files with Plaid data available."""
        # Create source directory and plaid subdirectory
        plaid_path = temp_config.resolved_source_path / "plaid"
        plaid_path.mkdir(parents=True)

        # Create mock parquet files
        (plaid_path / "accounts_20240101.parquet").touch()
        (plaid_path / "transactions_20240101.parquet").touch()

        # Mock the private methods
        mock_load_plaid = mocker.patch.object(
            ParquetLoader,
            "_load_plaid_data",
            return_value={"raw_plaid_accounts": 10, "raw_plaid_transactions": 100},
        )

        loader = ParquetLoader(temp_config)
        result = loader.load_all_parquet_files()

        assert result == {"raw_plaid_accounts": 10, "raw_plaid_transactions": 100}
        mock_load_plaid.assert_called_once()

    def test_get_database_status_file_not_exists(
        self, temp_config: LoadingConfig
    ) -> None:
        """Test get_database_status raises FileNotFoundError when database doesn't exist."""
        loader = ParquetLoader(temp_config)

        with pytest.raises(FileNotFoundError, match="Database file does not exist"):
            loader.get_database_status()

    def test_get_database_status_success(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test get_database_status returns correct status information."""
        # Create database file
        temp_config.resolved_database_path.touch()

        # Mock SQL queries - need to handle multiple calls
        def mock_sql_side_effect(query: str) -> MagicMock:
            mock_result = MagicMock()
            if "duckdb_tables()" in query:
                # First query: get table list
                mock_result.fetchall.return_value = [
                    ("raw_plaid_accounts", 1024),
                    ("raw_plaid_transactions", 2048),
                ]
            elif "COUNT(*)" in query:
                # Subsequent queries: get row counts
                if "raw_plaid_accounts" in query:
                    mock_result.fetchone.return_value = (10,)
                elif "raw_plaid_transactions" in query:
                    mock_result.fetchone.return_value = (100,)
            return mock_result

        mock_duckdb_connection.sql.side_effect = mock_sql_side_effect

        loader = ParquetLoader(temp_config)
        status = loader.get_database_status()

        expected = {
            "raw_plaid_accounts": {"row_count": 10, "estimated_size": 1024},
            "raw_plaid_transactions": {"row_count": 100, "estimated_size": 2048},
        }
        assert status == expected

    def test_get_database_status_invalid_table_name(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test get_database_status skips invalid table names."""
        # Create database file
        temp_config.resolved_database_path.touch()

        # Mock SQL queries - need to handle multiple calls
        def mock_sql_side_effect(query: str) -> MagicMock:
            mock_result = MagicMock()
            if "duckdb_tables()" in query:
                # First query: get table list with invalid table name
                mock_result.fetchall.return_value = [
                    ("valid_table", 1024),
                    ("invalid-table-name", 2048),  # Invalid identifier
                ]
            elif "COUNT(*)" in query and "valid_table" in query:
                # Only valid table should get count query
                mock_result.fetchone.return_value = (10,)
            return mock_result

        mock_duckdb_connection.sql.side_effect = mock_sql_side_effect

        loader = ParquetLoader(temp_config)
        status = loader.get_database_status()

        # Only valid table should be included
        expected = {
            "valid_table": {"row_count": 10, "estimated_size": 1024},
        }
        assert status == expected

    def test_load_plaid_data_no_files(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test _load_plaid_data with no Parquet files."""
        plaid_path = temp_config.resolved_source_path / "plaid"
        plaid_path.mkdir(parents=True)

        loader = ParquetLoader(temp_config)
        result = loader._load_plaid_data(mock_duckdb_connection, plaid_path)  # type: ignore[reportPrivateUsage]

        assert result == {}

    def test_load_plaid_data_with_files(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock, mocker: Any
    ) -> None:
        """Test _load_plaid_data with Parquet files."""
        plaid_path = temp_config.resolved_source_path / "plaid"
        plaid_path.mkdir(parents=True)

        # Create mock parquet files
        (plaid_path / "accounts_20240101.parquet").touch()
        (plaid_path / "transactions_20240101.parquet").touch()

        # Mock the table loading methods
        mock_load_accounts = mocker.patch.object(
            ParquetLoader, "_load_accounts_table", return_value=10
        )
        mock_load_transactions = mocker.patch.object(
            ParquetLoader, "_load_transactions_table", return_value=100
        )

        loader = ParquetLoader(temp_config)
        result = loader._load_plaid_data(mock_duckdb_connection, plaid_path)  # type: ignore[reportPrivateUsage]

        assert result == {"raw_plaid_accounts": 10, "raw_plaid_transactions": 100}
        mock_load_accounts.assert_called_once()
        mock_load_transactions.assert_called_once()

    def test_load_accounts_table_single_file_new_table(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test _load_accounts_table with single file creating new table."""
        plaid_path = temp_config.resolved_source_path / "plaid"
        plaid_path.mkdir(parents=True)
        parquet_file = plaid_path / "accounts_20240101.parquet"
        parquet_file.touch()

        # Mock table existence check (table doesn't exist)
        mock_duckdb_connection.sql.return_value.fetchone.side_effect = [
            (0,),  # Table doesn't exist
            (10,),  # Final count
        ]

        loader = ParquetLoader(temp_config)
        count = loader._load_accounts_table(mock_duckdb_connection, [parquet_file])  # type: ignore[reportPrivateUsage]

        assert count == 10
        # Verify CREATE TABLE was called
        sql_calls = [call[0][0] for call in mock_duckdb_connection.sql.call_args_list]
        assert any("CREATE TABLE raw_plaid_accounts AS" in call for call in sql_calls)

    def test_load_accounts_table_multiple_files_incremental(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test _load_accounts_table with multiple files in incremental mode."""
        plaid_path = temp_config.resolved_source_path / "plaid"
        plaid_path.mkdir(parents=True)
        parquet_files = [
            plaid_path / "accounts_20240101.parquet",
            plaid_path / "accounts_20240102.parquet",
        ]
        for file in parquet_files:
            file.touch()

        # Mock table existence check (table exists)
        mock_duckdb_connection.sql.return_value.fetchone.side_effect = [
            (1,),  # Table exists
            (15,),  # Final count
        ]

        loader = ParquetLoader(temp_config)
        count = loader._load_accounts_table(mock_duckdb_connection, parquet_files)  # type: ignore[reportPrivateUsage]

        assert count == 15
        # Verify INSERT was called for incremental loading
        sql_calls = [call[0][0] for call in mock_duckdb_connection.sql.call_args_list]
        assert any("INSERT INTO raw_plaid_accounts" in call for call in sql_calls)

    def test_load_accounts_table_full_refresh(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test _load_accounts_table with full refresh mode."""
        temp_config.incremental = False
        plaid_path = temp_config.resolved_source_path / "plaid"
        plaid_path.mkdir(parents=True)
        parquet_file = plaid_path / "accounts_20240101.parquet"
        parquet_file.touch()

        # Mock final count
        mock_duckdb_connection.sql.return_value.fetchone.return_value = (10,)

        loader = ParquetLoader(temp_config)
        count = loader._load_accounts_table(mock_duckdb_connection, [parquet_file])  # type: ignore[reportPrivateUsage]

        assert count == 10
        # Verify CREATE OR REPLACE TABLE was called
        sql_calls = [call[0][0] for call in mock_duckdb_connection.sql.call_args_list]
        assert any(
            "CREATE OR REPLACE TABLE raw_plaid_accounts AS" in call
            for call in sql_calls
        )

    def test_load_transactions_table_single_file_new_table(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test _load_transactions_table with single file creating new table."""
        plaid_path = temp_config.resolved_source_path / "plaid"
        plaid_path.mkdir(parents=True)
        parquet_file = plaid_path / "transactions_20240101.parquet"
        parquet_file.touch()

        # Mock table existence check (table doesn't exist)
        mock_duckdb_connection.sql.return_value.fetchone.side_effect = [
            (0,),  # Table doesn't exist
            (100,),  # Final count
        ]

        loader = ParquetLoader(temp_config)
        count = loader._load_transactions_table(mock_duckdb_connection, [parquet_file])  # type: ignore[reportPrivateUsage]

        assert count == 100
        # Verify CREATE TABLE was called
        sql_calls = [call[0][0] for call in mock_duckdb_connection.sql.call_args_list]
        assert any(
            "CREATE TABLE raw_plaid_transactions AS" in call for call in sql_calls
        )

    def test_load_transactions_table_multiple_files_incremental(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test _load_transactions_table with multiple files in incremental mode."""
        plaid_path = temp_config.resolved_source_path / "plaid"
        plaid_path.mkdir(parents=True)
        parquet_files = [
            plaid_path / "transactions_20240101.parquet",
            plaid_path / "transactions_20240102.parquet",
        ]
        for file in parquet_files:
            file.touch()

        # Mock table existence check (table exists)
        mock_duckdb_connection.sql.return_value.fetchone.side_effect = [
            (1,),  # Table exists
            (150,),  # Final count
        ]

        loader = ParquetLoader(temp_config)
        count = loader._load_transactions_table(mock_duckdb_connection, parquet_files)  # type: ignore[reportPrivateUsage]

        assert count == 150
        # Verify INSERT was called for incremental loading
        sql_calls = [call[0][0] for call in mock_duckdb_connection.sql.call_args_list]
        assert any("INSERT INTO raw_plaid_transactions" in call for call in sql_calls)

    def test_load_transactions_table_full_refresh(
        self, temp_config: LoadingConfig, mock_duckdb_connection: MagicMock
    ) -> None:
        """Test _load_transactions_table with full refresh mode."""
        temp_config.incremental = False
        plaid_path = temp_config.resolved_source_path / "plaid"
        plaid_path.mkdir(parents=True)
        parquet_file = plaid_path / "transactions_20240101.parquet"
        parquet_file.touch()

        # Mock final count
        mock_duckdb_connection.sql.return_value.fetchone.return_value = (100,)

        loader = ParquetLoader(temp_config)
        count = loader._load_transactions_table(mock_duckdb_connection, [parquet_file])  # type: ignore[reportPrivateUsage]

        assert count == 100
        # Verify CREATE OR REPLACE TABLE was called
        sql_calls = [call[0][0] for call in mock_duckdb_connection.sql.call_args_list]
        assert any(
            "CREATE OR REPLACE TABLE raw_plaid_transactions AS" in call
            for call in sql_calls
        )


class TestParquetLoaderIntegration:
    """Integration tests for ParquetLoader with real DuckDB operations."""

    @pytest.mark.integration
    def test_full_workflow_with_real_duckdb(self, tmp_path: Path) -> None:
        """Test complete workflow with real DuckDB database."""
        # Create test configuration
        config = LoadingConfig(
            source_path=tmp_path / "raw",
            database_path=tmp_path / "test.duckdb",
            incremental=True,
            create_database_dir=True,
        )

        # Create source directory structure
        plaid_path = config.resolved_source_path / "plaid"
        plaid_path.mkdir(parents=True)

        # Create a simple test parquet file with accounts data
        import polars as pl

        accounts_data = pl.DataFrame({
            "account_id": ["acc1", "acc2"],
            "name": ["Checking", "Savings"],
            "type": ["depository", "depository"],
            "subtype": ["checking", "savings"],
        })

        accounts_file = plaid_path / "accounts_20240101.parquet"
        accounts_data.write_parquet(accounts_file)

        # Create transactions data
        transactions_data = pl.DataFrame({
            "transaction_id": ["txn1", "txn2", "txn3"],
            "account_id": ["acc1", "acc1", "acc2"],
            "amount": [10.50, -25.00, 100.00],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        })

        transactions_file = plaid_path / "transactions_20240101.parquet"
        transactions_data.write_parquet(transactions_file)

        # Test loading
        loader = ParquetLoader(config)
        results = loader.load_all_parquet_files()

        assert "raw_plaid_accounts" in results
        assert "raw_plaid_transactions" in results
        assert results["raw_plaid_accounts"] == 2
        assert results["raw_plaid_transactions"] == 3

        # Test database status
        status = loader.get_database_status()
        assert "raw_plaid_accounts" in status
        assert "raw_plaid_transactions" in status
        assert status["raw_plaid_accounts"]["row_count"] == 2
        assert status["raw_plaid_transactions"]["row_count"] == 3

        # Test incremental loading with new data
        new_accounts_data = pl.DataFrame({
            "account_id": ["acc3"],
            "name": ["Credit Card"],
            "type": ["credit"],
            "subtype": ["credit_card"],
        })

        new_accounts_file = plaid_path / "accounts_20240102.parquet"
        new_accounts_data.write_parquet(new_accounts_file)

        # Load again (should be incremental)
        results = loader.load_all_parquet_files()
        assert results["raw_plaid_accounts"] == 3  # Should have 3 total now

        # Verify with database status
        status = loader.get_database_status()
        assert status["raw_plaid_accounts"]["row_count"] == 3
