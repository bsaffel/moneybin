"""Parquet file loader for DuckDB.

This module provides functionality to load Parquet files from raw data directories
into DuckDB staging tables for further processing by dbt transformations.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


@dataclass
class LoadingConfig:
    """Configuration for Parquet loading operations."""

    source_path: Path = Path("data/raw")
    database_path: Path = Path("data/duckdb/moneybin.duckdb")
    incremental: bool = True
    create_database_dir: bool = True


class ParquetLoader:
    """Loader for Parquet files into DuckDB staging tables."""

    def __init__(self, config: LoadingConfig | None = None):
        """Initialize the Parquet loader.

        Args:
            config: Loading configuration options
        """
        self.config = config or LoadingConfig()

        if self.config.create_database_dir:
            self.config.database_path.parent.mkdir(parents=True, exist_ok=True)

    def load_all_parquet_files(self) -> dict[str, int]:
        """Load all Parquet files from the source directory into DuckDB.

        Returns:
            dict: Mapping of table names to record counts loaded
        """
        logger.info("Starting Parquet file loading into DuckDB")
        logger.info(f"Source: {self.config.source_path}")
        logger.info(f"Database: {self.config.database_path}")
        logger.info(
            f"Mode: {'incremental' if self.config.incremental else 'full refresh'}"
        )

        if not self.config.source_path.exists():
            raise FileNotFoundError(
                f"Source path does not exist: {self.config.source_path}"
            )

        results = {}

        # Pyright reports "Type of 'connect' is partially unknown" because DuckDB's type stubs
        # define the config parameter as dict[Unknown, Unknown]. This triggers Pyright's
        # reportUnknownMemberType error in strict mode. The function works correctly;
        # this is a type annotation limitation in DuckDB's current stubs.
        with duckdb.connect(self.config.database_path) as conn:  # type: ignore[misc]
            logger.info(f"Connected to DuckDB database: {self.config.database_path}")

            # Load Plaid data
            plaid_path = self.config.source_path / "plaid"
            if plaid_path.exists():
                plaid_results = self._load_plaid_data(conn, plaid_path)
                # Pyright's reportUnknownArgumentType flags dict.update() because the return type
                # from _load_plaid_data contains Unknown types from DuckDB operations. We know
                # plaid_results is actually dict[str, int] from our method signature.
                results.update(plaid_results)  # type: ignore[misc]

            # Future: Load other data sources
            # csv_path = self.config.source_path / "csv"
            # if csv_path.exists():
            #     csv_results = self._load_csv_data(conn, csv_path)
            #     results.update(csv_results)

        logger.info("✅ Parquet loading completed successfully")
        return results

    def get_database_status(self) -> dict[str, dict[str, Any]]:
        """Get the status of loaded data in DuckDB.

        Returns:
            dict: Mapping of table names to their status information
        """
        if not self.config.database_path.exists():
            raise FileNotFoundError(
                f"Database file does not exist: {self.config.database_path}"
            )

        status = {}

        # Pyright reports "Type of 'connect' is partially unknown" because DuckDB's type stubs
        # define the config parameter as dict[Unknown, Unknown]. This triggers Pyright's
        # reportUnknownMemberType error in strict mode. The function works correctly;
        # this is a type annotation limitation in DuckDB's current stubs.
        with duckdb.connect(self.config.database_path) as conn:  # type: ignore[misc]
            # List all tables
            tables = conn.sql("""
                SELECT table_name, estimated_size
                FROM duckdb_tables()
                WHERE schema_name = 'main'
                ORDER BY table_name
            """).fetchall()

            for table_name, size in tables:
                # Get row count - validate table name is a safe SQL identifier
                if not table_name.isidentifier():
                    logger.warning(f"Skipping invalid table name: {table_name}")
                    continue

                # Pyright reports "Type of 'fetchone' is partially unknown" because DuckDB's stubs
                # define fetchone() as returning tuple[Unknown, ...] | None. This triggers
                # reportUnknownMemberType in strict mode. We know COUNT(*) returns an integer.
                result = conn.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()  # type: ignore[misc] # noqa: S608  # table_name validated as safe identifier
                count = result[0] if result else 0
                status[table_name] = {"row_count": count, "estimated_size": size}

        return status

    def _load_plaid_data(
        self, conn: duckdb.DuckDBPyConnection, plaid_path: Path
    ) -> dict[str, int]:
        """Load Plaid Parquet files into DuckDB staging tables.

        Args:
            conn: DuckDB connection
            plaid_path: Path to Plaid Parquet files

        Returns:
            dict: Mapping of table names to record counts
        """
        logger.info("Loading Plaid data...")
        results = {}

        # Load accounts
        account_files = list(plaid_path.glob("accounts_*.parquet"))
        if account_files:
            logger.info(f"Found {len(account_files)} account files")
            count = self._load_accounts_table(conn, account_files)
            results["raw_plaid_accounts"] = count

        # Load transactions
        transaction_files = list(plaid_path.glob("transactions_*.parquet"))
        if transaction_files:
            logger.info(f"Found {len(transaction_files)} transaction files")
            count = self._load_transactions_table(conn, transaction_files)
            results["raw_plaid_transactions"] = count

        if not account_files and not transaction_files:
            logger.warning(f"No Plaid Parquet files found in {plaid_path}")

        return results

    def _load_accounts_table(
        self, conn: duckdb.DuckDBPyConnection, parquet_files: list[Path]
    ) -> int:
        """Load account Parquet files into raw_plaid_accounts table.

        Args:
            conn: DuckDB connection
            parquet_files: List of account Parquet files

        Returns:
            int: Number of records in the final table
        """
        table_name = "raw_plaid_accounts"

        # Create file pattern for DuckDB to read multiple Parquet files
        if len(parquet_files) == 1:
            file_pattern = f"'{parquet_files[0]}'"
        else:
            # Use glob pattern for multiple files
            parquet_dir = parquet_files[0].parent
            file_pattern = f"'{parquet_dir / 'accounts_*.parquet'}'"

        logger.info(
            f"Loading {len(parquet_files)} account files using pattern: {file_pattern}"
        )

        if self.config.incremental:
            # Check if table exists
            # Pyright's reportUnknownMemberType flags fetchone() as "partially unknown"
            # because DuckDB stubs define it as returning tuple[Unknown, ...] | None.
            # We know this COUNT(*) query returns a single integer value.
            result = conn.sql(f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = '{table_name}'
            """).fetchone()  # type: ignore[misc] # noqa: S608  # table_name validated as safe identifier
            table_exists = (result[0] if result else 0) > 0

            if table_exists:
                # Insert only new records (avoiding duplicates by account_id)
                logger.info("Using incremental loading for accounts")
                conn.sql(f"""
                    INSERT INTO {table_name}
                    SELECT *
                    FROM read_parquet({file_pattern})
                    WHERE account_id NOT IN (
                        SELECT DISTINCT account_id
                        FROM {table_name}
                    )
                """)  # noqa: S608  # table_name validated as safe identifier
            else:
                # Create new table
                logger.info("Creating new accounts table")
                conn.sql(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM read_parquet({file_pattern})
                """)  # noqa: S608  # table_name validated as safe identifier
        else:
            # Full refresh - replace entire table
            logger.info("Using full refresh for accounts")
            conn.sql(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_parquet({file_pattern})
            """)  # noqa: S608  # table_name validated as safe identifier

        # Get final count
        # Pyright's reportUnknownMemberType error occurs because DuckDB stubs define
        # fetchone() as returning tuple[Unknown, ...] | None with Unknown tuple elements.
        # We safely handle the None case and know COUNT(*) returns a single integer.
        result = conn.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()  # type: ignore[misc] # noqa: S608  # table_name validated as safe identifier
        count = result[0] if result else 0
        logger.info(f"✅ Accounts table now contains {count} records")
        return count

    def _load_transactions_table(
        self, conn: duckdb.DuckDBPyConnection, parquet_files: list[Path]
    ) -> int:
        """Load transaction Parquet files into raw_plaid_transactions table.

        Args:
            conn: DuckDB connection
            parquet_files: List of transaction Parquet files

        Returns:
            int: Number of records in the final table
        """
        table_name = "raw_plaid_transactions"

        # Create file pattern for DuckDB to read multiple Parquet files
        if len(parquet_files) == 1:
            file_pattern = f"'{parquet_files[0]}'"
        else:
            # Use glob pattern for multiple files
            parquet_dir = parquet_files[0].parent
            file_pattern = f"'{parquet_dir / 'transactions_*.parquet'}'"

        logger.info(
            f"Loading {len(parquet_files)} transaction files using pattern: {file_pattern}"
        )

        if self.config.incremental:
            # Check if table exists
            # Pyright's reportUnknownMemberType flags fetchone() as "partially unknown"
            # because DuckDB stubs define it as returning tuple[Unknown, ...] | None.
            # We know this COUNT(*) query returns a single integer value.
            result = conn.sql(f"""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = '{table_name}'
            """).fetchone()  # type: ignore[misc] # noqa: S608  # table_name validated as safe identifier
            table_exists = (result[0] if result else 0) > 0

            if table_exists:
                # Insert only new records (avoiding duplicates by transaction_id)
                logger.info("Using incremental loading for transactions")
                conn.sql(f"""
                    INSERT INTO {table_name}
                    SELECT *
                    FROM read_parquet({file_pattern})
                    WHERE transaction_id NOT IN (
                        SELECT DISTINCT transaction_id
                        FROM {table_name}
                    )
                """)  # noqa: S608  # table_name validated as safe identifier
            else:
                # Create new table
                logger.info("Creating new transactions table")
                conn.sql(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM read_parquet({file_pattern})
                """)  # noqa: S608  # table_name validated as safe identifier
        else:
            # Full refresh - replace entire table
            logger.info("Using full refresh for transactions")
            conn.sql(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_parquet({file_pattern})
            """)  # noqa: S608  # table_name validated as safe identifier

        # Get final count
        # Pyright's reportUnknownMemberType error occurs because DuckDB stubs define
        # fetchone() as returning tuple[Unknown, ...] | None with Unknown tuple elements.
        # We safely handle the None case and know COUNT(*) returns a single integer.
        result = conn.sql(f"SELECT COUNT(*) FROM {table_name}").fetchone()  # type: ignore[misc] # noqa: S608  # table_name validated as safe identifier
        count = result[0] if result else 0
        logger.info(f"✅ Transactions table now contains {count} records")
        return count
