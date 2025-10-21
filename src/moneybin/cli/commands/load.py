"""Data loading commands for MoneyBin CLI.

This module provides commands for loading raw data files into DuckDB
for further processing by dbt transformations.
"""

import logging
from pathlib import Path

import typer

from moneybin.config import get_database_path, get_raw_data_path
from moneybin.loaders import ParquetLoader
from moneybin.loaders.parquet_loader import LoadingConfig
from moneybin.logging import setup_logging

app = typer.Typer(help="Load raw data files into DuckDB")
logger = logging.getLogger(__name__)


@app.command("parquet")
def load_parquet(
    source_path: Path = typer.Option(
        None,
        "--source",
        "-s",
        help="Source directory containing Parquet files (default: from config)",
    ),
    database_path: Path = typer.Option(
        None,
        "--database",
        "-d",
        help="Target DuckDB database file (default: from config)",
    ),
    incremental: bool = typer.Option(
        True,
        "--incremental/--full-refresh",
        help="Use incremental loading (avoid duplicates)",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    """Load Parquet files from raw data directories into DuckDB staging tables.

    This command scans for Parquet files in the source directory and loads them
    into appropriately named DuckDB tables for processing by dbt.

    Args:
        source_path: Directory containing raw Parquet files
        database_path: Path to DuckDB database file
        incremental: Whether to use incremental loading to avoid duplicates
        verbose: Enable debug level logging
    """
    setup_logging(cli_mode=True, verbose=verbose)

    try:
        # Create configuration with centralized defaults
        config = LoadingConfig(
            source_path=source_path or get_raw_data_path(),
            database_path=database_path or get_database_path(),
            incremental=incremental,
        )

        # Initialize loader and load files
        loader = ParquetLoader(config)
        results = loader.load_all_parquet_files()

        # Display results
        if results:
            logger.info("📊 Loading Results:")
            for table_name, count in results.items():
                logger.info(f"  {table_name}: {count:,} records")
        else:
            logger.warning("No data was loaded")

    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        logger.error(f"❌ Failed to load Parquet files: {e}")
        raise typer.Exit(1) from e


@app.command("status")
def load_status(
    database_path: Path = typer.Option(
        None,
        "--database",
        "-d",
        help="DuckDB database file to check (default: from config)",
    ),
) -> None:
    """Show the status of loaded data in DuckDB.

    Args:
        database_path: Path to DuckDB database file
    """
    setup_logging(cli_mode=True)

    try:
        # Create configuration with centralized defaults
        config = LoadingConfig(database_path=database_path or get_database_path())
        loader = ParquetLoader(config)

        # Get database status
        status = loader.get_database_status()

        logger.info("📊 DuckDB Data Status")
        logger.info("=" * 50)

        if not status:
            logger.info("No tables found in database")
            return

        for table_name, info in status.items():
            count = info["row_count"]
            size = info["estimated_size"]
            logger.info(f"  {table_name}: {count:,} rows ({size} bytes)")

    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        logger.error(f"❌ Failed to check database status: {e}")
        raise typer.Exit(1) from e
