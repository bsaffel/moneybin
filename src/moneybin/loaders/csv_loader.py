"""CSV data loader for DuckDB raw tables.

Loads extracted CSV data (Polars DataFrames) into DuckDB raw staging tables,
following the same pattern as the OFX loader.
"""

import logging
from pathlib import Path

import polars as pl

from moneybin.database import Database

logger = logging.getLogger(__name__)


class CSVLoader:
    """Load CSV extracted data into DuckDB raw tables."""

    def __init__(self, db: Database) -> None:
        """Initialize the CSV loader.

        Args:
            db: Database instance for all database operations.
        """
        self.db = db
        self.sql_dir = Path(__file__).parent.parent / "sql" / "schema"
        logger.info("Initialized CSV loader for database: %s", db.path)

    def create_raw_tables(self) -> None:
        """Create raw CSV tables in DuckDB by executing SQL schema files.

        Tables follow naming convention: raw.csv_<entity>
        """
        schema_files = [
            "raw_schema.sql",
            "raw_csv_accounts.sql",
            "raw_csv_transactions.sql",
        ]

        for sql_file in schema_files:
            sql_path = self.sql_dir / sql_file
            if not sql_path.exists():
                raise FileNotFoundError(f"SQL schema file not found: {sql_path}")

            with open(sql_path) as f:
                sql_content = f.read()
                self.db.execute(sql_content)
                logger.debug("Executed schema file: %s", sql_file)

        logger.info("Created CSV raw tables in DuckDB")

    def load_data(self, data: dict[str, pl.DataFrame]) -> dict[str, int]:
        """Load extracted CSV data into raw tables.

        Args:
            data: Dictionary of DataFrames (accounts, transactions).

        Returns:
            Row counts for each loaded table.
        """
        row_counts: dict[str, int] = {}

        self.create_raw_tables()

        # Load accounts
        if len(data.get("accounts", pl.DataFrame())) > 0:
            df = data["accounts"]
            self.db.ingest_dataframe("raw.csv_accounts", df, on_conflict="upsert")
            row_counts["accounts"] = len(df)
            logger.info("Loaded %d account(s)", row_counts["accounts"])

        # Load transactions
        if len(data.get("transactions", pl.DataFrame())) > 0:
            df = data["transactions"]
            self.db.ingest_dataframe("raw.csv_transactions", df, on_conflict="upsert")
            row_counts["transactions"] = len(df)
            logger.info("Loaded %d transaction(s)", row_counts["transactions"])

        return row_counts
