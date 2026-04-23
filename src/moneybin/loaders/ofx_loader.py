"""OFX data loader for DuckDB raw tables.

This module loads extracted OFX data into DuckDB raw staging tables following
the Fivetran pattern of preserving source data structure.
"""

import logging
from pathlib import Path

import polars as pl

from moneybin.database import Database

logger = logging.getLogger(__name__)


class OFXLoader:
    """Load OFX extracted data into DuckDB raw tables."""

    def __init__(self, db: Database) -> None:
        """Initialize the OFX loader.

        Args:
            db: Database instance for all database operations.
        """
        self.db = db
        self.sql_dir = Path(__file__).parent.parent / "sql" / "schema"
        logger.info(f"Initialized OFX loader for database: {db.path}")

    def create_raw_tables(self) -> None:
        """Create raw OFX tables in DuckDB by executing SQL schema files.

        Tables follow Fivetran naming convention: raw.ofx_<entity>
        Schema files are located in src/moneybin/sql/schema/
        """
        # Execute schema files in order
        schema_files = [
            "raw_schema.sql",
            "raw_ofx_institutions.sql",
            "raw_ofx_accounts.sql",
            "raw_ofx_transactions.sql",
            "raw_ofx_balances.sql",
        ]

        for sql_file in schema_files:
            sql_path = self.sql_dir / sql_file
            if not sql_path.exists():
                raise FileNotFoundError(f"SQL schema file not found: {sql_path}")

            with open(sql_path) as f:
                sql_content = f.read()
                self.db.execute(sql_content)
                logger.debug(f"Executed schema file: {sql_file}")

        logger.info("Created OFX raw tables in DuckDB")

    def load_data(self, data: dict[str, pl.DataFrame]) -> dict[str, int]:
        """Load extracted OFX data into raw tables.

        Args:
            data: Dictionary of DataFrames (institutions, accounts, transactions, balances)

        Returns:
            dict: Row counts for each loaded table
        """
        row_counts = {}

        # Use conn directly for INSERT with inline type casts (::TIMESTAMP)
        # and CASE expressions that ingest_dataframe's SELECT * doesn't support.
        conn = self.db.conn

        # Load institutions (use INSERT OR REPLACE for idempotency)
        if len(data.get("institutions", pl.DataFrame())) > 0:
            df = data["institutions"]
            conn.execute("""
                INSERT OR REPLACE INTO raw.ofx_institutions
                (organization, fid, source_file, extracted_at)
                SELECT organization, fid, source_file, extracted_at::TIMESTAMP
                FROM df
            """)
            row_counts["institutions"] = len(df)
            logger.info(f"Loaded {row_counts['institutions']} institution(s)")

        # Load accounts
        if len(data.get("accounts", pl.DataFrame())) > 0:
            df = data["accounts"]
            conn.execute("""
                INSERT OR REPLACE INTO raw.ofx_accounts
                (account_id, routing_number, account_type, institution_org,
                 institution_fid, source_file, extracted_at)
                SELECT account_id, routing_number, account_type, institution_org,
                       institution_fid, source_file, extracted_at::TIMESTAMP
                FROM df
            """)
            row_counts["accounts"] = len(df)
            logger.info(f"Loaded {row_counts['accounts']} account(s)")

        # Load transactions
        if len(data.get("transactions", pl.DataFrame())) > 0:
            df = data["transactions"]
            conn.execute("""
                INSERT OR REPLACE INTO raw.ofx_transactions
                (source_transaction_id, account_id, transaction_type, date_posted,
                 amount, payee, memo, check_number, source_file, extracted_at)
                SELECT source_transaction_id, account_id, transaction_type,
                       date_posted::TIMESTAMP, amount, payee, memo,
                       check_number, source_file, extracted_at::TIMESTAMP
                FROM df
            """)
            row_counts["transactions"] = len(df)
            logger.info(f"Loaded {row_counts['transactions']} transaction(s)")

        # Load balances
        if len(data.get("balances", pl.DataFrame())) > 0:
            df = data["balances"]
            conn.execute("""
                INSERT OR REPLACE INTO raw.ofx_balances
                (account_id, statement_start_date, statement_end_date,
                 ledger_balance, ledger_balance_date, available_balance,
                 source_file, extracted_at)
                SELECT account_id,
                       CASE WHEN statement_start_date IS NOT NULL
                            THEN statement_start_date::TIMESTAMP
                            ELSE NULL END,
                       CASE WHEN statement_end_date IS NOT NULL
                            THEN statement_end_date::TIMESTAMP
                            ELSE NULL END,
                       ledger_balance,
                       CASE WHEN ledger_balance_date IS NOT NULL
                            THEN ledger_balance_date::TIMESTAMP
                            ELSE NULL END,
                       available_balance,
                       source_file, extracted_at::TIMESTAMP
                FROM df
            """)
            row_counts["balances"] = len(df)
            logger.info(f"Loaded {row_counts['balances']} balance record(s)")

        return row_counts

    def query_raw_data(self, table_name: str, limit: int | None = None) -> pl.DataFrame:
        """Query raw OFX data from DuckDB.

        Args:
            table_name: Name of the table (institutions, accounts, transactions, balances)
            limit: Optional row limit

        Returns:
            pl.DataFrame: Query results

        Raises:
            ValueError: If table_name is not one of the allowed values
        """
        # Validate table_name to prevent SQL injection
        allowed_tables = {"institutions", "accounts", "transactions", "balances"}
        if table_name not in allowed_tables:
            raise ValueError(
                f"Invalid table name: {table_name}. "
                f"Must be one of: {', '.join(sorted(allowed_tables))}"
            )

        # Use DuckDB's parameter binding for LIMIT
        # Note: table_name is validated above, so f-string is safe
        if limit is not None:
            query = f"""
                SELECT * FROM raw.ofx_{table_name}
                ORDER BY loaded_at DESC LIMIT ?
            """  # noqa: S608 — table_name is validated against VALID_TABLES allowlist above
            return self.db.execute(query, [limit]).pl()
        else:
            query = f"""
                SELECT * FROM raw.ofx_{table_name}
                ORDER BY loaded_at DESC
            """  # noqa: S608 — table_name is validated against VALID_TABLES allowlist above
            return self.db.execute(query).pl()
