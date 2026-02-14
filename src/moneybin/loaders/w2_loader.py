"""W2 data loader for DuckDB raw tables.

This module loads extracted W2 data into DuckDB raw staging tables following
the same pattern as OFX data loading.
"""

import logging
from pathlib import Path

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class W2Loader:
    """Load W2 extracted data into DuckDB raw tables."""

    def __init__(self, database_path: Path | str):
        """Initialize the W2 loader.

        Args:
            database_path: Path to the DuckDB database file
        """
        self.database_path = Path(database_path)
        self.sql_dir = Path(__file__).parent.parent / "sql" / "schema"
        logger.info(f"Initialized W2 loader for database: {self.database_path}")

    def create_raw_tables(self) -> None:
        """Create raw W2 tables in DuckDB by executing SQL schema files.

        Tables follow Fivetran naming convention: raw.w2_forms
        Schema file is located in src/moneybin/sql/schema/
        """
        conn = duckdb.connect(str(self.database_path))

        try:
            # Execute schema files in order
            schema_files = [
                "raw_schema.sql",
                "raw_w2_forms.sql",
            ]

            for sql_file in schema_files:
                sql_path = self.sql_dir / sql_file
                if not sql_path.exists():
                    raise FileNotFoundError(f"SQL schema file not found: {sql_path}")

                with open(sql_path) as f:
                    sql_content = f.read()
                    conn.execute(sql_content)
                    logger.debug(f"Executed schema file: {sql_file}")

            logger.info("Created W2 raw tables in DuckDB")

        finally:
            conn.close()

    def load_data(self, data: pl.DataFrame) -> int:
        """Load extracted W2 data into raw table.

        Args:
            data: DataFrame containing W2 form data

        Returns:
            int: Number of rows loaded
        """
        conn = duckdb.connect(str(self.database_path))
        row_count = 0

        try:
            # Ensure tables exist
            self.create_raw_tables()

            # Load W2 forms (use INSERT OR REPLACE for idempotency)
            if len(data) > 0:
                df = data
                conn.execute("""
                    INSERT OR REPLACE INTO raw.w2_forms
                    (
                        tax_year, employee_ssn, employer_ein, control_number,
                        employee_first_name, employee_last_name, employee_address,
                        employer_name, employer_address,
                        wages, federal_income_tax,
                        social_security_wages, social_security_tax,
                        medicare_wages, medicare_tax,
                        social_security_tips, allocated_tips,
                        dependent_care_benefits, nonqualified_plans,
                        is_statutory_employee, is_retirement_plan, is_third_party_sick_pay,
                        state_local_info, optional_boxes,
                        source_file, extracted_at
                    )
                    SELECT
                        tax_year, employee_ssn, employer_ein, control_number,
                        employee_first_name, employee_last_name, employee_address,
                        employer_name, employer_address,
                        wages, federal_income_tax,
                        social_security_wages, social_security_tax,
                        medicare_wages, medicare_tax,
                        social_security_tips, allocated_tips,
                        dependent_care_benefits, nonqualified_plans,
                        is_statutory_employee, is_retirement_plan, is_third_party_sick_pay,
                        CASE WHEN state_local_info IS NOT NULL
                             THEN state_local_info::JSON
                             ELSE NULL END,
                        CASE WHEN optional_boxes IS NOT NULL
                             THEN optional_boxes::JSON
                             ELSE NULL END,
                        source_file, extracted_at::TIMESTAMP
                    FROM df
                """)
                row_count = len(df)
                logger.info(f"Loaded {row_count} W2 form(s)")

            return row_count

        finally:
            conn.close()

    def query_raw_data(self, limit: int | None = None) -> pl.DataFrame:
        """Query raw W2 data from DuckDB.

        Args:
            limit: Optional row limit

        Returns:
            pl.DataFrame: Query results
        """
        conn = duckdb.connect(str(self.database_path))

        try:
            if limit is not None:
                query = """
                    SELECT * FROM raw.w2_forms
                    ORDER BY loaded_at DESC LIMIT ?
                """
                df = conn.execute(query, [limit]).pl()
            else:
                query = """
                    SELECT * FROM raw.w2_forms
                    ORDER BY loaded_at DESC
                """
                df = conn.execute(query).pl()

            return df

        finally:
            conn.close()
