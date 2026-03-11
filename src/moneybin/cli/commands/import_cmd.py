# ruff: noqa: A001 — "import" shadows builtin, but it's a Typer subcommand name
"""Import commands for MoneyBin CLI.

This module provides the user-facing import workflow: auto-detect file type,
extract, load into DuckDB, and optionally run SQLMesh transforms.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:
    import duckdb

app = typer.Typer(
    help="Import financial files (OFX/QFX bank statements, W-2 PDFs) into MoneyBin",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


@app.command("file")
def import_file(
    file_path: str = typer.Argument(
        ..., help="Path to the financial data file to import"
    ),
    skip_transform: bool = typer.Option(
        False,
        "--skip-transform",
        help="Skip rebuilding core tables after import",
    ),
    institution_name: str = typer.Option(
        None, "--institution", "-i", help="Institution name override (OFX only)"
    ),
    tax_year: int = typer.Option(
        None, "--year", "-y", help="Tax year override (W-2 only)"
    ),
) -> None:
    """Import a financial data file — auto-detects type, loads into DuckDB, and rebuilds core tables.

    Supported file types:
      - OFX/QFX: Bank and credit card statements
      - PDF: IRS Form W-2 wage and tax statements

    Examples:
        moneybin import file ~/Downloads/WellsFargo_2025.qfx
        moneybin import file ~/Downloads/2024_W2.pdf
        moneybin import file statement.ofx --skip-transform
        moneybin import file file.qfx --institution "Wells Fargo"
        moneybin import file W2.pdf --year 2024
    """
    from moneybin.config import get_database_path
    from moneybin.services.import_service import import_file as do_import

    source = Path(file_path)

    if not source.exists():
        logger.error("File not found: %s", source)
        raise typer.Exit(1)

    try:
        result = do_import(
            db_path=get_database_path(),
            file_path=source,
            run_transforms=not skip_transform,
            institution_name=institution_name,
            tax_year=tax_year,
        )
        logger.info("✅ %s", result.summary())
    except ValueError as e:
        logger.error("❌ %s", e)
        raise typer.Exit(1) from e
    except FileNotFoundError as e:
        logger.error("❌ %s", e)
        raise typer.Exit(1) from e


@app.command("status")
def import_status() -> None:
    """Show a summary of all imported data: row counts, date ranges, and sources.

    Queries raw tables in DuckDB to display what has been imported so far.

    Example:
        moneybin import status
    """
    import duckdb

    from moneybin.config import get_database_path

    db_path = get_database_path()

    if not db_path.exists():
        logger.warning("Database not found: %s", db_path)
        logger.info("Run 'moneybin import file <path>' to import data first.")
        raise typer.Exit(1)

    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            _print_import_status(conn)
        finally:
            conn.close()
    except duckdb.IOException as e:
        logger.error("❌ Could not open database: %s", e)
        raise typer.Exit(1) from e


def _print_import_status(conn: duckdb.DuckDBPyConnection) -> None:
    """Query raw tables and print import summary.

    Args:
        conn: Read-only DuckDB connection.
    """
    tables = conn.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw'
        ORDER BY table_name
    """).fetchall()

    if not tables:
        print("\n📭 No imported data found.")
        print("   Run 'moneybin import file <path>' to get started.")
        return

    print("\n📊 Imported Data Summary")
    print("=" * 60)

    for schema, table in tables:
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {schema}.{table}"  # noqa: S608 — schema/table from information_schema, not user input
        ).fetchone()
        count = row_count[0] if row_count else 0

        # Try to get date range for transaction-like tables
        date_info = ""
        if "transaction" in table:
            try:
                dates = conn.execute(
                    f"SELECT MIN(CAST(date_posted AS DATE)), MAX(CAST(date_posted AS DATE)) FROM {schema}.{table}"  # noqa: S608 — schema/table from information_schema, not user input
                ).fetchone()
                if dates and dates[0]:
                    date_info = f"  ({dates[0]} to {dates[1]})"
            except Exception:  # noqa: BLE001 — column may not exist in all tables
                logger.debug("Could not get date range for %s.%s", schema, table)

        print(f"  {schema}.{table}: {count:,} rows{date_info}")

    print()
