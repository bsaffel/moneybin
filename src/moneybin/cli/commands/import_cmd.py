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
    from moneybin.database import Database

app = typer.Typer(
    help="Import financial files (OFX/QFX, CSV bank exports, W-2 PDFs) into MoneyBin",
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
    institution: str = typer.Option(
        None,
        "--institution",
        "-i",
        help="Institution name (OFX) or CSV profile name (auto-detects if omitted)",
    ),
    account_id: str = typer.Option(
        None, "--account-id", "-a", help="Account identifier (CSV only, required)"
    ),
) -> None:
    """Import a financial data file — auto-detects type, loads into DuckDB, and rebuilds core tables.

    Supported file types:
      - OFX/QFX: Bank and credit card statements
      - CSV: Bank transaction exports (Chase, Citi, etc.)
      - PDF: IRS Form W-2 wage and tax statements

    Examples:
        moneybin import file ~/Downloads/WellsFargo_2025.qfx
        moneybin import file ~/Downloads/chase_activity.csv --account-id chase-7022
        moneybin import file ~/Downloads/2024_W2.pdf
        moneybin import file statement.ofx --institution "Wells Fargo"
    """
    from moneybin.database import get_database
    from moneybin.services.import_service import import_file as do_import

    source = Path(file_path)

    if not source.exists():
        logger.error(f"File not found: {source}")
        raise typer.Exit(1)

    try:
        db = get_database()
        result = do_import(
            db=db,
            file_path=source,
            do_transforms=not skip_transform,
            institution=institution,
            account_id=account_id,
        )
        logger.info(f"✅ {result.summary()}")
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("status")
def import_status() -> None:
    """Show a summary of all imported data: row counts, date ranges, and sources.

    Queries raw tables in DuckDB to display what has been imported so far.

    Example:
        moneybin import status
    """
    from moneybin.config import get_settings
    from moneybin.database import get_database

    db_path = get_settings().database.path

    if not db_path.exists():
        logger.warning(f"Database not found: {db_path}")
        logger.info("Run 'moneybin import file <path>' to import data first.")
        raise typer.Exit(1)

    try:
        db = get_database()
        _print_import_status(db)
    except Exception as e:  # noqa: BLE001 — surface connection errors generically
        logger.error(f"❌ Could not open database: {e}")
        raise typer.Exit(1) from e


def _print_import_status(db: Database) -> None:
    """Query raw tables and print import summary.

    Args:
        db: Database instance.
    """
    tables = db.execute("""
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

    from sqlglot import exp

    for schema, table in tables:
        safe_schema = exp.to_identifier(schema, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
        safe_table = exp.to_identifier(table, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
        row_count = db.execute(
            f"SELECT COUNT(*) FROM {safe_schema}.{safe_table}"  # noqa: S608 — sqlglot-quoted catalog identifiers
        ).fetchone()
        count = row_count[0] if row_count else 0

        # Try to get date range for transaction-like tables
        date_info = ""
        if "transaction" in table:
            try:
                dates = db.execute(
                    f"SELECT MIN(CAST(date_posted AS DATE)), MAX(CAST(date_posted AS DATE)) FROM {safe_schema}.{safe_table}"  # noqa: S608 — sqlglot-quoted catalog identifiers
                ).fetchone()
                if dates and dates[0]:
                    date_info = f"  ({dates[0]} to {dates[1]})"
            except Exception:  # noqa: BLE001 — column may not exist in all tables
                logger.debug(f"Could not get date range for {schema}.{table}")

        print(f"  {schema}.{table}: {count:,} rows{date_info}")

    print()
