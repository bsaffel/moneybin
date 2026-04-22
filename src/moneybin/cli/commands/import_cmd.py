# ruff: noqa: A001 — "import" shadows builtin, but it's a Typer subcommand name
"""Import commands for MoneyBin CLI.

This module provides the user-facing import workflow: auto-detect file type,
extract, load into DuckDB, and optionally run SQLMesh transforms.
Also provides history, revert, preview, and format management subcommands.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, get_args

import typer

from moneybin.extractors.tabular.formats import NumberFormatType, SignConventionType

if TYPE_CHECKING:
    from moneybin.database import Database

app = typer.Typer(
    help=(
        "Import financial files (OFX/QFX, CSV/TSV/Excel/Parquet, W-2 PDFs) "
        "into MoneyBin"
    ),
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)

_VALID_SIGN_CONVENTIONS = frozenset(get_args(SignConventionType))
_VALID_NUMBER_FORMATS = frozenset(get_args(NumberFormatType))


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
        None, "--account-id", "-a", help="Account identifier (bypasses name matching)"
    ),
    account_name: str = typer.Option(
        None,
        "--account-name",
        "-n",
        help="Account name for single-account tabular files",
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Auto-confirm high-confidence detections"
    ),
    format_name: str = typer.Option(
        None,
        "--format",
        "-f",
        help="Use a specific named format (bypass auto-detection)",
    ),
    override: list[str] = typer.Option(
        None,
        "--override",
        help=(
            "Field→column override, repeatable (e.g. --override date=Date "
            "--override amount=Amount)"
        ),
    ),
    sign: str = typer.Option(
        None,
        "--sign",
        help=(
            "Sign convention override: negative_is_expense, "
            "negative_is_income, split_debit_credit"
        ),
    ),
    date_format: str = typer.Option(
        None,
        "--date-format",
        help="Date format override (strptime format string, e.g. %%Y-%%m-%%d)",
    ),
    number_format: str = typer.Option(
        None,
        "--number-format",
        help="Number format override: us, european, swiss_french, zero_decimal",
    ),
    sheet: str = typer.Option(
        None, "--sheet", help="Excel sheet name (default: auto-select largest)"
    ),
    delimiter: str = typer.Option(
        None, "--delimiter", help="Explicit delimiter for text formats"
    ),
    encoding: str = typer.Option(
        None, "--encoding", help="Explicit file encoding (e.g. utf-8, latin-1)"
    ),
    no_row_limit: bool = typer.Option(
        False, "--no-row-limit", help="Override row count limit"
    ),
    no_size_limit: bool = typer.Option(
        False, "--no-size-limit", help="Override file size limit"
    ),
    save_format: bool = typer.Option(
        True,
        "--save-format/--no-save-format",
        help="Auto-save detected format for future imports (default: save)",
    ),
) -> None:
    """Import a financial data file — auto-detects type, loads into DuckDB, and rebuilds core tables.

    Supported file types:
      - OFX/QFX: Bank and credit card statements
      - CSV/TSV/Excel: Bank transaction exports (Chase, Citi, etc.)
      - Parquet/Feather: Data warehouse exports
      - PDF: IRS Form W-2 wage and tax statements

    Examples:
        moneybin import file ~/Downloads/WellsFargo_2025.qfx
        moneybin import file ~/Downloads/chase_activity.csv --account-name "Chase Checking"
        moneybin import file ~/Downloads/transactions.xlsx --format chase_credit
        moneybin import file ~/Downloads/2024_W2.pdf
        moneybin import file statement.ofx --institution "Wells Fargo"
        moneybin import file export.csv --override date=Date --override amount=Amount
    """
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.services.import_service import import_file as do_import

    source = Path(file_path)

    if not source.exists():
        logger.error(f"❌ File not found: {source}")
        raise typer.Exit(1)

    # Validate --override values (format: "field=column") — parsing deferred until
    # the service function accepts them; validated here for early error reporting.
    if override:
        for raw in override:
            if "=" not in raw:
                logger.error(
                    f"❌ Invalid --override format (expected field=column): {raw!r}"
                )
                raise typer.Exit(1)

    # Validate sign convention if provided
    if sign and sign not in _VALID_SIGN_CONVENTIONS:
        logger.error(
            f"❌ Invalid --sign value: {sign!r}. "
            f"Valid options: {', '.join(sorted(_VALID_SIGN_CONVENTIONS))}"
        )
        raise typer.Exit(1)

    # Validate number format if provided
    if number_format and number_format not in _VALID_NUMBER_FORMATS:
        logger.error(
            f"❌ Invalid --number-format value: {number_format!r}. "
            f"Valid options: {', '.join(sorted(_VALID_NUMBER_FORMATS))}"
        )
        raise typer.Exit(1)

    # Parse --override values into dict
    overrides: dict[str, str] | None = None
    if override:
        overrides = {}
        for raw in override:
            field_name, col_name = raw.split("=", 1)
            overrides[field_name.strip()] = col_name.strip()

    try:
        db = get_database()
        result = do_import(
            db=db,
            file_path=source,
            run_transforms=not skip_transform,
            institution=institution,
            account_id=account_id,
            account_name=account_name,
            format_name=format_name,
            overrides=overrides,
            sheet=sheet,
            delimiter=delimiter,
            encoding=encoding,
            no_row_limit=no_row_limit,
            no_size_limit=no_size_limit,
        )
        logger.info(f"✅ {result.summary()}")
    except DatabaseKeyError as e:
        logger.error(f"❌ Database is locked: {e}")
        logger.info("💡 Run `moneybin db unlock` to unlock the database first")
        raise typer.Exit(1) from e
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except PermissionError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("history")
def import_history(
    limit: int = typer.Option(20, "--limit", "-n", help="Max records to show"),
    import_id: str = typer.Option(
        None, "--import-id", help="Show details for a specific import"
    ),
) -> None:
    """List recent imports with batch details.

    Shows import ID, source file, status, row counts, and detection confidence
    for each completed import batch.

    Examples:
        moneybin import history
        moneybin import history --limit 50
        moneybin import history --import-id abc123
    """
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.loaders.tabular_loader import TabularLoader

    try:
        db = get_database()
        loader = TabularLoader(db)
        records = loader.get_import_history(limit=limit, import_id=import_id)
    except DatabaseKeyError as e:
        logger.error(f"❌ Database is locked: {e}")
        logger.info("💡 Run `moneybin db unlock` to unlock the database first")
        raise typer.Exit(1) from e

    if not records:
        if import_id:
            logger.warning(f"⚠️  No import found with ID: {import_id}")
        else:
            logger.warning("⚠️  No import history found")
        return

    print(
        f"\n{'Import ID':<38} {'Status':<10} {'Imported':>8} {'Rejected':>8}  {'Source File'}"
    )
    print("-" * 100)
    for rec in records:
        imp_id = str(rec.get("import_id", ""))
        status = str(rec.get("status", ""))
        rows_imported = rec.get("rows_imported") or 0
        rows_rejected = rec.get("rows_rejected") or 0
        source_file = str(rec.get("source_file", ""))
        # Truncate source file path for display
        display_path = Path(source_file).name if source_file else ""
        print(
            f"{imp_id:<38} {status:<10} {rows_imported:>8} {rows_rejected:>8}  "
            f"{display_path}"
        )

    if import_id and records:
        rec = records[0]
        print("\nDetails:")
        for key, value in rec.items():
            if value is not None:
                print(f"  {key}: {value}")
    print()


@app.command("revert")
def import_revert(
    import_id: str = typer.Argument(..., help="Import batch ID to revert"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Revert an import — deletes all rows from that batch.

    Removes all transactions and accounts loaded in the specified import batch,
    and marks the batch as reverted in the import log.

    Examples:
        moneybin import revert abc123-...
        moneybin import revert abc123-... --yes
    """
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.loaders.tabular_loader import TabularLoader

    if not yes:
        confirmed = typer.confirm(
            f"Revert import {import_id[:8]}...? This will delete all rows from "
            f"this batch and cannot be undone."
        )
        if not confirmed:
            logger.info("Revert cancelled")
            raise typer.Exit(0)

    try:
        db = get_database()
        loader = TabularLoader(db)
        result = loader.revert_import(import_id)
    except DatabaseKeyError as e:
        logger.error(f"❌ Database is locked: {e}")
        logger.info("💡 Run `moneybin db unlock` to unlock the database first")
        raise typer.Exit(1) from e

    status = result.get("status")
    if status == "not_found":
        logger.error(f"❌ {result.get('reason', 'Import not found')}")
        raise typer.Exit(1)
    elif status == "already_reverted":
        logger.warning(f"⚠️  Import {import_id[:8]}... was already reverted")
    else:
        rows_deleted = result.get("rows_deleted", 0)
        logger.info(
            f"✅ Reverted import {import_id[:8]}...: {rows_deleted} rows deleted"
        )


@app.command("preview")
def import_preview(
    file_path: str = typer.Argument(..., help="File to preview"),
    format_name: str = typer.Option(
        None,
        "--format",
        "-f",
        help="Use a specific named format (bypass auto-detection)",
    ),
    sheet: str = typer.Option(
        None, "--sheet", help="Excel sheet name (default: auto-select largest)"
    ),
    delimiter: str = typer.Option(
        None, "--delimiter", help="Explicit delimiter for text formats"
    ),
    encoding: str = typer.Option(
        None, "--encoding", help="Explicit file encoding (e.g. utf-8, latin-1)"
    ),
    override: list[str] = typer.Option(
        None,
        "--override",
        help="Field→column override, repeatable (e.g. --override date=Date)",
    ),
) -> None:
    """Preview file structure without importing.

    Runs detection and column-mapping stages without loading any data into
    the database. Shows detected format, column mapping, and sample rows.

    Examples:
        moneybin import preview ~/Downloads/chase_activity.csv
        moneybin import preview ~/Downloads/transactions.xlsx --sheet Sheet1
    """
    from moneybin.extractors.tabular.column_mapper import map_columns
    from moneybin.extractors.tabular.format_detector import detect_format
    from moneybin.extractors.tabular.formats import load_builtin_formats
    from moneybin.extractors.tabular.readers import read_file

    source = Path(file_path)

    if not source.exists():
        logger.error(f"❌ File not found: {source}")
        raise typer.Exit(1)

    # Parse --override values
    overrides: dict[str, str] | None = None
    if override:
        parsed: dict[str, str] = {}
        for raw in override:
            if "=" not in raw:
                logger.error(
                    f"❌ Invalid --override format (expected field=column): {raw!r}"
                )
                raise typer.Exit(1)
            field, _, col = raw.partition("=")
            parsed[field.strip()] = col.strip()
        overrides = parsed

    try:
        # Stage 1: Detect format
        format_info = detect_format(
            source,
            delimiter_override=delimiter,
            encoding_override=encoding,
        )

        # Stage 2: Read file
        read_result = read_file(source, format_info, sheet=sheet)
        df = read_result.df

        if len(df) == 0:
            logger.warning(f"⚠️  No data rows found in {source.name}")
            return

        # Stage 3: Column mapping
        matched_format = None
        builtin = load_builtin_formats()
        if format_name:
            matched_format = builtin.get(format_name)
            if matched_format is None:
                logger.warning(
                    f"⚠️  Format {format_name!r} not found in built-in formats"
                )
        else:
            headers = list(df.columns)
            for fmt in builtin.values():
                if fmt.matches_headers(headers):
                    matched_format = fmt
                    break

        print(f"\nFile: {source.name}")
        print(f"Type: {format_info.file_type}")
        if format_info.delimiter:
            print(f"Delimiter: {format_info.delimiter!r}")
        print(f"Encoding: {format_info.encoding}")
        print(f"Rows: {len(df):,}")
        if read_result.rows_skipped_trailing:
            print(f"Trailing rows skipped: {read_result.rows_skipped_trailing}")
        print(f"Columns ({len(df.columns)}): {', '.join(df.columns)}")

        if matched_format:
            print(
                f"\nMatched format: {matched_format.name} ({matched_format.institution_name})"
            )
            print(f"Sign convention: {matched_format.sign_convention}")
            print(f"Date format: {matched_format.date_format}")
            print(f"Number format: {matched_format.number_format}")
            print("\nColumn mapping:")
            for field, col in matched_format.field_mapping.items():
                print(f"  {field} ← {col}")
        else:
            mapping_result = map_columns(df, overrides=overrides)
            print(f"\nDetected mapping (confidence: {mapping_result.confidence}):")
            for field, col in mapping_result.field_mapping.items():
                print(f"  {field} ← {col}")
            if mapping_result.sign_convention:
                print(f"Sign convention: {mapping_result.sign_convention}")
            if mapping_result.date_format:
                print(f"Date format: {mapping_result.date_format}")
            if mapping_result.number_format:
                print(f"Number format: {mapping_result.number_format}")

        # Show sample rows
        sample_n = min(5, len(df))
        print(f"\nSample ({sample_n} rows):")
        print(df.head(sample_n))
        print()

    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("list-formats")
def list_formats() -> None:
    """List all formats (built-in and user-saved).

    Displays format name, institution, sign convention, and date format
    for all available import formats.

    Example:
        moneybin import list-formats
    """
    from moneybin.extractors.tabular.formats import load_builtin_formats

    builtin = load_builtin_formats()

    if not builtin:
        logger.warning("⚠️  No built-in formats found")
        return

    print(f"\n{'Name':<24} {'Institution':<28} {'Sign Convention':<24} {'Date Format'}")
    print("-" * 100)
    for fmt in sorted(builtin.values(), key=lambda f: f.name):
        print(
            f"{fmt.name:<24} {fmt.institution_name:<28} "
            f"{fmt.sign_convention:<24} {fmt.date_format}"
        )
    print(f"\n{len(builtin)} built-in format(s)\n")


@app.command("show-format")
def show_format(name: str = typer.Argument(..., help="Format name to show")) -> None:
    """Show details for a specific format.

    Displays the full configuration for a built-in or user-saved format,
    including column mappings, detection signature, and format options.

    Example:
        moneybin import show-format chase_credit
    """
    from moneybin.extractors.tabular.formats import load_builtin_formats

    builtin = load_builtin_formats()
    fmt = builtin.get(name)

    if fmt is None:
        logger.error(f"❌ Format not found: {name!r}")
        available = ", ".join(sorted(builtin.keys())) or "(none)"
        logger.info(f"💡 Available formats: {available}")
        raise typer.Exit(1)

    print(f"\nFormat: {fmt.name}")
    print(f"Institution: {fmt.institution_name}")
    print(f"File type: {fmt.file_type}")
    if fmt.delimiter:
        print(f"Delimiter: {fmt.delimiter!r}")
    print(f"Encoding: {fmt.encoding}")
    if fmt.skip_rows:
        print(f"Skip rows: {fmt.skip_rows}")
    if fmt.sheet:
        print(f"Sheet: {fmt.sheet}")
    print(f"Sign convention: {fmt.sign_convention}")
    print(f"Date format: {fmt.date_format}")
    print(f"Number format: {fmt.number_format}")
    print(f"Multi-account: {fmt.multi_account}")
    print(f"\nHeader signature: {fmt.header_signature}")
    print("\nField mapping:")
    for field, col in fmt.field_mapping.items():
        print(f"  {field} ← {col}")
    if fmt.skip_trailing_patterns:
        print(f"\nSkip trailing patterns: {fmt.skip_trailing_patterns}")
    print()


@app.command("delete-format")
def delete_format(
    name: str = typer.Argument(..., help="Format name to delete"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Delete a user-saved format.

    Removes a user-saved format from the database. Built-in formats cannot
    be deleted.

    Example:
        moneybin import delete-format my_custom_format
        moneybin import delete-format my_custom_format --yes
    """
    from moneybin.extractors.tabular.formats import load_builtin_formats

    # Check if it's a built-in format
    builtin = load_builtin_formats()
    if name in builtin:
        logger.error(f"❌ {name!r} is a built-in format and cannot be deleted")
        raise typer.Exit(1)

    if not yes:
        confirmed = typer.confirm(f"Delete format {name!r}?")
        if not confirmed:
            logger.info("Delete cancelled")
            raise typer.Exit(0)

    # TODO(Task 29): Wire through format persistence from database once
    # save/load from database is implemented. For now, user formats are not
    # yet persisted, so we can only report not-found.
    logger.warning(
        f"⚠️  Format {name!r} not found (user format persistence not yet implemented)"
    )
    raise typer.Exit(1)


@app.command("status")
def import_status() -> None:
    """Show a summary of all imported data: row counts, date ranges, and sources.

    Queries raw tables in DuckDB to display what has been imported so far.

    Example:
        moneybin import status
    """
    from moneybin.config import get_settings
    from moneybin.database import DatabaseKeyError, get_database

    db_path = get_settings().database.path

    if not db_path.exists():
        logger.warning(f"Database not found: {db_path}")
        logger.info("Run 'moneybin import file <path>' to import data first.")
        raise typer.Exit(1)

    try:
        db = get_database()
        _print_import_status(db)
    except DatabaseKeyError as e:
        logger.error(f"❌ Database is locked: {e}")
        logger.info("💡 Run `moneybin db unlock` to unlock the database first")
        raise typer.Exit(1) from e
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
