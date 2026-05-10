# ruff: noqa: A001 — "import" shadows builtin, but it's a Typer subcommand name
"""Import commands for MoneyBin CLI.

This module provides the user-facing import workflow: auto-detect file type,
extract, load into DuckDB, and optionally run SQLMesh transforms.
Also provides history, revert, preview, and format management subcommands.
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, get_args

import typer

from moneybin.cli.commands import import_inbox, import_labels
from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json
from moneybin.extractors.tabular.formats import NumberFormatType, SignConventionType

if TYPE_CHECKING:
    from moneybin.database import Database
    from moneybin.extractors.tabular.formats import TabularFormat

app = typer.Typer(
    help=(
        "Import financial files (OFX/QFX, CSV/TSV/Excel/Parquet, W-2 PDFs) "
        "into MoneyBin"
    ),
    no_args_is_help=True,
)
formats_app = typer.Typer(
    help="Manage tabular import format definitions",
    no_args_is_help=True,
)
app.add_typer(formats_app, name="formats")
app.add_typer(import_inbox.app, name="inbox", help="Drain the watched import inbox")
app.add_typer(import_labels.app, name="labels", help="Manage labels on imports")
logger = logging.getLogger(__name__)

_VALID_SIGN_CONVENTIONS = frozenset(get_args(SignConventionType))
_VALID_NUMBER_FORMATS = frozenset(get_args(NumberFormatType))


def _parse_overrides(override: list[str] | None) -> dict[str, str] | None:
    """Parse and validate --override field=column values."""
    if not override:
        return None
    result: dict[str, str] = {}
    for raw in override:
        if "=" not in raw:
            logger.error(
                f"❌ Invalid --override format (expected field=column): {raw!r}"
            )
            raise typer.Exit(1)
        field, _, col = raw.partition("=")
        result[field.strip()] = col.strip()
    return result


def _load_all_formats(
    db: Database | None = None,
) -> tuple[dict[str, TabularFormat], dict[str, TabularFormat]]:
    """Load built-in + user-saved formats, returning (all_formats, builtin).

    Falls back to built-in only if DB is unavailable.
    """
    from moneybin.extractors.tabular.formats import (
        load_builtin_formats,
        load_formats_from_db,
        merge_formats,
    )

    builtin = load_builtin_formats()
    user_formats: dict[str, TabularFormat] = {}
    if db is not None:
        try:
            user_formats = load_formats_from_db(db)
        except Exception:  # noqa: BLE001, S110 — DB table may not exist yet
            logger.debug("Could not load user formats from DB, using built-in only")
    all_formats = merge_formats(builtin, user_formats)
    return all_formats, builtin


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
    institution: str | None = typer.Option(
        None,
        "--institution",
        "-i",
        help=(
            "Institution override for OFX/QFX/QBO files. Consulted only when "
            "the file's <FI><ORG>, FID lookup, and filename heuristic all "
            "yield nothing. For CSV/tabular files, selects the format profile."
        ),
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-F",
        help="Re-import a file already in the import log (creates a new batch).",
    ),
    account_id: str | None = typer.Option(
        None, "--account-id", "-a", help="Account identifier (bypasses name matching)"
    ),
    account_name: str | None = typer.Option(
        None,
        "--account-name",
        "-n",
        help="Account name for single-account tabular files",
    ),
    format_name: str | None = typer.Option(
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
    sign: str | None = typer.Option(
        None,
        "--sign",
        help=(
            "Sign convention override: negative_is_expense, "
            "negative_is_income, split_debit_credit"
        ),
    ),
    date_format: str | None = typer.Option(
        None,
        "--date-format",
        help="Date format override (strptime format string, e.g. %%Y-%%m-%%d)",
    ),
    number_format: str | None = typer.Option(
        None,
        "--number-format",
        help="Number format override: us, european, swiss_french, zero_decimal",
    ),
    sheet: str | None = typer.Option(
        None, "--sheet", help="Excel sheet name (default: auto-select largest)"
    ),
    delimiter: str | None = typer.Option(
        None, "--delimiter", help="Explicit delimiter for text formats"
    ),
    encoding: str | None = typer.Option(
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
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Auto-accept the top fuzzy account match without prompting",
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
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.services.import_service import ImportService

    source = Path(file_path)

    if not source.exists():
        logger.error(f"❌ File not found: {source}")
        raise typer.Exit(1)

    overrides = _parse_overrides(override)

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

    interactive = not yes and sys.stdin.isatty()

    try:
        with handle_cli_errors() as db:
            result = ImportService(db).import_file(
                file_path=source,
                apply_transforms=not skip_transform,
                institution=institution,
                force=force,
                interactive=interactive,
                account_id=account_id,
                account_name=account_name,
                format_name=format_name,
                overrides=overrides,
                sign=sign or None,
                date_format=date_format or None,
                number_format=number_format or None,
                save_format=save_format,
                sheet=sheet,
                delimiter=delimiter,
                encoding=encoding,
                no_row_limit=no_row_limit,
                no_size_limit=no_size_limit,
                auto_accept=yes,
            )
            logger.info(f"✅ {result.summary()}")
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except PermissionError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("history")
def import_history(
    limit: int = typer.Option(20, "--limit", "-n", help="Max records to show"),
    import_id: str | None = typer.Option(
        None, "--import-id", help="Show details for a specific import"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List recent imports with batch details.

    Shows import ID, source file, status, row counts, and detection confidence
    for each completed import batch.

    Examples:
        moneybin import history
        moneybin import history --limit 50
        moneybin import history --import-id abc123
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.loaders.tabular_loader import TabularLoader

    with handle_cli_errors() as db:
        loader = TabularLoader(db)
        records = loader.get_import_history(limit=limit, import_id=import_id)

    if output == OutputFormat.JSON:
        emit_json("imports", records)
        return

    if not records:
        if not quiet:
            if import_id:
                logger.warning(f"⚠️  No import found with ID: {import_id}")
            else:
                logger.warning("⚠️  No import history found")
        return

    typer.echo(
        f"\n{'Import ID':<38} {'Status':<10} {'Imported':>8} {'Rejected':>8}  {'Source File'}"
    )
    typer.echo("-" * 100)
    for rec in records:
        imp_id = str(rec.get("import_id", ""))
        status = str(rec.get("status", ""))
        rows_imported = rec.get("rows_imported") or 0
        rows_rejected = rec.get("rows_rejected") or 0
        source_file = str(rec.get("source_file", ""))
        # Truncate source file path for display
        display_path = Path(source_file).name if source_file else ""
        typer.echo(
            f"{imp_id:<38} {status:<10} {rows_imported:>8} {rows_rejected:>8}  "
            f"{display_path}"
        )

    if import_id and records:
        rec = records[0]
        typer.echo("\nDetails:")
        for key, value in rec.items():
            if value is not None:
                typer.echo(f"  {key}: {value}")
    typer.echo()


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
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.loaders.tabular_loader import TabularLoader

    if not yes:
        confirmed = typer.confirm(
            f"Revert import {import_id[:8]}...? This will delete all rows from "
            f"this batch and cannot be undone."
        )
        if not confirmed:
            logger.info("Revert cancelled")
            raise typer.Exit(0)

    with handle_cli_errors() as db:
        loader = TabularLoader(db)
        result = loader.revert_import(import_id)

    status = result.get("status")
    if status == "not_found":
        logger.error(f"❌ {result.get('reason', 'Import not found')}")
        raise typer.Exit(1)
    elif status == "superseded":
        logger.error(f"❌ {result.get('reason', 'Import was superseded')}")
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
    format_name: str | None = typer.Option(
        None,
        "--format",
        "-f",
        help="Use a specific named format (bypass auto-detection)",
    ),
    sheet: str | None = typer.Option(
        None, "--sheet", help="Excel sheet name (default: auto-select largest)"
    ),
    delimiter: str | None = typer.Option(
        None, "--delimiter", help="Explicit delimiter for text formats"
    ),
    encoding: str | None = typer.Option(
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
    from moneybin.extractors.tabular.readers import read_file

    source = Path(file_path)

    if not source.exists():
        logger.error(f"❌ File not found: {source}")
        raise typer.Exit(1)

    overrides = _parse_overrides(override)

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

        # Stage 3: Column mapping — load built-in + user-saved formats
        matched_format = None
        from moneybin.database import get_database

        try:
            preview_db: Database | None = get_database()
        except Exception:  # noqa: BLE001 — DB may not exist yet; use built-in only
            preview_db = None
        all_formats, _ = _load_all_formats(preview_db)
        if format_name:
            matched_format = all_formats.get(format_name)
            if matched_format is None:
                logger.warning(
                    f"⚠️  Format {format_name!r} not found in available formats"
                )
        else:
            headers = list(df.columns)
            for fmt in all_formats.values():
                if fmt.matches_headers(headers):
                    matched_format = fmt
                    break

        typer.echo(f"\nFile: {source.name}")
        typer.echo(f"Type: {format_info.file_type}")
        if format_info.delimiter:
            typer.echo(f"Delimiter: {format_info.delimiter!r}")
        typer.echo(f"Encoding: {format_info.encoding}")
        typer.echo(f"Rows: {len(df):,}")
        if read_result.rows_skipped_trailing:
            typer.echo(f"Trailing rows skipped: {read_result.rows_skipped_trailing}")
        typer.echo(f"Columns ({len(df.columns)}): {', '.join(df.columns)}")

        if matched_format:
            typer.echo(
                f"\nMatched format: {matched_format.name} ({matched_format.institution_name})"
            )
            typer.echo(f"Sign convention: {matched_format.sign_convention}")
            typer.echo(f"Date format: {matched_format.date_format}")
            typer.echo(f"Number format: {matched_format.number_format}")
            typer.echo("\nColumn mapping:")
            for field, col in matched_format.field_mapping.items():
                typer.echo(f"  {field} ← {col}")
        else:
            mapping_result = map_columns(df, overrides=overrides)
            typer.echo(f"\nDetected mapping (confidence: {mapping_result.confidence}):")
            for field, col in mapping_result.field_mapping.items():
                typer.echo(f"  {field} ← {col}")
            if mapping_result.sign_convention:
                typer.echo(f"Sign convention: {mapping_result.sign_convention}")
            if mapping_result.date_format:
                typer.echo(f"Date format: {mapping_result.date_format}")
            if mapping_result.number_format:
                typer.echo(f"Number format: {mapping_result.number_format}")

        # Show sample rows
        sample_n = min(5, len(df))
        typer.echo(f"\nSample ({sample_n} rows):")
        typer.echo(df.head(sample_n))
        typer.echo()

    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@formats_app.command("list")
def formats_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List all formats (built-in and user-saved).

    Displays format name, institution, sign convention, and date format
    for all available import formats.

    Example:
        moneybin import formats list
    """
    from moneybin.database import get_database

    try:
        db: Database | None = get_database()
    except Exception:  # noqa: BLE001 — DB may not exist yet; show built-in only
        db = None

    all_formats, builtin = _load_all_formats(db)

    if output == OutputFormat.JSON:
        formats_payload = [
            {
                "name": fmt.name,
                "institution": fmt.institution_name,
                "sign_convention": fmt.sign_convention,
                "date_format": fmt.date_format,
                "source": "builtin" if fmt.name in builtin else "user",
            }
            for fmt in sorted(all_formats.values(), key=lambda f: f.name)
        ]
        emit_json("formats", formats_payload)
        return

    if not all_formats:
        if not quiet:
            logger.warning("⚠️  No formats found")
        return

    typer.echo(
        f"\n{'Name':<24} {'Institution':<28} {'Sign Convention':<24} {'Date Format'}"
    )
    typer.echo("-" * 100)
    for fmt in sorted(all_formats.values(), key=lambda f: f.name):
        source_tag = " (user)" if fmt.name not in builtin else ""
        typer.echo(
            f"{fmt.name:<24} {fmt.institution_name:<28} "
            f"{fmt.sign_convention:<24} {fmt.date_format}{source_tag}"
        )
    if not quiet:
        n_builtin = len(builtin)
        n_user = len(all_formats) - len(builtin)
        typer.echo(f"\n{n_builtin} built-in, {n_user} user-saved format(s)\n")


@formats_app.command("show")
def formats_show(
    name: str = typer.Argument(..., help="Format name to show"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — show has no info chatter; only data lines
) -> None:
    """Show details for a specific format.

    Displays the full configuration for a built-in or user-saved format,
    including column mappings, detection signature, and format options.

    Example:
        moneybin import formats show chase_credit
    """
    from moneybin.database import get_database

    try:
        db: Database | None = get_database()
    except Exception:  # noqa: BLE001 — DB may not exist yet; show built-in only
        db = None

    all_formats, _ = _load_all_formats(db)
    fmt = all_formats.get(name)

    if fmt is None:
        logger.error(f"❌ Format not found: {name!r}")
        available = ", ".join(sorted(all_formats.keys())) or "(none)"
        logger.info(f"💡 Available formats: {available}")
        raise typer.Exit(1)

    if output == OutputFormat.JSON:
        payload = {
            "name": fmt.name,
            "institution": fmt.institution_name,
            "file_type": fmt.file_type,
            "delimiter": fmt.delimiter,
            "encoding": fmt.encoding,
            "skip_rows": fmt.skip_rows,
            "sheet": fmt.sheet,
            "sign_convention": fmt.sign_convention,
            "date_format": fmt.date_format,
            "number_format": fmt.number_format,
            "multi_account": fmt.multi_account,
            "header_signature": fmt.header_signature,
            "field_mapping": dict(fmt.field_mapping),
            "skip_trailing_patterns": fmt.skip_trailing_patterns,
        }
        emit_json("format", payload)
        return

    typer.echo(f"\nFormat: {fmt.name}")
    typer.echo(f"Institution: {fmt.institution_name}")
    typer.echo(f"File type: {fmt.file_type}")
    if fmt.delimiter:
        typer.echo(f"Delimiter: {fmt.delimiter!r}")
    typer.echo(f"Encoding: {fmt.encoding}")
    if fmt.skip_rows:
        typer.echo(f"Skip rows: {fmt.skip_rows}")
    if fmt.sheet:
        typer.echo(f"Sheet: {fmt.sheet}")
    typer.echo(f"Sign convention: {fmt.sign_convention}")
    typer.echo(f"Date format: {fmt.date_format}")
    typer.echo(f"Number format: {fmt.number_format}")
    typer.echo(f"Multi-account: {fmt.multi_account}")
    typer.echo(f"\nHeader signature: {fmt.header_signature}")
    typer.echo("\nField mapping:")
    for field, col in fmt.field_mapping.items():
        typer.echo(f"  {field} ← {col}")
    if fmt.skip_trailing_patterns:
        typer.echo(f"\nSkip trailing patterns: {fmt.skip_trailing_patterns}")
    typer.echo()


@formats_app.command("delete")
def formats_delete(
    name: str = typer.Argument(..., help="Format name to delete"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Delete a user-saved format.

    Removes a user-saved format from the database. Built-in formats cannot
    be deleted.

    Example:
        moneybin import formats delete my_custom_format
        moneybin import formats delete my_custom_format --yes
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.extractors.tabular.formats import (
        delete_format_from_db,
        load_builtin_formats,
    )

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

    with handle_cli_errors() as db:
        deleted = delete_format_from_db(db, name)

    if not deleted:
        logger.error(f"❌ Format {name!r} not found")
        raise typer.Exit(1)
    logger.info(f"✅ Deleted format {name!r}")


@app.command("status")
def import_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show a summary of all imported data: row counts, date ranges, and sources.

    Queries raw tables in DuckDB to display what has been imported so far.

    Example:
        moneybin import status
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.config import get_settings

    db_path = get_settings().database.path

    if not db_path.exists():
        if output == OutputFormat.JSON:
            typer.echo(
                json.dumps(
                    {
                        "database": str(db_path),
                        "tables": [],
                        "exists": False,
                        "error": "database not found",
                    },
                    indent=2,
                    default=str,
                )
            )
        elif not quiet:
            logger.warning(f"Database not found: {db_path}")
            logger.info("Run 'moneybin import file <path>' to import data first.")
        # Both modes exit non-zero so machine consumers can detect missing/
        # uninitialized state. The JSON payload carries the same signal as
        # the human warning; the exit code carries it for scripts.
        raise typer.Exit(1)

    try:
        with handle_cli_errors() as db:
            rows = _collect_import_status(db)
    except Exception as e:  # noqa: BLE001 — surface connection errors generically
        logger.error(f"❌ Could not open database: {e}")
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        typer.echo(
            json.dumps(
                {
                    "database": str(db_path),
                    "tables": [asdict(r) for r in rows],
                    "exists": True,
                },
                indent=2,
                default=str,
            )
        )
        return

    if not rows:
        if not quiet:
            typer.echo("\nNo imported data found.")
            typer.echo("   Run 'moneybin import file <path>' to get started.")
        return

    if not quiet:
        typer.echo("\nImported Data Summary")
        typer.echo("=" * 60)

    for row in rows:
        date_info = ""
        if row.date_min is not None:
            date_info = f"  ({row.date_min} to {row.date_max})"
        typer.echo(f"  {row.schema}.{row.table}: {row.rows:,} rows{date_info}")

    if not quiet:
        typer.echo()


@dataclass(frozen=True, slots=True)
class _ImportStatusRow:
    schema: str
    table: str
    rows: int
    date_min: date | None
    date_max: date | None


def _collect_import_status(db: Database) -> list[_ImportStatusRow]:
    """Query raw tables and return per-table row counts and date ranges."""
    tables = db.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw'
        ORDER BY table_name
    """).fetchall()

    from sqlglot import exp

    results: list[_ImportStatusRow] = []
    for schema, table in tables:
        safe_schema = exp.to_identifier(schema, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
        safe_table = exp.to_identifier(table, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
        row_count = db.execute(
            f"SELECT COUNT(*) FROM {safe_schema}.{safe_table}"  # noqa: S608 — sqlglot-quoted catalog identifiers
        ).fetchone()
        count = row_count[0] if row_count else 0

        date_min: date | None = None
        date_max: date | None = None
        if "transaction" in table:
            date_col = "date_posted" if "ofx" in table else "transaction_date"
            safe_date_col = exp.to_identifier(date_col, quoted=True).sql("duckdb")  # type: ignore[reportUnknownMemberType]  # sqlglot has no stubs
            try:
                dates = db.execute(
                    f"SELECT MIN(CAST({safe_date_col} AS DATE)), MAX(CAST({safe_date_col} AS DATE)) FROM {safe_schema}.{safe_table}"  # noqa: S608 — sqlglot-quoted catalog identifiers; date_col from hardcoded map
                ).fetchone()
                if dates and dates[0]:
                    date_min, date_max = dates[0], dates[1]
            except Exception:  # noqa: BLE001 — column may not exist in all tables
                logger.debug(f"Could not get date range for {schema}.{table}")

        results.append(
            _ImportStatusRow(
                schema=schema,
                table=table,
                rows=count,
                date_min=date_min,
                date_max=date_max,
            )
        )
    return results
