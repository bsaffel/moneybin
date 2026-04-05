"""Data extraction commands for MoneyBin CLI.

Power-user commands for extracting financial data from local files
(OFX/QFX bank statements, W-2 PDFs) with fine-grained options.

For the simple happy path, use 'moneybin import file' instead.
"""

import logging

import typer

app = typer.Typer(
    help="Parse local files (OFX, W-2, CSV) into structured data and Parquet",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


@app.command("ofx")
def extract_ofx(
    file_path: str = typer.Argument(..., help="Path to OFX/QFX file to extract"),
    institution: str = typer.Option(
        None, "--institution", "-i", help="Institution name override"
    ),
    skip_transform: bool = typer.Option(
        False,
        "--skip-transform",
        help="Skip rebuilding core tables after import",
    ),
) -> None:
    """Extract and load financial data from an OFX or QFX file.

    Parses the file, loads data into DuckDB raw tables, and rebuilds
    core tables via SQLMesh.

    Examples:
        moneybin data extract ofx ~/Downloads/WellsFargo_2025.qfx
        moneybin data extract ofx file.qfx --institution "Wells Fargo"
        moneybin data extract ofx file.qfx --skip-transform
    """
    from moneybin.config import get_database_path
    from moneybin.services.import_service import import_file

    try:
        result = import_file(
            db_path=get_database_path(),
            file_path=file_path,
            run_transforms=not skip_transform,
            institution=institution,
        )
        logger.info("✅ %s", result.summary())
    except FileNotFoundError as e:
        logger.error("❌ %s", e)
        raise typer.Exit(1) from e
    except ValueError as e:
        logger.error("❌ %s", e)
        raise typer.Exit(1) from e


@app.command("w2")
def extract_w2(
    file_path: str = typer.Argument(..., help="Path to W-2 PDF file to extract"),
    skip_transform: bool = typer.Option(
        False,
        "--skip-transform",
        help="Skip rebuilding core tables after import",
    ),
) -> None:
    """Extract and load wage/tax data from an IRS Form W-2 PDF.

    Parses the PDF, loads data into DuckDB raw tables, and rebuilds
    core tables via SQLMesh.

    Examples:
        moneybin data extract w2 ~/Downloads/2024_W2.pdf
        moneybin data extract w2 W2.pdf --skip-transform
    """
    from moneybin.config import get_database_path
    from moneybin.services.import_service import import_file

    try:
        result = import_file(
            db_path=get_database_path(),
            file_path=file_path,
            run_transforms=not skip_transform,
        )
        logger.info("✅ %s", result.summary())
    except FileNotFoundError as e:
        logger.error("❌ %s", e)
        raise typer.Exit(1) from e
    except ValueError as e:
        logger.error("❌ %s", e)
        raise typer.Exit(1) from e


@app.command("csv")
def extract_csv(
    file_path: str = typer.Argument(..., help="Path to CSV file to extract"),
    account_id: str = typer.Option(
        ..., "--account-id", "-a", help="Account identifier (e.g. chase-checking)"
    ),
    institution: str = typer.Option(
        None,
        "--institution",
        "-i",
        help="Institution profile name (auto-detects if omitted)",
    ),
    skip_transform: bool = typer.Option(
        False,
        "--skip-transform",
        help="Skip rebuilding core tables after import",
    ),
) -> None:
    """Extract and load transaction data from a CSV file.

    Parses the CSV using an institution-specific column mapping profile,
    loads data into DuckDB raw tables, and rebuilds core tables via SQLMesh.

    The profile is auto-detected from CSV headers. Use --institution to
    specify explicitly, or create new profiles via MCP.

    Examples:
        moneybin data extract csv transactions.csv --account-id chase-7022
        moneybin data extract csv statement.csv -a citi-card -i citi_credit
        moneybin data extract csv data.csv -a wf-savings --skip-transform
    """
    from moneybin.config import get_database_path
    from moneybin.services.import_service import import_file

    try:
        result = import_file(
            db_path=get_database_path(),
            file_path=file_path,
            run_transforms=not skip_transform,
            account_id=account_id,
            institution=institution,
        )
        logger.info("✅ %s", result.summary())
    except FileNotFoundError as e:
        logger.error("❌ %s", e)
        raise typer.Exit(1) from e
    except ValueError as e:
        logger.error("❌ %s", e)
        raise typer.Exit(1) from e
