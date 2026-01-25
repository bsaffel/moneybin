"""Data extraction commands for MoneyBin CLI.

This module provides commands for extracting financial data from local files
(CSV, Excel, OFX, PDF statements, etc.).

For syncing data from external services (Plaid, Yodlee), use 'moneybin sync' instead.
"""

import logging

import typer

from moneybin.logging import setup_logging

app = typer.Typer(help="Extract financial data from local files")
logger = logging.getLogger(__name__)


@app.command("csv")
def extract_csv(
    file_path: str = typer.Argument(..., help="Path to CSV file to extract"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    """Extract financial data from a CSV file.

    This command parses CSV bank statements and converts them to standardized
    Parquet format for loading into DuckDB.

    Args:
        file_path: Path to the CSV file
        verbose: Enable debug level logging
    """
    setup_logging(cli_mode=True, verbose=verbose)

    logger.info(f"CSV extraction from: {file_path}")
    logger.warning("⚠️  CSV extraction not yet implemented")
    logger.info("This feature will parse CSV bank statements and save to Parquet")
    raise typer.Exit(1)


@app.command("excel")
def extract_excel(
    file_path: str = typer.Argument(..., help="Path to Excel file to extract"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    """Extract financial data from an Excel file.

    This command parses Excel-format financial data and converts it to
    standardized Parquet format for loading into DuckDB.

    Args:
        file_path: Path to the Excel file
        verbose: Enable debug level logging
    """
    setup_logging(cli_mode=True, verbose=verbose)

    logger.info(f"Excel extraction from: {file_path}")
    logger.warning("⚠️  Excel extraction not yet implemented")
    logger.info("This feature will parse Excel financial data and save to Parquet")
    raise typer.Exit(1)


@app.command("ofx")
def extract_ofx(
    file_path: str = typer.Argument(..., help="Path to OFX/QFX file to extract"),
    institution_name: str = typer.Option(
        None, "--institution", "-i", help="Institution name override"
    ),
    copy_to_raw: bool = typer.Option(
        True, "--copy/--no-copy", help="Copy source file to data/raw/ofx directory"
    ),
    load_to_db: bool = typer.Option(
        True, "--load/--no-load", help="Load extracted data to DuckDB raw tables"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    """Extract financial data from an OFX or QFX file.

    This command parses OFX/QFX (Quicken) financial data files and:
    1. Copies the file to data/raw/ofx/ (optional)
    2. Extracts institutions, accounts, transactions, and balances
    3. Saves extracted data as Parquet files
    4. Loads data into DuckDB raw tables (optional)

    Examples:
        # Extract and load Wells Fargo data
        moneybin extract ofx ~/Downloads/WellsFargo_2025.qfx

        # Extract with custom institution name
        moneybin extract ofx file.qfx --institution "Wells Fargo"

        # Extract without loading to database
        moneybin extract ofx file.qfx --no-load

    Args:
        file_path: Path to the OFX/QFX file
        institution_name: Optional institution name override
        copy_to_raw: Copy source file to data/raw/ofx
        load_to_db: Load extracted data to DuckDB
        verbose: Enable debug level logging
    """
    from pathlib import Path

    from moneybin.config import get_database_path, get_raw_data_path
    from moneybin.extractors.ofx_extractor import OFXExtractor
    from moneybin.loaders.ofx_loader import OFXLoader
    from moneybin.utils.file import copy_to_raw as copy_file_to_raw

    setup_logging(cli_mode=True, verbose=verbose)

    source_file = Path(file_path)

    # Validate file exists
    if not source_file.exists():
        logger.error(f"File not found: {source_file}")
        raise typer.Exit(1)

    # Validate file extension
    if source_file.suffix.lower() not in (".ofx", ".qfx"):
        logger.warning(
            f"File has extension {source_file.suffix}, expected .ofx or .qfx"
        )

    try:
        # Step 1: Copy file to raw data directory (optional)
        working_file = source_file
        if copy_to_raw:
            logger.info("📁 Copying file to data/raw/ofx/...")
            working_file = copy_file_to_raw(
                source_file, file_type="ofx", base_data_path=get_raw_data_path()
            )
            logger.info(f"✅ Copied to: {working_file}")

        # Step 2: Extract data from OFX file
        logger.info(f"📊 Extracting OFX data from: {working_file}")
        extractor = OFXExtractor()
        data = extractor.extract_from_file(working_file, institution_name)

        # Log extraction summary
        logger.info("✅ Extraction complete:")
        logger.info(f"   Institutions: {len(data['institutions'])} rows")
        logger.info(f"   Accounts: {len(data['accounts'])} rows")
        logger.info(f"   Transactions: {len(data['transactions'])} rows")
        logger.info(f"   Balances: {len(data['balances'])} rows")

        # Step 3: Load to DuckDB (optional)
        if load_to_db:
            logger.info("💾 Loading data to DuckDB raw tables...")
            loader = OFXLoader(get_database_path())
            row_counts = loader.load_data(data)

            logger.info("✅ Data loaded to DuckDB:")
            for table, count in row_counts.items():
                logger.info(f"   raw.ofx_{table}: {count} rows")

        logger.info("🎉 OFX import complete!")

    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        raise typer.Exit(1) from e
    except ValueError as e:
        logger.error(f"Invalid OFX file: {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if verbose:
            logger.exception("Full traceback:")
        raise typer.Exit(1) from e


@app.command("pdf")
def extract_pdf(
    file_path: str = typer.Argument(..., help="Path to PDF statement to extract"),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    """Extract financial data from a PDF statement.

    This command uses OCR and table extraction to parse PDF bank statements
    and convert them to standardized Parquet format for loading into DuckDB.

    Args:
        file_path: Path to the PDF file
        verbose: Enable debug level logging
    """
    setup_logging(cli_mode=True, verbose=verbose)

    logger.info(f"PDF extraction from: {file_path}")
    logger.warning("⚠️  PDF extraction not yet implemented")
    logger.info("This feature will parse PDF statements and save to Parquet")
    raise typer.Exit(1)


@app.command("w2")
def extract_w2(
    file_path: str = typer.Argument(..., help="Path to W2 PDF file to extract"),
    tax_year: int = typer.Option(
        None,
        "--year",
        "-y",
        help="Tax year (e.g., 2024). If not provided, will auto-detect.",
    ),
    copy_to_raw: bool = typer.Option(
        True, "--copy/--no-copy", help="Copy source file to data/raw/w2 directory"
    ),
    load_to_db: bool = typer.Option(
        True, "--load/--no-load", help="Load extracted data to DuckDB raw tables"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    """Extract wage and tax data from an IRS Form W-2 PDF.

    This command parses W2 PDF files and:
    1. Copies the file to data/raw/w2/ (optional)
    2. Extracts employee, employer, wages, and tax information
    3. Saves extracted data as Parquet files
    4. Loads data into DuckDB raw tables (optional)

    The data model balances structure with flexibility:
    - Core fields (wages, taxes) as typed columns for easy querying
    - State/local tax info as JSON (supports multiple states)
    - Optional boxes as JSON (only stored when present)

    Examples:
        # Extract and load 2024 W2 (auto-detect year)
        moneybin extract w2 ~/Downloads/2024_W2.pdf

        # Extract with explicit year
        moneybin extract w2 W2.pdf --year 2024

        # Extract without loading to database
        moneybin extract w2 W2.pdf --no-load

        # Extract without copying source file
        moneybin extract w2 W2.pdf --no-copy

    Args:
        file_path: Path to the W2 PDF file
        tax_year: Optional tax year (auto-detected if not provided)
        copy_to_raw: Copy source file to data/raw/w2
        load_to_db: Load extracted data to DuckDB
        verbose: Enable debug level logging
    """
    from pathlib import Path

    from moneybin.config import get_database_path, get_raw_data_path
    from moneybin.extractors.w2_extractor import W2Extractor
    from moneybin.loaders.w2_loader import W2Loader
    from moneybin.utils.file import copy_to_raw as copy_file_to_raw

    setup_logging(cli_mode=True, verbose=verbose)

    source_file = Path(file_path)

    # Validate file exists
    if not source_file.exists():
        logger.error(f"File not found: {source_file}")
        raise typer.Exit(1)

    # Validate file extension
    if source_file.suffix.lower() != ".pdf":
        logger.warning(f"File has extension {source_file.suffix}, expected .pdf")

    try:
        # Step 1: Copy file to raw data directory (optional)
        working_file = source_file
        if copy_to_raw:
            logger.info("📁 Copying file to data/raw/w2/...")
            working_file = copy_file_to_raw(
                source_file, file_type="w2", base_data_path=get_raw_data_path()
            )
            logger.info(f"✅ Copied to: {working_file}")

        # Step 2: Extract data from W2 PDF
        logger.info(f"📊 Extracting W2 data from: {working_file}")
        if tax_year:
            logger.info(f"   Using tax year: {tax_year}")
        extractor = W2Extractor()
        data = extractor.extract_from_file(working_file, tax_year)

        # Log extraction summary
        w2_info = data.row(0, named=True)
        logger.info("✅ Extraction complete:")
        logger.info(f"   Tax Year: {w2_info['tax_year']}")
        logger.info(
            f"   Employee: {w2_info['employee_first_name']} {w2_info['employee_last_name']}"
        )
        logger.info(f"   Employer: {w2_info['employer_name']}")
        logger.info(f"   Wages: ${w2_info['wages']:,.2f}")
        logger.info(f"   Federal Tax: ${w2_info['federal_income_tax']:,.2f}")

        # Step 3: Load to DuckDB (optional)
        if load_to_db:
            logger.info("💾 Loading data to DuckDB raw tables...")
            loader = W2Loader(get_database_path())
            row_count = loader.load_data(data)

            logger.info(f"✅ Data loaded to DuckDB: raw.w2_forms ({row_count} rows)")

        logger.info("🎉 W2 import complete!")

    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        raise typer.Exit(1) from e
    except ValueError as e:
        logger.error(f"Invalid W2 file: {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if verbose:
            logger.exception("Full traceback:")
        raise typer.Exit(1) from e
