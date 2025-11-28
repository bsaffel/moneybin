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
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    """Extract financial data from an OFX or QFX file.

    This command parses OFX/QFX (Quicken) financial data files and converts
    them to standardized Parquet format for loading into DuckDB.

    Args:
        file_path: Path to the OFX/QFX file
        verbose: Enable debug level logging
    """
    setup_logging(cli_mode=True, verbose=verbose)

    logger.info(f"OFX extraction from: {file_path}")
    logger.warning("⚠️  OFX extraction not yet implemented")
    logger.info("This feature will parse OFX/QFX files and save to Parquet")
    raise typer.Exit(1)


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
