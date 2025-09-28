"""Data extraction commands for MoneyBin CLI.

This module provides commands for extracting financial data from various sources,
primarily focusing on Plaid API integration.
"""

import logging
from pathlib import Path

import typer

from moneybin.extractors.plaid_extractor import (
    PlaidConnectionManager,
    PlaidExtractor,
)
from moneybin.logging import setup_logging
from moneybin.utils.secrets_manager import setup_secure_environment

app = typer.Typer(help="Extract financial data from various sources")
logger = logging.getLogger(__name__)


@app.command("plaid")
def extract_plaid(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
    setup_env: bool = typer.Option(
        False, "--setup-env", help="Create sample .env file and exit"
    ),
) -> None:
    """Extract financial data from all configured Plaid institutions.

    This command will:
    1. Validate Plaid credentials
    2. Extract accounts and transactions from all configured institutions
    3. Save raw data to data/raw/plaid/ directory

    Args:
        verbose: Enable debug level logging
        setup_env: Create sample .env file and exit
    """
    setup_logging(cli_mode=True, verbose=verbose)

    logger.info("Starting MoneyBin Plaid API extraction (Modern SDK)")

    try:
        # Create sample environment file if needed or requested
        if setup_env or not Path(".env").exists():
            setup_secure_environment()
            logger.info("Created sample .env file - please configure your credentials")
            if setup_env:
                return

        # Initialize extractor
        extractor = PlaidExtractor()
        logger.info("✅ Plaid extractor initialized successfully")

        # Test credential validation
        try:
            credentials = extractor.credentials
            logger.info(f"✅ Using Plaid {credentials.environment} environment")
        except Exception as e:
            logger.error(f"❌ Credential validation failed: {e}")
            logger.error("Please check your .env file configuration")
            raise typer.Exit(1) from e

        # Initialize connection manager
        manager = PlaidConnectionManager()

        # Extract from all configured institutions
        logger.info("Starting extraction from all configured institutions...")
        all_data = manager.extract_all_institutions()

        if not all_data:
            logger.warning("No data extracted - check your access tokens")
            logger.info("To add institutions, set environment variables like:")
            logger.info("PLAID_TOKEN_WELLS_FARGO=access-sandbox-xxx")
            raise typer.Exit(1)

        # Display summary
        logger.info("✅ Extraction completed successfully")
        logger.info("📁 Raw data saved to: data/raw/plaid/")

    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}")
        raise typer.Exit(1) from e


@app.command("all")
def extract_all(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
) -> None:
    """Extract data from all configured sources.

    Currently this is equivalent to 'extract plaid' but provides a foundation
    for adding additional data sources in the future.

    Args:
        verbose: Enable debug level logging
    """
    # For now, just call the Plaid extraction
    extract_plaid(verbose=verbose, setup_env=False)
