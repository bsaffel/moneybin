"""Data synchronization commands for MoneyBin CLI.

This module provides commands for syncing financial data from external services
through the MoneyBin Sync service (Plaid, Yodlee, etc.).
"""

import logging
from pathlib import Path

import typer

from moneybin.config import get_current_profile
from moneybin.connectors.plaid_sync import PlaidConnectionManager
from moneybin.logging import setup_logging
from moneybin.utils.secrets_manager import setup_secure_environment

app = typer.Typer(help="Sync financial data from external services")
logger = logging.getLogger(__name__)


@app.command("plaid")
def sync_plaid(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
    setup_env: bool = typer.Option(
        False, "--setup-env", help="Create sample .env file and exit"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force full sync, bypassing incremental logic",
    ),
) -> None:
    """Sync financial data from all configured Plaid institutions.

    This command uses the MoneyBin Sync service to:
    1. Validate Plaid credentials (server-side)
    2. Sync accounts and transactions from all configured institutions
    3. Save raw data to profile-specific directory (data/{profile}/raw/plaid/)

    By default, uses incremental synchronization (only new complete days).
    Use --force to sync the full lookback period regardless of previous syncs.

    Args:
        verbose: Enable debug level logging
        setup_env: Create sample .env file and exit
        force: Force full sync, bypassing incremental logic
    """
    setup_logging(cli_mode=True, verbose=verbose)

    profile = get_current_profile()
    logger.info(f"Starting MoneyBin Plaid sync (Profile: {profile})")

    try:
        # Create sample environment file if needed or requested
        if setup_env or not Path(".env").exists():
            setup_secure_environment()
            logger.info("Created sample .env file - please configure your credentials")
            if setup_env:
                return

        # Initialize connection manager (which validates Plaid configuration)
        manager = PlaidConnectionManager()

        # Sync from all configured institutions
        if force:
            logger.info("🔄 Starting FORCED sync from all configured institutions...")
            logger.info(
                "⚠️  This will sync the full lookback period regardless of previous syncs"
            )
        else:
            logger.info(
                "📈 Starting INCREMENTAL sync from all configured institutions..."
            )
            logger.info(
                "✨ Only new complete days will be synced (use --force for full sync)"
            )

        all_data = manager.extract_all_institutions(force_extraction=force)

        if not all_data:
            logger.warning("No data synced - check your access tokens")
            logger.info("To add institutions, set environment variables like:")
            logger.info("PLAID_TOKEN_WELLS_FARGO=access-sandbox-xxx")
            raise typer.Exit(1)

        # Display summary
        logger.info("✅ Sync completed successfully")
        logger.info(f"📁 Raw data saved to: data/{profile}/raw/plaid/")

    except Exception as e:
        logger.error(f"❌ Sync failed: {e}")
        raise typer.Exit(1) from e


@app.command("all")
def sync_all(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose logging"
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force full sync, bypassing incremental logic",
    ),
) -> None:
    """Sync data from all configured external services.

    Currently this is equivalent to 'sync plaid' but provides a foundation
    for adding additional sync connectors in the future (Yodlee, etc.).

    Args:
        verbose: Enable debug level logging
        force: Force full sync, bypassing incremental logic
    """
    # For now, just call the Plaid sync
    sync_plaid(verbose=verbose, setup_env=False, force=force)
