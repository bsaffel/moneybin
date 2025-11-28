"""Credential management commands for MoneyBin CLI.

This module provides commands for managing API credentials, environment setup,
and credential validation across all integrated services.
"""

import logging
from pathlib import Path

import typer

# Import the actual implementation classes
from moneybin.config import get_current_profile
from moneybin.logging import setup_logging
from moneybin.utils.secrets_manager import SecretsManager, setup_secure_environment

app = typer.Typer(help="Manage API credentials and environment configuration")
logger = logging.getLogger(__name__)


@app.command("setup")
def setup(
    force: bool = typer.Option(
        False, "--force", "-f", help="Overwrite existing .env file"
    ),
) -> None:
    """Set up secure environment configuration.

    Creates necessary directories and environment files for secure credential storage.

    Args:
        force: Overwrite existing .env file if it exists
    """
    if Path(".env").exists() and not force:
        logger.info("✅ .env file already exists (use --force to overwrite)")
        return

    setup_secure_environment()
    logger.info("✅ Secure environment setup completed")


@app.command("validate")
def validate() -> None:
    """Validate all configured credentials and API connections."""
    # Set up logging for this command
    setup_logging(cli_mode=True)

    profile = get_current_profile()
    logger.info(f"Validating credentials (Profile: {profile})")

    # Test credentials loading
    manager = SecretsManager()
    validation_results = manager.validate_all_credentials()

    logger.info("🔐 Credential Validation Results:")
    for service, is_valid in validation_results.items():
        status = "✅ Valid" if is_valid else "❌ Invalid/Missing"
        logger.info(f"  {service.capitalize()}: {status}")

    # Summary
    valid_count = sum(validation_results.values())
    total_count = len(validation_results)

    if valid_count == total_count:
        logger.info(f"✅ All {total_count} credential(s) are valid")
    else:
        logger.warning(f"⚠️  {valid_count}/{total_count} credential(s) are valid")
        logger.info(
            "Check your .env file or run 'moneybin credentials setup' to create a template"
        )


@app.command("list-services")
def list_services() -> None:
    """List all supported services and their credential requirements."""
    # Set up logging for this command
    setup_logging(cli_mode=True)

    logger.info("🔧 Supported Services:")
    logger.info("  Plaid API:")
    logger.info("    - PLAID_CLIENT_ID")
    logger.info("    - PLAID_SECRET")
    logger.info("    - PLAID_ENVIRONMENT (sandbox/development/production)")
    logger.info("    - PLAID_TOKEN_<INSTITUTION_NAME> (for each institution)")

    logger.info("\n💡 Example institution tokens:")
    logger.info("    PLAID_TOKEN_WELLS_FARGO=access-sandbox-xxx")
    logger.info("    PLAID_TOKEN_CHASE=access-sandbox-yyy")


@app.command("validate-plaid")
def validate_plaid() -> None:
    """Validate Plaid API credentials specifically."""
    from moneybin.extractors.plaid_extractor import PlaidExtractor

    try:
        extractor = PlaidExtractor()
        credentials = extractor.credentials
        logger.info(
            f"✅ Plaid credentials valid for {credentials.environment} environment"
        )

        # Test basic API connectivity
        import os

        # Check for configured tokens by looking at environment variables
        token_vars = [
            key for key in os.environ.keys() if key.startswith("PLAID_TOKEN_")
        ]

        if token_vars:
            logger.info(f"✅ Found {len(token_vars)} configured institution(s)")
            for token_var in token_vars:
                institution_name = (
                    token_var.replace("PLAID_TOKEN_", "").replace("_", " ").title()
                )
                logger.info(f"  - {institution_name}")
        else:
            logger.warning("⚠️  No access tokens configured")
            logger.info("To add institutions, set environment variables like:")
            logger.info("PLAID_TOKEN_WELLS_FARGO=access-sandbox-xxx")

    except Exception as e:
        logger.error(f"❌ Plaid credential validation failed: {e}")
        raise typer.Exit(1) from e
