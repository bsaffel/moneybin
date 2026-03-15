"""Credential management commands for MoneyBin CLI.

This module provides commands for managing API credentials, environment setup,
and credential validation across all integrated services.
"""

import logging

import typer

# Import the actual implementation classes
from moneybin.config import get_current_profile
from moneybin.utils.secrets_manager import SecretsManager

app = typer.Typer(
    help="Manage API credentials and environment configuration", no_args_is_help=True
)
logger = logging.getLogger(__name__)


@app.command("validate")
def validate() -> None:
    """Validate all configured credentials and API connections."""
    # Set up logging for this command

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
            "Check your .env file and run 'moneybin config credentials list-services' for details"
        )


@app.command("list-services")
def list_services() -> None:
    """List all supported services and their credential requirements."""
    # Set up logging for this command

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
    import os

    from moneybin.connectors.plaid_sync import PlaidSyncConnector

    try:
        # Initialize connector (validates credentials on init)
        _ = PlaidSyncConnector()
        logger.info("✅ Plaid credentials validated successfully")

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
