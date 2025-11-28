"""Main CLI application for MoneyBin.

This module provides the unified entry point for all MoneyBin CLI operations,
organizing commands into logical groups for data extraction, credential management,
and system utilities.
"""

import logging
from typing import Annotated

import typer

from ..config import set_current_profile
from ..logging import setup_logging
from .commands import credentials, extract, load, sync, transform

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="moneybin",
    help="MoneyBin: Personal financial data aggregation and analysis tool",
    add_completion=False,
    rich_markup_mode="rich",
    no_args_is_help=True,
)


@app.callback()
def main_callback(
    profile: Annotated[
        str,
        typer.Option(
            "--profile",
            "-p",
            help="User profile to use (e.g., alice, bob, yourself). Default: default",
            envvar="MONEYBIN_PROFILE",
        ),
    ] = "default",
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Enable verbose debug logging",
        ),
    ] = False,
) -> None:
    """Global options for MoneyBin CLI.

    The profile option determines which user's financial data to work with:
    - Each profile loads from .env.{profile} files (e.g., .env.alice, .env.bob)
    - Each profile has separate Plaid credentials and transaction data
    - Use profiles for: different family members, personal vs business, etc.
    - Profile names must be alphanumeric with optional dashes/underscores

    Examples:
      moneybin --profile=alice sync plaid        # Sync Alice's bank accounts
      moneybin --profile=bob load parquet        # Load Bob's transactions
      moneybin --profile=household transform run # Transform household data

    Can also be set via MONEYBIN_PROFILE environment variable.
    """
    import re

    # Initialize logging with centralized configuration
    setup_logging(cli_mode=True, verbose=verbose)

    # Validate profile name
    if not re.match(r"^[a-zA-Z0-9_-]+$", profile):
        logger.error(
            f"Invalid profile: {profile}. "
            "Must contain only alphanumeric characters, dashes, and underscores"
        )
        raise typer.BadParameter(
            f"Invalid profile name: {profile}. "
            "Use only alphanumeric characters, dashes, and underscores"
        )

    # Set the current profile globally
    set_current_profile(profile)

    # Log which profile is active
    logger.info(f"👤 Using profile: {profile}")


# Add command groups
app.add_typer(sync.app, name="sync", help="Sync data from external services")
app.add_typer(extract.app, name="extract", help="Extract data from local files")
app.add_typer(
    credentials.app, name="credentials", help="Credential management commands"
)
app.add_typer(load.app, name="load", help="Data loading commands")
app.add_typer(transform.app, name="transform", help="Data transformation commands")


def main() -> None:
    """Entry point for the MoneyBin CLI application."""
    app()


if __name__ == "__main__":
    main()
