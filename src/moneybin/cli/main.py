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
from ..utils.user_config import ensure_default_profile
from .commands import config, credentials, db, extract, load, mcp, sync, transform

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
        str | None,
        typer.Option(
            "--profile",
            "-p",
            help="User profile to use (e.g., alice, bob, yourself). Uses saved default if not specified.",
            envvar="MONEYBIN_PROFILE",
        ),
    ] = None,
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
    - Each profile has isolated data storage in data/{profile}/
    - Each profile loads from .env.{profile} files (e.g., .env.alice, .env.bob)
    - Use profiles for: different family members, personal vs business, etc.
    - Profile names are normalized to lowercase with hyphens

    Examples:
      moneybin sync plaid                        # Use default profile
      moneybin --profile=alice sync plaid        # Sync Alice's bank accounts
      moneybin --profile=bob load parquet        # Load Bob's transactions
      moneybin --profile=household transform run # Transform household data

    Can also be set via MONEYBIN_PROFILE environment variable.
    Priority: --profile flag > MONEYBIN_PROFILE > saved default > prompt
    """
    # Resolve profile name BEFORE setting up logging so logs go to correct directory
    # Priority:
    # 1. --profile flag (already checked by typer)
    # 2. MONEYBIN_PROFILE env var (already checked by typer)
    # 3. Saved default from ~/.moneybin/config.yaml
    # 4. Prompt user for first name
    if profile is None:
        try:
            profile = ensure_default_profile()
        except KeyboardInterrupt:
            # User cancelled setup
            raise typer.Abort() from None

    # Set the current profile globally (will normalize the name)
    try:
        set_current_profile(profile)
    except ValueError as e:
        raise typer.BadParameter(str(e)) from e

    # Initialize logging AFTER profile is set so logs go to profile-specific directory
    setup_logging(cli_mode=True, verbose=verbose, profile=profile)

    # Log which profile is active
    logger.info(f"👤 Using profile: {profile}")


# Add command groups
app.add_typer(config.app, name="config", help="User configuration management")
app.add_typer(sync.app, name="sync", help="Sync data from external services")
app.add_typer(extract.app, name="extract", help="Extract data from local files")
app.add_typer(
    credentials.app, name="credentials", help="Credential management commands"
)
app.add_typer(load.app, name="load", help="Data loading commands")
app.add_typer(transform.app, name="transform", help="Data transformation commands")
app.add_typer(db.app, name="db", help="Database exploration and query commands")
app.add_typer(mcp.app, name="mcp", help="MCP server for AI assistant integration")


def main() -> None:
    """Entry point for the MoneyBin CLI application."""
    app()


if __name__ == "__main__":
    main()
