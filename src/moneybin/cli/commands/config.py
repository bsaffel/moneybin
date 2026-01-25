"""Configuration management commands for MoneyBin.

This module provides CLI commands for managing user-level configuration
including default profile settings.
"""

import logging
from typing import Annotated

import typer

from moneybin.utils.user_config import (
    get_default_profile,
    get_user_config_path,
    reset_user_config,
    set_default_profile,
)

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="config",
    help="User configuration management",
    no_args_is_help=True,
)


@app.command("show")
def show_config() -> None:
    """Show current user configuration.

    Displays the default profile and configuration file location.

    Example:
        moneybin config show
    """
    config_path = get_user_config_path()
    default_profile = get_default_profile()

    print("\n📋 MoneyBin User Configuration")
    print(f"   Config file: {config_path}")
    print(f"   Exists: {config_path.exists()}")

    if default_profile:
        print(f"\n✅ Default profile: {default_profile}")
        print(f"   Data location: data/{default_profile}/")
    else:
        print("\n⚠️  No default profile set")
        print("   You will be prompted to create one on next run")

    print()


@app.command("get-default-profile")
def get_default_profile_command() -> None:
    """Get the default profile name.

    Example:
        moneybin config get-default-profile
    """
    default_profile = get_default_profile()

    if default_profile:
        print(default_profile)
    else:
        logger.warning("No default profile configured")
        raise typer.Exit(1)


@app.command("set-default-profile")
def set_default_profile_command(
    profile_name: Annotated[
        str,
        typer.Argument(
            help="Profile name to set as default (e.g., alice, bob, john-work)"
        ),
    ],
) -> None:
    """Set the default profile name.

    The profile name will be normalized to lowercase with hyphens.
    This profile will be used when --profile is not specified.

    Examples:
        moneybin config set-default-profile alice
        moneybin config set-default-profile "John Smith"
        moneybin config set-default-profile bob_work
    """
    try:
        set_default_profile(profile_name)
        normalized = get_default_profile()
        print(f"✅ Default profile set to: {normalized}")
        print(f"   Data location: data/{normalized}/")
    except ValueError as e:
        logger.error(f"Failed to set default profile: {e}")
        raise typer.Exit(1) from e


@app.command("reset")
def reset_config() -> None:
    """Reset user configuration.

    This deletes the user configuration file and you will be prompted
    to create a new default profile on next run.

    Example:
        moneybin config reset
    """
    confirm = typer.confirm(
        "Are you sure you want to reset your configuration?\n"
        "You will need to set up your default profile again.",
        default=False,
    )

    if not confirm:
        print("❌ Cancelled")
        raise typer.Exit(0)

    try:
        reset_user_config()
        print("✅ Configuration reset successfully")
    except Exception as e:
        logger.error(f"Failed to reset configuration: {e}")
        raise typer.Exit(1) from e


@app.command("path")
def show_config_path() -> None:
    """Show the path to the user configuration file.

    Example:
        moneybin config path
    """
    config_path = get_user_config_path()
    print(config_path)
