"""Profile management commands for MoneyBin CLI."""

import logging
import sys
from typing import Annotated

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json
from moneybin.services.profile_service import (
    ProfileExistsError,
    ProfileNotFoundError,
    ProfileService,
)

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage user profiles (create, list, switch, delete, show, set)",
    no_args_is_help=True,
)


@app.command("create")
def profile_create(
    name: Annotated[str, typer.Argument(help="Profile name (will be normalized)")],
    init_inbox: Annotated[
        bool | None,
        typer.Option(
            "--init-inbox/--no-init-inbox",
            help=(
                "Create the import-inbox layout (~/Documents/MoneyBin/<profile>/"
                "{inbox,processed,failed}/). If unset, prompts when interactive "
                "and skips when not."
            ),
        ),
    ] = None,
) -> None:
    """Create a new profile with directory structure, config, and encrypted database."""
    if init_inbox is None:
        init_inbox = (
            typer.confirm(
                f"Set up the import inbox at ~/Documents/MoneyBin/{name}/?",
                default=True,
            )
            if sys.stdin.isatty()
            else False
        )
    svc = ProfileService()
    try:
        profile_dir = svc.create(name, init_inbox=init_inbox)
        logger.info(f"✅ Created profile {name} at {profile_dir}")
        if init_inbox:
            logger.info(f"Import inbox ready at ~/Documents/MoneyBin/{name}/inbox/")
    except ProfileExistsError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        logger.error(f"❌ Failed to create profile '{name}': {e}")
        logger.info(f"💡 Run 'moneybin profile create {name}' to retry")
        raise typer.Exit(1) from e


@app.command("list")
def profile_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List all profiles, marking the active one."""
    svc = ProfileService()
    profiles = svc.list()

    if output == "json":
        emit_json("profiles", profiles)
        return

    if not profiles:
        if not quiet:
            logger.info("No profiles found")
            logger.info("💡 Run 'moneybin profile create <name>' to create one")
        return
    for p in profiles:
        marker = " (active)" if p["active"] else ""
        logger.info(f"  {p['name']}{marker}")


@app.command("switch")
def profile_switch(
    name: Annotated[str, typer.Argument(help="Profile name to switch to")],
) -> None:
    """Set a different profile as the active default."""
    svc = ProfileService()
    try:
        svc.switch(name)
        logger.info(f"✅ Switched to profile: {name}")
    except ProfileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("delete")
def profile_delete(
    name: Annotated[str, typer.Argument(help="Profile name to delete")],
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip confirmation prompt"),
    ] = False,
) -> None:
    """Delete a profile and all its data (database, logs, config)."""
    svc = ProfileService()
    if not yes:
        confirm = typer.confirm(
            f"Delete profile '{name}' and ALL its data? This cannot be undone."
        )
        if not confirm:
            return
    try:
        svc.delete(name)
        logger.info(f"✅ Deleted profile: {name}")
    except ProfileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
    except ValueError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("show")
def profile_show(
    name: Annotated[
        str | None,
        typer.Argument(help="Profile name (defaults to active profile)"),
    ] = None,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — show has no info chatter; only data lines
) -> None:
    """Show resolved settings for a profile."""
    svc = ProfileService()
    if name is None:
        from moneybin.config import get_current_profile

        try:
            name = get_current_profile(auto_resolve=False)
        except RuntimeError:
            name = None
    try:
        info = svc.show(name)
        if output == "json":
            emit_json("profile", info)
            return
        marker = " (active)" if info["active"] else ""
        logger.info(f"Profile: {info['name']}{marker}")
        logger.info(f"  Path:     {info['path']}")
        logger.info(f"  Database: {info['database_path']}")
        db_status = "exists" if info["database_exists"] else "not created"
        logger.info(f"  DB state: {db_status}")
        if info.get("config"):
            logger.info("  Config:")
            for section, values in info["config"].items():  # type: ignore[union-attr]  # narrowed by .get check
                if isinstance(values, dict):
                    for k, v in values.items():
                        logger.info(f"    {section}.{k}: {v}")
    except ProfileNotFoundError as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e


@app.command("set")
def profile_set(
    key: Annotated[str, typer.Argument(help="Config key (e.g., logging.level)")],
    value: Annotated[str, typer.Argument(help="Value to set")],
    name: Annotated[
        str | None,
        typer.Option("--profile", "-p", help="Profile name (defaults to active)"),
    ] = None,
) -> None:
    """Set a configuration value on a profile."""
    svc = ProfileService()
    target: str
    if name:
        target = name
    else:
        from moneybin.config import get_current_profile

        try:
            target = get_current_profile(auto_resolve=False)
        except RuntimeError:
            profiles = svc.list()
            active = next((p["name"] for p in profiles if p["active"]), None)
            target = str(active) if active else "default"
    try:
        svc.set(target, key, value)
        logger.info(f"✅ Set {key}={value}")
    except (ProfileNotFoundError, ValueError) as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e
