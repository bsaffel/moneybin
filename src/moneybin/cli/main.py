"""Main CLI application for MoneyBin.

This module provides the unified entry point for all MoneyBin CLI operations,
organizing commands into groups: profile, import, sync, categorize, transform,
db, mcp.
"""

import logging
import sys
from typing import Annotated

import typer

from ..config import set_current_profile
from ..observability import setup_observability
from ..utils.user_config import ensure_default_profile
from .commands import (
    categorize,
    db,
    import_cmd,
    logs,
    matches,
    mcp,
    migrate,
    profile,
    stats,
    sync,
    synthetic,
    transform,
)
from .commands.stubs import (
    export_app,
    track_app,
)

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
    ctx: typer.Context,
    profile_name: Annotated[
        str | None,
        typer.Option(
            "--profile",
            "-p",
            help="User profile to use. Uses saved default if not specified.",
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

    Profile resolution chain (uniform across all commands):
        1. ``--profile`` flag
        2. ``MONEYBIN_PROFILE`` env var
        3. ``active_profile`` in ``<base>/config.yaml``
        4. First-run wizard (``ensure_default_profile``) — only when the
           command needs an existing profile to operate.

    Profile commands (``profile *``) skip the existence check, since
    ``profile create`` legitimately operates on a name that doesn't yet
    exist. They still benefit from the same resolution chain so
    ``profile show`` / ``profile set`` honor ``--profile`` / env var.
    """
    import os

    from ..utils.user_config import get_default_profile

    # Commands that manage profiles don't require the resolved profile
    # to point at an existing directory (e.g. `profile create alice`).
    is_profile_cmd = ctx.invoked_subcommand == "profile"

    profile_source: str | None = None
    if profile_name is not None:
        # Typer reads MONEYBIN_PROFILE into profile_name automatically;
        # distinguish env vs flag by checking the env var directly.
        if (
            os.environ.get("MONEYBIN_PROFILE") == profile_name
            and "--profile" not in sys.argv
            and "-p" not in sys.argv
        ):
            profile_source = "MONEYBIN_PROFILE env var"
        else:
            profile_source = "--profile flag"

    if profile_name is None:
        if is_profile_cmd:
            # Profile commands tolerate an unset profile (e.g. `profile create`).
            # Just consult config.yaml — no first-run wizard.
            config_profile = get_default_profile()
            if config_profile is not None:
                profile_name = config_profile
                profile_source = "config.yaml"
        else:
            # Non-profile commands need a profile. ensure_default_profile()
            # consults config.yaml first and prompts only on true first run.
            try:
                profile_name = ensure_default_profile()
                profile_source = "config.yaml or first-run wizard"
            except KeyboardInterrupt:
                raise typer.Abort() from None

    if profile_name is not None:
        try:
            set_current_profile(profile_name)
        except ValueError as e:
            raise typer.BadParameter(str(e)) from e

        if not is_profile_cmd:
            from ..config import get_base_dir
            from ..utils.user_config import normalize_profile_name

            normalized = normalize_profile_name(profile_name)
            profile_dir = get_base_dir() / "profiles" / normalized
            if not profile_dir.exists():
                logger.error(f"❌ Profile '{normalized}' does not exist")
                logger.info("💡 Run 'moneybin profile list' to see available profiles")
                logger.info(
                    f"💡 Run 'moneybin profile create {normalized}' to create it"
                )
                raise typer.Exit(1)

    setup_observability(
        stream="cli",
        verbose=verbose,
        profile=profile_name,
    )
    if profile_name is not None and not is_profile_cmd:
        if profile_source:
            logger.info(f"Using profile: {profile_name} (from {profile_source})")
        else:
            logger.info(f"Using profile: {profile_name}")


# Command groups ordered by workflow: setup → ingest → enrich → pipeline → analyze → output → integrations → ops
app.add_typer(
    profile.app,
    name="profile",
    help="Manage user profiles (create, list, switch, delete, show, set)",
)
app.add_typer(
    import_cmd.app,
    name="import",
    help="Import financial files into MoneyBin",
)
app.add_typer(
    sync.app,
    name="sync",
    help="Sync transactions from external services",
)
app.add_typer(
    categorize.app,
    name="categorize",
    help="Manage transaction categories, rules, and merchants",
)
app.add_typer(matches.app, name="matches", help="Review and manage transaction matches")
app.add_typer(
    transform.app,
    name="transform",
    help="Run SQLMesh data transformations",
)
app.add_typer(
    synthetic.app,
    name="synthetic",
    help="Generate and manage synthetic financial data for testing",
)
app.add_typer(track_app, name="track", help="Balance tracking and net worth")
app.add_typer(stats.app, name="stats", help="Show lifetime metric aggregates")
app.add_typer(export_app, name="export", help="Export data to external formats")
app.add_typer(
    mcp.app,
    name="mcp",
    help="MCP server for AI assistant integration",
)
app.add_typer(
    db.app,
    name="db",
    help="Database management and exploration",
)
app.add_typer(
    logs.app,
    name="logs",
    help="Manage log files",
)

# Add db migrate as a sub-typer of db
db.app.add_typer(migrate.app, name="migrate", help="Database migration management")


def main() -> None:
    """Entry point for the MoneyBin CLI application."""
    app()


if __name__ == "__main__":
    main()
