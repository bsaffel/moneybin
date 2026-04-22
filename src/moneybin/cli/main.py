"""Main CLI application for MoneyBin.

This module provides the unified entry point for all MoneyBin CLI operations,
organizing commands into groups: profile, import, sync, categorize, transform,
db, mcp.
"""

import logging
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
    matches_app,
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
    """Global options for MoneyBin CLI."""
    if profile_name is None and ctx.invoked_subcommand not in ("profile",):
        try:
            profile_name = ensure_default_profile()
        except KeyboardInterrupt:
            raise typer.Abort() from None

    if profile_name is not None:
        try:
            set_current_profile(profile_name)
        except ValueError as e:
            raise typer.BadParameter(str(e)) from e

    setup_observability(stream="cli", verbose=verbose, profile=profile_name)
    if profile_name is not None:
        logger.info(f"Using profile: {profile_name}")

    # Auto-migrate old directory layout on first run after upgrade
    from moneybin.services.profile_service import ProfileService

    try:
        svc = ProfileService()
        migrated = svc.migrate_old_layout()
        if migrated:
            logger.info(f"Migrated {len(migrated)} profile(s) to new directory layout")
    except Exception:  # noqa: BLE001 — migration is best-effort, don't block CLI startup
        logger.debug("Migration check failed", exc_info=True)


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
app.add_typer(matches_app, name="matches", help="Review and manage transaction matches")
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
