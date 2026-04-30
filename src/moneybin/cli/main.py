"""Main CLI application for MoneyBin.

This module provides the unified entry point for all MoneyBin CLI operations,
organizing commands into groups: profile, import, sync, categorize, transform,
db, mcp.
"""

import logging
import os
from typing import Annotated

import typer

from ..config import register_profile_resolver, set_current_profile
from ..observability import setup_observability
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
from .utils import resolve_profile, stash_cli_flags

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
            help="User profile to use. Uses MONEYBIN_PROFILE env var or "
            "saved default if not specified.",
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

    The callback is intentionally inert: it stashes ``--profile`` /
    ``--verbose`` and registers a profile resolver, but does not touch the
    active profile, run the first-run wizard, or open any files. Profile
    resolution fires the first time a command actually calls ``get_settings``
    / ``get_current_profile``. Keeping the callback inert means
    ``moneybin <cmd> --help`` and docker-style usage errors
    (``moneybin logs`` with no stream) never trigger the wizard or write
    profile dirs before the leaf command surfaces its own response.
    """
    stash_cli_flags(profile_name, verbose)
    setup_observability(stream="cli", verbose=verbose, profile=None)

    # Profile commands are recovery tools (`profile create` legitimately runs
    # against a profile that doesn't yet exist) and synthetic commands manage
    # their own profile lifecycle — both must skip the dir-check + wizard.
    is_profile_cmd = ctx.invoked_subcommand == "profile"
    is_synthetic_cmd = ctx.invoked_subcommand == "synthetic"

    explicit = profile_name or os.environ.get("MONEYBIN_PROFILE")
    if is_profile_cmd or is_synthetic_cmd:
        # Honor explicit selection by name only — no dir check, no lazy
        # resolver (these commands explicitly pass auto_resolve=False on
        # ``get_current_profile`` and call ``set_current_profile`` themselves).
        if explicit:
            try:
                set_current_profile(explicit)
            except ValueError as e:
                raise typer.BadParameter(str(e)) from e
        return

    # Explicit selection (--profile flag or MONEYBIN_PROFILE) eagerly finishes
    # setup so profile-specific log files, dir-check errors, and the "Using
    # profile" banner appear consistently — even for fast-exit commands that
    # never read settings. The wizard path (no flag, no env) stays lazy: it
    # fires only when a command actually calls ``get_settings`` /
    # ``get_current_profile``, so docker-style usage errors (``moneybin logs``
    # with no stream) and ``--help`` never trigger it.
    if explicit:
        resolve_profile()
    else:
        register_profile_resolver(resolve_profile)


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
app.command(name="stats", help="Show lifetime metric aggregates")(stats.stats_command)
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
app.command(
    name="logs",
    help="View, prune, or locate MoneyBin log files for the active profile.",
)(logs.logs_command)

# Add db migrate as a sub-typer of db
db.app.add_typer(migrate.app, name="migrate", help="Database migration management")


def main() -> None:
    """Entry point for the MoneyBin CLI application."""
    app()


if __name__ == "__main__":
    main()
