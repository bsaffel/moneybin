"""Main CLI application for MoneyBin.

Unified entry point for MoneyBin CLI operations. Commands are organized into
top-level groups for entity management, workflows, reports, and infrastructure
per `docs/specs/cli-restructure.md` v2.

Cold-start cost is kept down by deferring heavy transitive imports
(``fastmcp``, ``sqlmesh``, ``polars``) inside the command function bodies
that need them — see `.claude/rules/cli.md` → "Cold-Start Hygiene".
"""

import logging
import os
from typing import Annotated

import typer

from ..config import register_profile_resolver, set_current_profile
from ..observability import setup_observability
from .commands import (
    accounts,
    assets,
    categories,
    db,
    import_cmd,
    logs,
    mcp,
    merchants,
    migrate,
    privacy,
    profile,
    reports,
    stats,
    sync,
    synthetic,
    system,
    tax,
    transactions,
    transform,
)
from .commands import (
    budget as budget_cmd,
)
from .commands.stubs import (
    export_app,
)
from .utils import resolve_profile, stash_cli_flags

logger = logging.getLogger(__name__)


app = typer.Typer(
    name="moneybin",
    help="MoneyBin: Personal financial data aggregation and analysis tool",
    add_completion=True,
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

    # Set the active profile name eagerly when one is explicit. This only
    # validates the name format and updates module state — no dir check,
    # no I/O — so it's safe for `--help` and bare-group invocations.
    if explicit := profile_name or os.environ.get("MONEYBIN_PROFILE"):
        try:
            set_current_profile(explicit)
        except ValueError as e:
            raise typer.BadParameter(str(e)) from e

    # Profile commands are recovery tools (`profile create` legitimately runs
    # against a profile that doesn't yet exist) and synthetic commands manage
    # their own profile lifecycle — both skip the lazy dir-check + wizard.
    if ctx.invoked_subcommand in ("profile", "synthetic"):
        return

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
    accounts.app,
    name="accounts",
    help="Account listing, settings, and lifecycle ops",
)
app.add_typer(
    reports.app,
    name="reports",
    help="Cross-domain analytical reports",
)
app.add_typer(transactions.app, name="transactions")
app.add_typer(assets.app, name="assets")
app.add_typer(categories.app, name="categories")
app.add_typer(merchants.app, name="merchants")
app.add_typer(
    privacy.app, name="privacy", help="Privacy utilities: redaction and audit"
)
app.add_typer(budget_cmd.app, name="budget")
app.add_typer(tax.app, name="tax")
app.add_typer(system.app, name="system")
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
