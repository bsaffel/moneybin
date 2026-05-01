"""Main CLI application for MoneyBin.

This module provides the unified entry point for all MoneyBin CLI operations,
organizing commands into groups: profile, import, sync, categorize, transform,
db, mcp.
"""

import logging
import os
from typing import Annotated

import click
import typer
from typer.core import TyperGroup

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


_RAW_ARGS_META_KEY = "moneybin._raw_args"


class _MoneybinRootGroup(TyperGroup):
    """Root group that captures the unparsed argv before Click clears it.

    Click's ``Group.invoke`` resets ``ctx._protected_args`` and ``ctx.args``
    to ``[]`` *before* the root callback runs, so the callback cannot see
    the unparsed subcommand chain through the context. We need that chain
    to detect implicit-help (a bare group with ``no_args_is_help=True``)
    without falling back to ``sys.argv`` — ``sys.argv`` is wrong under
    ``CliRunner`` and pytest-xdist (where it contains the worker's args).
    Stashing the raw argv on ``ctx.meta`` during ``parse_args`` is the
    single point that sees the original list for both real CLI invocations
    and programmatic ``app(args=[...])`` calls.
    """

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        ctx.meta[_RAW_ARGS_META_KEY] = list(args)
        return super().parse_args(ctx, args)


app = typer.Typer(
    name="moneybin",
    help="MoneyBin: Personal financial data aggregation and analysis tool",
    add_completion=False,
    rich_markup_mode="rich",
    no_args_is_help=True,
    cls=_MoneybinRootGroup,
)


_HELP_TOKENS = frozenset({"--help", "-h"})

# Root-callback options that consume the next argv token. Used by
# ``_will_show_help`` to skip flag-value pairs when walking the
# stashed argv.
_ROOT_VALUE_FLAGS = frozenset({"-p", "--profile"})


def _will_show_help(ctx: typer.Context, root: click.Command, argv: list[str]) -> bool:
    """Walk argv to decide whether the invocation will render help.

    Returns True for an explicit ``--help``/``-h`` flag anywhere in the
    chain *or* implicit-help cases where the chain ends on a group with
    ``no_args_is_help=True`` (e.g. bare ``moneybin db``). The argv list is
    captured by ``_MoneybinRootGroup.parse_args`` and stashed on
    ``ctx.meta`` because Click clears ``ctx._protected_args``/``ctx.args``
    before the root callback runs.
    """
    cmd = root
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok in _HELP_TOKENS:
            return True
        if tok.startswith("-"):
            # Skip the value of root-level options that take one
            # (e.g. `-p alice`). `--profile=alice` is a single token
            # and needs no extra skip. Unknown leaf-level flags (e.g.
            # `--print-path`) also fall here; treating them as
            # value-less is safe because we only care about reaching
            # the end of the subcommand chain.
            if tok in _ROOT_VALUE_FLAGS and i + 1 < len(argv):
                i += 2
            else:
                i += 1
            continue
        if isinstance(cmd, click.Group):
            sub = cmd.get_command(ctx, tok)
            if sub is None:
                return False
            cmd = sub
            i += 1
            continue
        # Reached a leaf command — remaining tokens are its arguments;
        # the leaf will run, not help.
        return False
    return isinstance(cmd, click.Group) and bool(getattr(cmd, "no_args_is_help", False))


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
    #
    # Eager resolution is bypassed when the invocation will land on a help
    # screen — explicit (`--help`/`-h` anywhere in the chain) or implicit
    # (a group with `no_args_is_help=True` and no further tokens, e.g.
    # bare `moneybin db`). Help text must remain side-effect free even when
    # `MONEYBIN_PROFILE` points to a deleted profile.
    #
    # The unparsed argv is captured on ``ctx.meta`` by
    # ``_MoneybinRootGroup.parse_args`` (Click clears its own copy before
    # the callback runs). Reading from ``ctx.meta`` keeps detection
    # correct for both real CLI invocations and ``CliRunner``-based tests.
    raw_args: list[str] = ctx.meta.get(_RAW_ARGS_META_KEY, [])
    help_requested = _will_show_help(ctx, ctx.command, raw_args)

    if explicit and not help_requested:
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
