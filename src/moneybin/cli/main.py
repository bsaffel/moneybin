"""Main CLI application for MoneyBin.

This module provides the unified entry point for all MoneyBin CLI operations,
organizing commands into groups: profile, import, sync, categorize, transform,
db, mcp.
"""

import logging
from typing import Annotated

import click
import typer
from typer.core import TyperGroup

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


class _HelpAwareGroup(TyperGroup):
    """TyperGroup that flags ``ctx.meta['help_requested']`` when ``--help`` will fire.

    Click handles ``--help`` lazily: a root-level ``moneybin --help`` is intercepted
    before this group's callback runs, but ``moneybin <subcommand> --help`` and
    deeper invocations run the parent callback first. The parent callback does
    profile resolution (env lookup, first-run wizard, observability setup) which
    must stay inert when help is the user's intent.

    Rather than peeking at ``sys.argv`` (which doesn't reflect ``CliRunner.invoke``
    arguments and false-positives on values like ``--description "--help me"``),
    we walk the unparsed token chain via Click's parser and detect whether any
    ``--help``/``-h`` token would actually parse as a help option for the
    upcoming subcommand. The result is stored on ``ctx.meta`` (inherited by
    child contexts) for the parent callback to consult.
    """

    def invoke(self, ctx: click.Context) -> object:
        if _help_token_in_chain(ctx):
            ctx.meta["help_requested"] = True
        return super().invoke(ctx)


def _help_token_in_chain(group_ctx: click.Context) -> bool:
    """Return True if ``--help`` will fire as a help option for some subcommand.

    Walks down the resolved subcommand chain using Click's parser in
    ``resilient_parsing`` mode (so the help callback doesn't fire prematurely)
    and checks whether any help-option name was consumed as an option for any
    command in the chain. This avoids false positives where ``--help`` appears
    as the value of another option (e.g., ``--description --help``).
    """
    # Click stores upcoming-but-not-yet-consumed tokens on the private
    # ``_protected_args`` (subcommand name) and ``args`` (rest). The public
    # ``protected_args`` property is deprecated in Click 9.0, so we read the
    # underlying private attribute directly.
    protected: list[str] = getattr(group_ctx, "_protected_args", [])
    remaining: list[str] = [*protected, *group_ctx.args]
    cmd: click.Command = group_ctx.command
    cur_ctx: click.Context = group_ctx
    while remaining and isinstance(cmd, click.Group):
        sub_name = remaining[0]
        sub_cmd = cmd.get_command(cur_ctx, sub_name)
        if sub_cmd is None:
            return False
        sub_args = remaining[1:]
        try:
            tmp_ctx = sub_cmd.make_context(
                sub_name,
                list(sub_args),
                parent=cur_ctx,
                resilient_parsing=True,
            )
            parser = sub_cmd.make_parser(tmp_ctx)
            _, params_left, _ = parser.parse_args(args=list(sub_args))
        except Exception:  # noqa: BLE001  # any parse error -> assume not help
            return False
        help_names = set(tmp_ctx.help_option_names)
        for name in help_names:
            # Token present in input but absent from leftover args means the
            # parser consumed it as the help option (not as a value).
            if name in sub_args and name not in params_left:
                return True
        cur_ctx = tmp_ctx
        cmd = sub_cmd
        remaining = params_left
    return False


app = typer.Typer(
    name="moneybin",
    help="MoneyBin: Personal financial data aggregation and analysis tool",
    add_completion=False,
    rich_markup_mode="rich",
    no_args_is_help=True,
    cls=_HelpAwareGroup,
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
    # --help is documentation; it must be inert. Skip profile resolution
    # entirely when help is requested so we don't trigger the first-run
    # wizard, write profile dirs, or fail when MONEYBIN_PROFILE points at
    # a non-existent profile. The flag is set by ``_HelpAwareGroup.invoke``
    # via parse-ahead detection — see that class for the rationale.
    if ctx.meta.get("help_requested", False):
        return

    import os

    # Commands that manage profiles don't require the resolved profile
    # to point at an existing directory (e.g. `profile create alice`).
    is_profile_cmd = ctx.invoked_subcommand == "profile"

    # Synthetic commands manage their own profiles: `generate`/`reset` write
    # to persona profiles (alice/bob/charlie) and call set_current_profile()
    # themselves; `verify` provisions an ephemeral scenario profile in a
    # tempdir. None of them should ever touch the user's default profile or
    # trigger the first-run wizard.
    is_synthetic_cmd = ctx.invoked_subcommand == "synthetic"

    # Resolve env var manually (instead of via Typer's envvar=) so we can
    # cleanly distinguish flag-provided vs env-provided values without
    # inspecting raw argv.
    profile_source: str | None = None
    if profile_name is not None:
        profile_source = "--profile flag"
    elif env_profile := os.environ.get("MONEYBIN_PROFILE"):
        profile_name = env_profile
        profile_source = "MONEYBIN_PROFILE env var"

    if profile_name is None and not is_profile_cmd and not is_synthetic_cmd:
        # Non-profile commands need a profile. ensure_default_profile()
        # consults config.yaml first and prompts only on true first run.
        # Profile commands (list/show/set/create/delete) intentionally skip
        # this fallback so they remain runnable even if the active profile's
        # settings are invalid — users need profile commands to recover.
        try:
            profile_name = ensure_default_profile()
            profile_source = "config.yaml or first-run wizard"
        except KeyboardInterrupt:
            raise typer.Abort() from None

    if profile_name is not None and not is_synthetic_cmd:
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

    # Profile commands are recovery tools — they must run even when the
    # active profile's settings are broken. Skip per-profile settings load
    # (which would call get_settings() and could fail) by passing profile=None.
    setup_observability(
        stream="cli",
        verbose=verbose,
        profile=None if is_profile_cmd or is_synthetic_cmd else profile_name,
    )
    if profile_name is not None and not is_profile_cmd and not is_synthetic_cmd:
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
