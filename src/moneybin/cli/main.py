"""Main CLI application for MoneyBin.

Unified entry point for MoneyBin CLI operations. Commands are organized into
top-level groups for entity management, workflows, reports, and infrastructure
per `docs/specs/moneybin-cli.md` v2.

Command modules import lazily (``_MoneyBinRoot.lazy_commands``), so ``--help``
and the start menu load none of their service graphs. Heavy third-party
imports (``fastmcp``, ``sqlmesh``, ``polars``) are also deferred inside command
bodies — see `.claude/rules/cli.md` → "Cold-Start Hygiene".
"""

import importlib.metadata
import logging
import os
from pathlib import Path
from typing import Annotated

import typer

from ..config import (
    mark_profile_resolution_pending,
    register_profile_resolver,
    set_current_profile,
)
from ..observability import setup_observability
from .navigation import LazyCommand, RootGroup, show_start_menu
from .utils import resolve_profile, stash_cli_flags

logger = logging.getLogger(__name__)

# DEPRECATED: direct-human-output — migrate this text path through the shared
# terminal policy; docs/specs/cli-human-experience.md#implementation-boundary-and-migration.


_COMMANDS = "moneybin.cli.commands"


class _MoneyBinRoot(RootGroup):
    """The root group; each command module imports when first resolved."""

    lazy_commands = {
        "accounts": LazyCommand(f"{_COMMANDS}.accounts", "app"),
        "assets": LazyCommand(f"{_COMMANDS}.assets", "app"),
        "investments": LazyCommand(f"{_COMMANDS}.investments", "app"),
        "reports": LazyCommand(f"{_COMMANDS}.reports", "app"),
        "transactions": LazyCommand(f"{_COMMANDS}.transactions", "app"),
        "categories": LazyCommand(f"{_COMMANDS}.categories", "app"),
        "fx": LazyCommand(f"{_COMMANDS}.fx", "app"),
        "merchants": LazyCommand(f"{_COMMANDS}.merchants", "app"),
        "review": LazyCommand(f"{_COMMANDS}.review", "review_command"),
        "export": LazyCommand(f"{_COMMANDS}.export", "export_app"),
        "gsheet": LazyCommand(f"{_COMMANDS}.gsheet", "app"),
        "import": LazyCommand(f"{_COMMANDS}.import_cmd", "app"),
        "refresh": LazyCommand(f"{_COMMANDS}.refresh", "refresh_command"),
        "sync": LazyCommand(f"{_COMMANDS}.sync", "app"),
        "demo": LazyCommand(f"{_COMMANDS}.demo", "demo_command"),
        "mcp": LazyCommand(f"{_COMMANDS}.mcp", "app"),
        "privacy": LazyCommand(f"{_COMMANDS}.privacy", "app"),
        "profile": LazyCommand(f"{_COMMANDS}.profile", "app"),
        "db": LazyCommand(f"{_COMMANDS}.db", "app"),
        "logs": LazyCommand(f"{_COMMANDS}.logs", "logs_command"),
        "sql": LazyCommand(f"{_COMMANDS}.sql", "app"),
        "stats": LazyCommand(f"{_COMMANDS}.stats", "stats_command"),
        "synthetic": LazyCommand(f"{_COMMANDS}.synthetic", "app"),
        "system": LazyCommand(f"{_COMMANDS}.system", "app"),
        "transform": LazyCommand(f"{_COMMANDS}.transform", "app"),
        "budget": LazyCommand(f"{_COMMANDS}.budget", "app", hidden=True),
    }


app = typer.Typer(
    name="moneybin",
    help="MoneyBin: Personal financial data aggregation and analysis tool",
    add_completion=True,
    rich_markup_mode="rich",
    cls=_MoneyBinRoot,
    no_args_is_help=False,
    invoke_without_command=True,
    subcommand_metavar="[COMMAND] [ARGS]...",
    # Pinned, not merely inherited: frames on the database-open path hold the
    # plaintext encryption key and profile passphrases as locals, and a rich
    # traceback is not a log record, so SanitizedLogFormatter cannot redact it.
    # Typer defaults this to False today, but the dependency floor is a range
    # — state it here so the guarantee does not rest on a dependency default.
    # Sub-apps render through this root app's handler, so setting it once here
    # covers every command group.
    pretty_exceptions_show_locals=False,
)


def get_version() -> str:
    """Installed moneybin distribution version."""
    return importlib.metadata.version("moneybin")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(get_version())
        raise typer.Exit()


def _home_callback(value: Path | None) -> Path | None:
    """Export ``--home`` as MONEYBIN_HOME before anything resolves the data home.

    ``get_base_dir()`` reads ``os.environ`` directly — it runs during
    ``MoneyBinSettings.__init__``, before ``get_settings()`` exists — so the
    flag has to become a real environment variable rather than a stashed
    parameter. Writing it through also means ``mcp install`` pins this home
    into the generated client config, since that command distinguishes an
    explicit home from a derived one by reading the same variable.

    Eager so it lands before ``--profile`` is resolved: profiles live under
    ``<home>/profiles/``, so the home has to be settled first.
    """
    if value is not None:
        os.environ["MONEYBIN_HOME"] = str(value)
    return value


@app.callback()
def main_callback(
    ctx: typer.Context,
    home: Annotated[
        Path | None,
        typer.Option(
            "--home",
            callback=_home_callback,
            is_eager=True,
            envvar="MONEYBIN_HOME",
            show_envvar=True,
            help="Data directory holding profiles, config and databases. "
            "Overrides MONEYBIN_HOME. Defaults to <repo>/.moneybin inside a "
            "checkout, otherwise ~/.moneybin.",
        ),
    ] = None,
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
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show the installed MoneyBin version and exit.",
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
    if ctx.invoked_subcommand is None:
        if profile_name is not None:
            from moneybin.utils.user_config import normalize_profile_name

            try:
                normalize_profile_name(profile_name)
            except ValueError as e:
                raise typer.BadParameter(str(e), param_hint="--profile") from e
        show_start_menu()
        return

    stash_cli_flags(profile_name, verbose)
    setup_observability(stream="cli", verbose=verbose, profile=None)

    # `demo` always targets the dedicated `demo` profile, so a --profile alongside it
    # would be silently discarded. Reject it rather than accept-and-ignore: quietly
    # doing something other than what the flag says is exactly the invisible magic
    # `design-principles.md` rules out. (An ambient MONEYBIN_PROFILE is not an
    # instruction about *this* command, so it stays tolerated and overridden.)
    if ctx.invoked_subcommand == "demo" and profile_name:
        raise typer.BadParameter(
            "'demo' always targets the dedicated 'demo' profile and cannot be pointed "
            "at another one. Drop --profile, or use 'moneybin synthetic generate' for "
            "a differently-named synthetic sandbox.",
            param_hint="--profile",
        )

    # Set the active profile name eagerly when one is explicit. This only
    # validates the name format and updates module state — no dir check,
    # no I/O — so it's safe for `--help` and bare-group invocations. Readers
    # that skip the lazy resolver on purpose (`get_current_profile(
    # auto_resolve=False)`, which opts out of the first-run wizard rather than
    # out of a named profile — `mcp config path` is one) depend on it.
    if explicit := profile_name or os.environ.get("MONEYBIN_PROFILE"):
        try:
            set_current_profile(explicit)
        except ValueError as e:
            raise typer.BadParameter(str(e)) from e

    # Profile commands are recovery tools (`profile create` legitimately runs
    # against a profile that doesn't yet exist), and synthetic + demo commands
    # manage their own profile lifecycle — all skip the lazy dir-check + wizard.
    if ctx.invoked_subcommand in ("profile", "synthetic", "demo"):
        return

    register_profile_resolver(resolve_profile)

    # The name above is only a hint: the profile directory has not been
    # checked and observability still points at no profile's log files. Say so
    # explicitly, or the eager set silently satisfies the resolver's gate and
    # the command runs unlogged with SQLMesh's per-statement INFO on stderr.
    if explicit:
        mark_profile_resolution_pending()


def main() -> None:
    """Entry point for the MoneyBin CLI application."""
    app()


if __name__ == "__main__":
    main()
