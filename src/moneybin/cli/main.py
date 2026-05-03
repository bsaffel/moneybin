"""Main CLI application for MoneyBin.

This module provides the unified entry point for all MoneyBin CLI operations,
organizing commands into groups: profile, import, sync, categorize, transform,
db, mcp.

Command modules are lazy-loaded: each group's module (and its transitive
imports) loads only when that group is first invoked, not at ``moneybin
--help`` time. This keeps the cold-start cost proportional to what the user
actually runs.
"""

import logging
import os
from importlib import import_module
from typing import Annotated, Any

import typer

from ..config import register_profile_resolver, set_current_profile
from ..observability import setup_observability
from .commands import logs, stats
from .commands.stubs import (
    export_app,
    track_app,
)
from .utils import resolve_profile, stash_cli_flags

logger = logging.getLogger(__name__)

_COMMANDS_PKG = "moneybin.cli.commands"


class _LazyTyper(typer.Typer):
    """A Typer subclass whose commands load from a module on first dispatch.

    Typer's get_group_from_info accesses ``registered_commands`` and
    ``registered_groups`` when building the Click command tree — exactly at
    the moment a subcommand is about to be invoked. Overriding those two
    properties to trigger a module import on first access defers all of the
    module's transitive imports until the group is actually used.
    """

    def __init__(
        self,
        module_path: str,
        attr: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._lazy_module_path = module_path
        self._lazy_attr = attr
        self._lazy_loaded = False

    def _load(self) -> None:
        if self._lazy_loaded:
            return
        module = import_module(self._lazy_module_path)
        real: typer.Typer = getattr(module, self._lazy_attr)
        # Copy over the real app's registered commands and groups so that
        # Typer/Click sees the full command tree from this point forward.
        self.registered_commands = real.registered_commands  # type: ignore[assignment]
        self.registered_groups = real.registered_groups  # type: ignore[assignment]
        if real.registered_callback is not None and self.registered_callback is None:
            self.registered_callback = real.registered_callback  # type: ignore[assignment]
        # Set the flag last: if any of the above raises, a subsequent access
        # re-enters _load() and surfaces the original ImportError/etc instead
        # of masking it with a KeyError on __dict__["registered_commands"].
        self._lazy_loaded = True

    @property  # type: ignore[override]
    def registered_commands(self) -> list[Any]:  # type: ignore[override]
        self._load()
        return self.__dict__["registered_commands"]

    @registered_commands.setter
    def registered_commands(self, value: list[Any]) -> None:
        self.__dict__["registered_commands"] = value

    @property  # type: ignore[override]
    def registered_groups(self) -> list[Any]:  # type: ignore[override]
        self._load()
        return self.__dict__["registered_groups"]

    @registered_groups.setter
    def registered_groups(self, value: list[Any]) -> None:
        self.__dict__["registered_groups"] = value


def _add_lazy_typer(
    parent: typer.Typer,
    module_path: str,
    name: str,
    help_text: str,
    *,
    attr: str = "app",
) -> None:
    """Register a lazy-loaded command group on *parent*.

    The real module at *module_path* is imported only when Typer/Click first
    inspects ``registered_commands`` or ``registered_groups`` — which happens
    at subcommand dispatch time, not at ``moneybin --help`` time.

    Contract: the lazy wrapper hardcodes ``no_args_is_help=True`` and
    ``rich_markup_mode=None`` and does not merge per-module Typer kwargs.
    Lazy-loaded command modules must construct their root ``typer.Typer``
    with these same defaults — otherwise the wrapped settings will diverge
    from the underlying module's intent. The current 13 modules all comply.
    """
    lazy = _LazyTyper(
        module_path=module_path,
        attr=attr,
        name=name,
        help=help_text,
        no_args_is_help=True,
        rich_markup_mode=None,
    )
    parent.add_typer(lazy, name=name, help=help_text)


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
_add_lazy_typer(
    app,
    f"{_COMMANDS_PKG}.profile",
    name="profile",
    help_text="Manage user profiles (create, list, switch, delete, show, set)",
)
_add_lazy_typer(
    app,
    f"{_COMMANDS_PKG}.import_cmd",
    name="import",
    help_text="Import financial files into MoneyBin",
)
_add_lazy_typer(
    app,
    f"{_COMMANDS_PKG}.sync",
    name="sync",
    help_text="Sync transactions from external services",
)
_add_lazy_typer(
    app,
    f"{_COMMANDS_PKG}.categorize",
    name="categorize",
    help_text="Manage transaction categories, rules, and merchants",
)
_add_lazy_typer(
    app,
    f"{_COMMANDS_PKG}.matches",
    name="matches",
    help_text="Review and manage transaction matches",
)
_add_lazy_typer(
    app,
    f"{_COMMANDS_PKG}.transform",
    name="transform",
    help_text="Run SQLMesh data transformations",
)
_add_lazy_typer(
    app,
    f"{_COMMANDS_PKG}.synthetic",
    name="synthetic",
    help_text="Generate and manage synthetic financial data for testing",
)
app.add_typer(track_app, name="track", help="Balance tracking and net worth")
app.command(name="stats", help="Show lifetime metric aggregates")(stats.stats_command)
app.add_typer(export_app, name="export", help="Export data to external formats")
_add_lazy_typer(
    app,
    f"{_COMMANDS_PKG}.mcp",
    name="mcp",
    help_text="MCP server for AI assistant integration",
)
_add_lazy_typer(
    app,
    f"{_COMMANDS_PKG}.db",
    name="db",
    help_text="Database management and exploration",
)
app.command(
    name="logs",
    help="View, prune, or locate MoneyBin log files for the active profile.",
)(logs.logs_command)


def main() -> None:
    """Entry point for the MoneyBin CLI application."""
    app()


if __name__ == "__main__":
    main()
