"""One root command menu for quick discovery and complete help."""

import importlib
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, cast

import typer
from typer import _click as click
from typer.core import TyperCommand, TyperGroup

HELP_SECTIONS: dict[str, dict[str, str]] = {
    "Your finances": {
        "accounts": "View and manage accounts",
        "assets": "Track property, vehicles, and valuables",
        "investments": "Explore positions, lots, and gains",
        "reports": "View financial reports",
        "transactions": "Browse and manage transactions",
    },
    "Review and organize": {
        "categories": "Manage your category taxonomy",
        "fx": "Inspect and correct exchange rates",
        "merchants": "Manage merchant mappings",
        "review": "See pending decisions across review queues",
    },
    "Import, sync, and export": {
        "export": "Export data to other formats",
        "gsheet": "Connect Google Sheets workbooks",
        "import": "Import financial files",
        "refresh": "Update derived data and categorization",
        "sync": "Pull data from connected services",
    },
    "Setup and connections": {
        "demo": "Try MoneyBin with sample data",
        "mcp": "Connect MoneyBin to AI assistants",
        "privacy": "Inspect redaction and privacy audit tools",
        "profile": "Create, switch, and manage profiles",
    },
    "Advanced tools": {
        "db": "Inspect and manage the database",
        "logs": "View and manage application logs",
        "sql": "Run privacy-safe SQL queries",
        "stats": "View lifetime operational metrics",
        "synthetic": "Generate and manage test data",
        "system": "Check system and data status",
        "transform": "Run data transformations",
    },
}
_ENTRIES: dict[str, tuple[str, str]] = {
    name: (section, description)
    for section, commands in HELP_SECTIONS.items()
    for name, description in commands.items()
}
SHORT_COMMANDS = frozenset({
    "accounts",
    "investments",
    "reports",
    "transactions",
    "review",
    "export",
    "import",
    "sync",
    "demo",
})


@dataclass(frozen=True)
class LazyCommand:
    """A root command imported on first use rather than when ``cli.main`` loads."""

    module: str
    attr: str  # a ``typer.Typer`` (group) or a command function (leaf)
    hidden: bool = False


class _LazyCommands(Mapping[str, click.Command]):
    """The root's ``commands`` mapping: every name is known, each value loads on access.

    Anything that walks the click tree (``group.commands[name]``) keeps working;
    only help and completion avoid the import, through ``RootGroup``'s stubs.
    """

    def __init__(self, group: "RootGroup") -> None:
        self._group = group

    def __getitem__(self, name: str) -> click.Command:
        if name not in self._group.lazy_commands:
            raise KeyError(name)
        return self._group.load(name)

    def __iter__(self) -> Iterator[str]:
        return iter(self._group.lazy_commands)

    def __len__(self) -> int:
        return len(self._group.lazy_commands)


class RootGroup(TyperGroup):
    """Order mixed leaves and groups together, independent of Typer registration.

    Commands come from ``lazy_commands`` and load on first resolution, so
    ``--help`` and the start menu never import a command group's service graph.
    """

    lazy_commands: ClassVar[dict[str, LazyCommand]] = {}
    _listing = False  # True while rendering help or completion: stubs, no imports

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Start with nothing loaded."""
        super().__init__(*args, **{**kwargs, "commands": _LazyCommands(self)})
        self._loaded: dict[str, click.Command] = {}

    def _stub(self, name: str) -> click.Command:
        """Describe a command for a listing from navigation data alone."""
        panel, description = _ENTRIES.get(name, (None, None))
        return TyperCommand(
            name,
            short_help=description,
            rich_help_panel=panel,
            hidden=self.lazy_commands[name].hidden,
        )

    def load(self, name: str) -> click.Command:
        """Import and build the real command, once."""
        if name not in self._loaded:
            spec = self.lazy_commands[name]
            target = getattr(importlib.import_module(spec.module), spec.attr)
            # Same markup mode as the root app in main.py, which sub-apps don't inherit.
            loader = typer.Typer(rich_markup_mode="rich", add_completion=False)
            command: click.Command
            if isinstance(target, typer.Typer):
                loader.add_typer(target, name=name, hidden=spec.hidden)
                group = cast(TyperGroup, typer.main.get_command(loader))
                command = group.commands[name]
            else:
                loader.command(name=name)(target)
                command = typer.main.get_command(loader)
            if name in _ENTRIES and isinstance(command, (TyperCommand, TyperGroup)):
                command.rich_help_panel, command.short_help = _ENTRIES[name]
            self._loaded[name] = command
        return self._loaded[name]

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        """Resolve a command, importing it unless a listing only needs its summary."""
        if self._listing and cmd_name in self.lazy_commands:
            return self._stub(cmd_name)
        return super().get_command(ctx, cmd_name)

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """Render help from stubs so listing every command imports none of them."""
        self._listing = True
        try:
            super().format_help(ctx, formatter)
        finally:
            self._listing = False

    def shell_complete(
        self, ctx: click.Context, incomplete: str
    ):  # return type is click's private CompletionItem
        """Complete command names from stubs, for the same reason as help."""
        self._listing = True
        try:
            return super().shell_complete(ctx, incomplete)
        finally:
            self._listing = False

    def list_commands(self, ctx: click.Context) -> list[str]:
        """List root commands in section order, then alphabetically within each."""
        order = {
            name: index
            for index, name in enumerate(
                name for commands in HELP_SECTIONS.values() for name in commands
            )
        }
        return sorted(
            self.lazy_commands,  # not super(): it iterates .items(), which loads
            key=lambda name: (order.get(name, len(order)), name),
        )

    def format_commands(
        self, ctx: click.Context, formatter: click.HelpFormatter
    ) -> None:
        """Preserve sections when Typer uses Click's plain help renderer."""
        sections: dict[str, list[tuple[str, str]]] = {}
        for name in self.list_commands(ctx):
            command = self.get_command(ctx, name)
            if command is None or command.hidden:
                continue
            section = getattr(command, "rich_help_panel", None) or "Commands"
            sections.setdefault(section, []).append((
                name,
                command.get_short_help_str(),
            ))
        for title, rows in sections.items():
            with formatter.section(title):
                formatter.write_dl(rows)


def show_start_menu() -> None:
    """Render discovery without loading any profile or application settings."""
    import sys

    from moneybin.cli.render import render_command_menu
    from moneybin.cli.terminal import resolve_terminal_policy
    from moneybin.config import CLISettings

    terminal = resolve_terminal_policy(
        stdin=sys.stdin,
        stdout=sys.stdout,
        stderr=sys.stderr,
        output="text",
        quiet=False,
        no_pager=True,
        settings=CLISettings(),
    )
    sections = [
        (
            section,
            [
                (name, description)
                for name, description in commands.items()
                if name in SHORT_COMMANDS
            ],
        )
        for section, commands in HELP_SECTIONS.items()
    ]
    render_command_menu(sections, terminal=terminal)
