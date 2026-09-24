"""One root command menu for quick discovery and complete help."""

import click
import typer
from typer.core import TyperGroup

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


class RootGroup(TyperGroup):
    """Order mixed leaves and groups together, independent of Typer registration."""

    def list_commands(self, ctx: click.Context) -> list[str]:
        """List root commands in section order, then alphabetically within each."""
        order = {
            name: index
            for index, name in enumerate(
                name for commands in HELP_SECTIONS.values() for name in commands
            )
        }
        return sorted(
            super().list_commands(ctx),
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


def configure_root_help(app: typer.Typer) -> None:
    """Attach shared descriptions and native Typer help panels to registered commands."""
    entries = {
        name: (section, description)
        for section, commands in HELP_SECTIONS.items()
        for name, description in commands.items()
    }
    for command in [*app.registered_commands, *app.registered_groups]:
        if command.name in entries:
            command.rich_help_panel, command.help = entries[command.name]


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
