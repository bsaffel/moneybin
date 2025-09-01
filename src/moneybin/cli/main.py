"""Main CLI application for MoneyBin.

This module provides the unified entry point for all MoneyBin CLI operations,
organizing commands into logical groups for data extraction, credential management,
and system utilities.
"""

import typer

from .commands import credentials, extract

app = typer.Typer(
    name="moneybin",
    help="MoneyBin: Personal financial data aggregation and analysis tool",
    add_completion=False,
    rich_markup_mode="rich",
    no_args_is_help=True,
)

# Add command groups
app.add_typer(extract.app, name="extract", help="Data extraction commands")
app.add_typer(
    credentials.app, name="credentials", help="Credential management commands"
)


def main() -> None:
    """Entry point for the MoneyBin CLI application."""
    app()


if __name__ == "__main__":
    main()
