"""Stub commands for features not yet implemented.

These reserve the CLI namespace and provide clear messages directing
users to the relevant spec or future release. Each stub will be replaced
by a real implementation when its owning spec is executed.
"""

import logging

import typer

logger = logging.getLogger(__name__)

__all__ = ["_not_implemented"]


def _not_implemented(owning_spec: str) -> None:
    """Print a not-implemented message and exit cleanly.

    Args:
        owning_spec: The spec filename under docs/specs/ that owns this feature.
    """
    logger.info("This command is not yet implemented.")
    logger.info(f"💡 See docs/specs/{owning_spec} for the design")


# --- track ---
track_app = typer.Typer(
    help="Balance tracking, net worth, and financial monitoring",
    no_args_is_help=True,
)

track_balance_app = typer.Typer(help="Balance assertions and tracking")
track_app.add_typer(track_balance_app, name="balance")


@track_balance_app.command("show")
def track_balance_show() -> None:
    """Show current balance for an account."""
    _not_implemented("net-worth.md")


track_networth_app = typer.Typer(help="Net worth tracking")
track_app.add_typer(track_networth_app, name="networth")


@track_networth_app.command("show")
def track_networth_show() -> None:
    """Show current net worth."""
    _not_implemented("net-worth.md")


track_budget_app = typer.Typer(help="Budget tracking", no_args_is_help=True)
track_app.add_typer(track_budget_app, name="budget")


@track_budget_app.command("show")
def track_budget_show() -> None:
    """Show budget status."""
    _not_implemented("budget-tracking.md")


track_recurring_app = typer.Typer(
    help="Recurring transaction detection", no_args_is_help=True
)
track_app.add_typer(track_recurring_app, name="recurring")


@track_recurring_app.command("show")
def track_recurring_show() -> None:
    """Show detected recurring transactions."""
    _not_implemented("recurring-transactions.md")


track_investments_app = typer.Typer(help="Investment tracking", no_args_is_help=True)
track_app.add_typer(track_investments_app, name="investments")


@track_investments_app.command("show")
def track_investments_show() -> None:
    """Show investment portfolio."""
    _not_implemented("investment-tracking.md")


# --- export ---
export_app = typer.Typer(
    help="Export data to CSV, Excel, and other formats", no_args_is_help=True
)


@export_app.command("run")
def export_run() -> None:
    """Export financial data to a file."""
    _not_implemented("export.md")
