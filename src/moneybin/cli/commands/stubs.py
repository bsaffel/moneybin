"""Stub commands for features not yet implemented.

These reserve the CLI namespace and provide clear messages directing
users to the relevant spec or future release. Each stub will be replaced
by a real implementation when its owning spec is executed.
"""

import logging

import typer

logger = logging.getLogger(__name__)


def _not_implemented(owning_spec: str) -> None:
    """Print a not-implemented message and exit cleanly.

    Args:
        owning_spec: The spec filename under docs/specs/ that owns this feature.
    """
    logger.info("This command is not yet implemented.")
    logger.info(f"💡 See docs/specs/{owning_spec} for the design")


# --- matches ---
matches_app = typer.Typer(
    help="Review and manage transaction matches (dedup, transfers)",
    no_args_is_help=True,
)


@matches_app.command("run")
def matches_run() -> None:
    """Run matcher against existing transactions."""
    _not_implemented("matching-same-record-dedup.md")


@matches_app.command("review")
def matches_review() -> None:
    """Interactive: accept/reject/skip/quit match proposals."""
    _not_implemented("matching-same-record-dedup.md")


@matches_app.command("log")
def matches_log() -> None:
    """Show recent match decisions."""
    _not_implemented("matching-same-record-dedup.md")


@matches_app.command("undo")
def matches_undo() -> None:
    """Reverse a match decision."""
    _not_implemented("matching-same-record-dedup.md")


@matches_app.command("backfill")
def matches_backfill() -> None:
    """One-time scan of all existing transactions."""
    _not_implemented("matching-same-record-dedup.md")


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


track_budget_app = typer.Typer(help="Budget tracking")
track_app.add_typer(track_budget_app, name="budget")


@track_budget_app.callback(invoke_without_command=True)
def track_budget_stub() -> None:
    """Budget tracking commands."""
    _not_implemented("budget-tracking.md")


track_recurring_app = typer.Typer(help="Recurring transaction detection")
track_app.add_typer(track_recurring_app, name="recurring")


@track_recurring_app.callback(invoke_without_command=True)
def track_recurring_stub() -> None:
    """Recurring transaction commands."""
    logger.info("This command is not yet implemented.")
    logger.info("💡 This feature is planned for a future spec")


track_investments_app = typer.Typer(help="Investment tracking")
track_app.add_typer(track_investments_app, name="investments")


@track_investments_app.callback(invoke_without_command=True)
def track_investments_stub() -> None:
    """Investment tracking commands."""
    _not_implemented("investment-tracking.md")


# --- export ---
export_app = typer.Typer(help="Export data to CSV, Excel, and other formats")


@export_app.callback(invoke_without_command=True)
def export_callback() -> None:
    """Export financial data."""
    _not_implemented("export.md")


# --- stats ---
def stats_command() -> None:
    """Show lifetime metric aggregates."""
    _not_implemented("observability.md")


# --- db migrate ---
db_migrate_app = typer.Typer(help="Database migration management")


@db_migrate_app.command("apply")
def db_migrate_apply() -> None:
    """Apply pending database migrations."""
    _not_implemented("database-migration.md")


@db_migrate_app.command("status")
def db_migrate_status() -> None:
    """Show migration state."""
    _not_implemented("database-migration.md")
