"""system — system and data status meta-view."""

import json
import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors

app = typer.Typer(
    help="System and data status",
    no_args_is_help=True,
)

logger = logging.getLogger(__name__)


@app.command("status")
def system_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — status is data-only; nothing to suppress
) -> None:
    """Show data inventory and pending review queue counts."""
    from moneybin.services.system_service import SystemService

    with handle_cli_errors() as db:
        s = SystemService(db).status()

    min_d, max_d = s.transactions_date_range
    if output == OutputFormat.JSON:
        typer.echo(
            json.dumps(
                {
                    "accounts_count": s.accounts_count,
                    "transactions_count": s.transactions_count,
                    "transactions_date_range": [
                        min_d.isoformat() if min_d else None,
                        max_d.isoformat() if max_d else None,
                    ],
                    "last_import_at": s.last_import_at.isoformat()
                    if s.last_import_at
                    else None,
                    "matches_pending": s.matches_pending,
                    "categorize_pending": s.categorize_pending,
                },
                indent=2,
                default=str,
            )
        )
        return

    typer.echo(f"Accounts: {s.accounts_count}")
    if s.transactions_count:
        typer.echo(f"Transactions: {s.transactions_count} ({min_d} – {max_d})")
    else:
        typer.echo("Transactions: 0")
    typer.echo(f"Last import: {s.last_import_at or 'never'}")
    typer.echo(f"Matches pending: {s.matches_pending}")
    typer.echo(f"Uncategorized: {s.categorize_pending}")
