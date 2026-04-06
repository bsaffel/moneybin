"""Data synchronization commands for MoneyBin CLI.

Syncs financial data via the moneybin_server service, which handles
institution connections (Plaid, etc.) on the backend.

TODO: Implement once moneybin_server exposes a sync API.
"""

import logging

import typer

app = typer.Typer(
    help="Sync financial data from external services", no_args_is_help=True
)
logger = logging.getLogger(__name__)


@app.command("all")
def sync_all(
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force full sync, bypassing incremental logic",
    ),
) -> None:
    """Sync financial data from all configured institutions via moneybin_server."""
    # TODO: Call moneybin_server sync API once available.
    logger.error("❌ Sync is not yet available. Coming soon via moneybin_server.")
    raise typer.Exit(1)
