"""moneybin reports uncategorized — curator queue for uncategorized transactions."""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import (
    handle_cli_errors,
    render_rich_table,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.reports_service import ReportsService

logger = logging.getLogger(__name__)

uncategorized_app = typer.Typer(
    help="Curator queue for uncategorized transactions",
    no_args_is_help=True,
)


@uncategorized_app.command("show")
def reports_uncategorized_show(
    min_amount: str = typer.Option(
        "0", "--min-amount", help="Filter to absolute amount >= this"
    ),
    account: str | None = typer.Option(
        None, "--account", help="Filter to account name"
    ),
    limit: int = typer.Option(50, "--limit", help="Maximum rows"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show uncategorized transactions ordered by curator-impact."""
    try:
        min_amount_dec = Decimal(min_amount)
    except InvalidOperation as e:
        raise typer.BadParameter(f"Invalid --min-amount: {min_amount}") from e
    with handle_cli_errors(output=output) as db:
        cols, rows = ReportsService(db).uncategorized_queue(
            min_amount=min_amount_dec, account=account, limit=limit
        )

    def _render_text(_: object) -> None:
        render_rich_table(cols, rows)

    render_or_json(
        build_envelope(
            data=[dict(zip(cols, r, strict=False)) for r in rows], sensitivity="low"
        ),
        output,
        render_fn=_render_text,
    )
