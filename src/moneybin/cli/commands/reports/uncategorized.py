"""moneybin reports uncategorized — curator queue for uncategorized transactions."""

from __future__ import annotations

import dataclasses
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
from moneybin.database import get_database
from moneybin.protocol.envelope import build_envelope
from moneybin.services.reports_service import ReportsService

logger = logging.getLogger(__name__)


def reports_uncategorized(
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
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            payload = ReportsService(db).uncategorized_queue(
                min_amount=min_amount_dec, account=account, limit=limit
            )

    def _render_text(_: object) -> None:
        if not payload.rows:
            return
        cols = list(dataclasses.asdict(payload.rows[0]).keys())
        rows = [tuple(dataclasses.asdict(r).values()) for r in payload.rows]
        render_rich_table(cols, rows)

    render_or_json(
        build_envelope(data=payload, sensitivity="medium"),
        output,
        render_fn=_render_text,
        cli_actor="reports_uncategorized",
    )
