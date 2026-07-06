"""``investments lots`` sub-group: list tax lots and set specific-id selection."""

from __future__ import annotations

from decimal import Decimal

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.investments import (
    InvestmentLotSelectionEntry,
    InvestmentLotsPayload,
    InvestmentLotsSelectPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.investment_service import InvestmentService

app = typer.Typer(
    help="Tax lots: list and specific-identification selection",
    no_args_is_help=True,
)


@app.command("list")
def investments_lots_list(
    account: str | None = typer.Option(
        None, "--account", help="Account ID or free-text reference"
    ),
    security: str | None = typer.Option(
        None, "--security", help="Ticker, CUSIP, ISIN, or catalog name"
    ),
    open_only: bool = typer.Option(
        True,
        "--open/--all",
        help="Show only open lots (default) or the full open+closed history",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List tax lots with remaining quantity and basis. Open lots only by default."""
    with handle_cli_errors(
        cli_actor="investments_lots_list", payload_type=InvestmentLotsPayload
    ):
        with get_database(read_only=True) as db:
            result = InvestmentService(db).lots(
                account_ref=account, security_ref=security, open_only=open_only
            )
    if output == OutputFormat.JSON:
        # No explicit sensitivity: InvestmentLotsPayload carries TXN_AMOUNT/
        # BALANCE (HIGH) fields; render_or_json derives the tier from the
        # typed payload's Annotated metadata — identical to the MCP tool.
        render_or_json(
            build_envelope(data=InvestmentLotsPayload.from_result(result)),
            output,
            cli_actor="investments_lots_list",
        )
        return
    for row in result.rows:
        state = "open" if row.is_open else "closed"
        flag = " ⚠️ basis_incomplete" if row.basis_incomplete else ""
        typer.echo(
            f"{row.lot_id:<10} {row.security_id:<8} acq={row.acquisition_date} "
            f"remaining={row.remaining_quantity} "
            f"basis_remaining={row.cost_basis_remaining} "
            f"method={row.cost_basis_method} [{state}]{flag}"
        )
    if not quiet:
        for w in result.warnings:
            typer.echo(f"⚠️  {w}", err=True)


def _parse_lot_selection(entry: str) -> tuple[str, Decimal]:
    """Parse ``LOT_ID:QUANTITY`` into a ``(lot_id, Decimal)`` pair."""
    lot_id, sep, qty_str = entry.partition(":")
    if not sep or not lot_id:
        raise ValueError(f"--lot must be LOT_ID:QUANTITY, got {entry!r}")
    return lot_id, Decimal(qty_str)


@app.command("select")
def investments_lots_select(
    disposal_txn_id: str = typer.Argument(
        ..., help="investment_transaction_id of the disposal (a sell)"
    ),
    lot: list[str] = typer.Option(
        [],
        "--lot",
        help="LOT_ID:QUANTITY (repeatable) — replaces the full selection",
    ),
    clear: bool = typer.Option(
        False,
        "--clear",
        help="Clear all lot-selection overrides for this disposal (revert to FIFO)",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Set (or clear) the full specific-identification lot selection for a disposal.

    Declarative set (Shape 1a): the listed ``(lot, quantity)`` pairs REPLACE
    any prior selection for this disposal — an omitted lot is dropped, not
    left in place. ``--clear`` submits the empty set, reverting to FIFO.
    Identical semantics to the ``investments_lots_select`` MCP tool.
    """
    if clear and lot:
        typer.echo("error: --clear and --lot are mutually exclusive", err=True)
        raise typer.Exit(2)
    if not clear and not lot:
        typer.echo("error: pass --lot LOT_ID:QTY (repeatable) or --clear", err=True)
        raise typer.Exit(2)

    with handle_cli_errors(
        cli_actor="investments_lots_select", payload_type=InvestmentLotsSelectPayload
    ):
        selections = [] if clear else [_parse_lot_selection(entry) for entry in lot]
        with get_database(read_only=False) as db:
            InvestmentService(db).select_lots(disposal_txn_id, selections, actor="cli")

    if output == OutputFormat.JSON:
        # No explicit sensitivity: selections[].quantity carries TXN_AMOUNT
        # (HIGH); render_or_json derives the tier from the typed payload's
        # Annotated metadata — identical to the investments_lots_select MCP
        # tool, which reports HIGH for this same field (cli.md).
        payload = InvestmentLotsSelectPayload(
            disposal_txn_id=disposal_txn_id,
            selections=[
                InvestmentLotSelectionEntry(lot_id=lot_id, quantity=qty)
                for lot_id, qty in selections
            ],
        )
        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="investments_lots_select",
        )
        return
    if clear:
        typer.echo(f"✅ Cleared lot selection for {disposal_txn_id} (reverts to FIFO)")
    else:
        typer.echo(
            f"✅ Set lot selection for {disposal_txn_id}: {len(selections)} lot(s)"
        )
