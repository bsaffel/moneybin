"""``investments lots`` sub-group: list tax lots and set specific-id selection."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import typer

from moneybin.cli.output import (
    OutputFormat,
    currency_label,
    output_option,
    quiet_option,
    render_or_json,
    wide_option,
)
from moneybin.cli.render import Money, column_view, render_note, render_rows
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.investments import (
    InvestmentLotSelectionEntry,
    InvestmentLotsPayload,
    InvestmentLotsSelectPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.investment_service import InvestmentService, LotRow

app = typer.Typer(
    help="Tax lots: list and specific-identification selection",
    no_args_is_help=True,
)


_LOTS_COLUMNS: tuple[tuple[str, Callable[[LotRow], object]], ...] = (
    ("lot", lambda r: r.lot_id),
    ("security", lambda r: r.security_id),
    ("acquired", lambda r: r.acquisition_date),
    ("remaining", lambda r: r.remaining_quantity),
    ("basis", lambda r: r.cost_basis_remaining),
    ("currency", lambda r: currency_label(r.currency_code)),
    ("method", lambda r: r.cost_basis_method),
    ("state", lambda r: "open" if r.is_open else "closed"),
    ("note", lambda r: "\u26a0\ufe0f basis_incomplete" if r.basis_incomplete else ""),
)

_LOTS_DEFAULT = ("lot", "security", "acquired", "remaining", "basis", "note")
"""Which lot, of what, bought when, how much is left, at what basis — and
whether that basis is known to be incomplete.

Method and currency are the qualifiers a reader consults after the fact, so
`--wide` carries them. `note` is not one of those: it says the `basis` cell
beside it is a floor rather than a figure, which qualifies the answer rather
than commenting on the run. Its only substitute was the warning line from
`InvestmentService.lots`, and that goes through `render_note` — so `-q` dropped
it and left a conservative basis reading as an authoritative one.

`currency` is the one column here declared against its own argument.
`investments list`, `gains` and `holdings` all keep it in the default view,
because none of the four takes a currency filter and `multi-currency.md` makes
the row's own `currency_code` the canonical unit of its amount. This table is
the one that cannot afford it. Measured at 80 columns with production-width
ids rather than argued from the header contract, which bounds only the header
row: six columns already fold the lot id by a character and break
`⚠️ basis_incomplete` across lines, and a seventh folds `security` too and
takes the marker to three lines. Slots here are contested, so the one that goes
to `state` below buys an answer nothing else supplies, while `currency` repeats
down all but the rare mixed-denomination ledger and is reachable with `--wide`.
"""

_LOTS_ALL_DEFAULT = (
    "lot",
    "security",
    "acquired",
    "remaining",
    "basis",
    "state",
    "note",
)
"""The `--all` view, which is the default set plus `state`.

`--all` asks for the mixed open-and-closed history, and the curated view named
neither. The state is strictly derivable — `core.fct_investment_lots` defines
`is_open` as `remaining_quantity > 0` — but that rule lives in the model, not
on screen and not in `--help`, so without this column a reader infers a lot's
lifecycle from a numeric cell via a rule nothing shows them. Naming it is what
a column is for. Under `--open` the answer is constant and the column would
read `open` all the way down, so that *default* view does not pay for it —
`--wide` still shows it there, because `--wide` is every declared column and
not a second curated set, and the command's own `--help` says so. This is the
one table whose default set depends on the query, because it is the one command
whose result changes kind rather than size.
"""


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
    wide: bool = wide_option,
) -> None:
    """List tax lots with remaining quantity and basis. Open lots only by default.

    Shows the lot, its security, when it was acquired, how much remains, the
    remaining basis, and a note marking any basis known to be incomplete.
    ``--all`` adds an open/closed ``state`` column, since that view returns
    both. ``--wide`` shows every declared column: the currency, the cost-basis
    method, and ``state`` — which under the default ``--open`` reads ``open``
    on every row.

    Lots in an account whose investment ledger arrives from two sources at once
    double-count, because the two ledgers interleave rather than merge. The
    command says so on stderr and ``-q`` does not silence it — unlike the
    incomplete-basis note, which has a per-row marker to fall back on — and
    under ``--output json`` it rides ``summary.degraded_reason``, the same code
    the holdings read uses.
    """
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
            build_envelope(
                data=InvestmentLotsPayload.from_result(result),
                degraded=result.degraded_reason is not None,
                degraded_reason=result.degraded_reason,
            ),
            output,
            cli_actor="investments_lots_list",
        )
        return
    if result.rows:
        view = column_view(
            _LOTS_COLUMNS,
            result.rows,
            default=_LOTS_DEFAULT if open_only else _LOTS_ALL_DEFAULT,
            wide=wide,
        )
        render_rows(
            view.names,
            view.rows,
            # A lot's remaining basis is a position rather than a movement, so it
            # renders unsigned and uncoloured. The remaining quantity is a share
            # count, not an amount, and is left as stored.
            money={"basis": Money("balance")},
            numeric=("remaining",),
            total_columns=view.total,
        )
    for w in result.warnings:
        # `-q` drops status lines, not disclosures about the figures. The
        # basis-incomplete warning is droppable because the default view keeps
        # a per-row `⚠️ basis_incomplete` marker; a source overlap has no such
        # fallback here — `investments holdings` leans on its `status` column
        # and this table has none — so silencing it would leave a doubled
        # quantity and basis on screen with nothing saying so.
        render_note(
            f"⚠️  {w}",
            quiet=quiet and w != result.degraded_reason,
            warn=True,
        )


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

    Requires the security to resolve to ``specific`` cost basis — only specific
    identification consumes lot selections. Elect it first with
    ``moneybin investments securities set <security-id> --method specific``;
    ``--clear`` needs no election.
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
