"""Investments top-level command group.

Owns the investment ledger (``add``, ``list``), positions (``holdings``),
and realized gains (``gains``), and mounts the ``lots`` and ``securities``
sub-groups — thin wrappers over ``InvestmentService`` per THE SPEC
(``docs/specs/investments-data-model.md`` §CLI Interface). Retires the
``accounts investments`` placeholder.

Package layout mirrors the other multi-subgroup CLI nouns (``accounts/``,
``transactions/``, ``system/``): each sub-group is its own module
(``lots.py``, ``securities.py``) with its own ``app``, mounted here — the
established pattern, not a flat single file.

``lots`` and ``securities`` are Typer sub-groups (mirroring the flat
``investments_lots_select`` / ``investments_securities_*`` MCP tool names —
see surface-design.md's CLI-nesting note) rather than the spec's illustrative
bare-noun syntax: every ``typer.Typer()`` group sets ``no_args_is_help=True``
(cli.md), so a noun with 2+ distinct actions is a sub-group with explicit
verbs, not a bare command that also carries a nested one.
"""

from __future__ import annotations

import logging
from datetime import date as date_cls
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
    InvestmentEventsPayload,
    InvestmentGainsPayload,
    InvestmentHoldingsPayload,
    InvestmentRecordPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.investment_service import InvestmentService

from . import lots, securities

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Investment ledger, positions, lots, gains, and securities catalog",
    no_args_is_help=True,
)


def _parse_date(value: str | None) -> date_cls | None:
    return date_cls.fromisoformat(value) if value is not None else None


def _parse_decimal(value: str | None) -> Decimal | None:
    return Decimal(value) if value is not None else None


# ---------------------------------------------------------------------------
# Ledger: add, list
# ---------------------------------------------------------------------------


@app.command("add")
def investments_add(
    account: str = typer.Option(
        ..., "--account", help="Account ID or free-text reference"
    ),
    type_: str = typer.Option(
        ...,
        "--type",
        help=(
            "Event type: buy, sell, reinvest, dividend, interest, "
            "capital_gain_distribution, transfer_in, transfer_out, deposit, "
            "withdrawal, split, fee, return_of_capital, other"
        ),
    ),
    date_str: str = typer.Option(..., "--date", help="Trade date YYYY-MM-DD"),
    security: str | None = typer.Option(
        None,
        "--security",
        help="Ticker, CUSIP, ISIN, or catalog name (resolved to a security_id)",
    ),
    quantity: str | None = typer.Option(
        None, "--quantity", help="Signed share/unit quantity"
    ),
    price: str | None = typer.Option(None, "--price", help="Per-unit price"),
    amount: str | None = typer.Option(
        None, "--amount", help="Signed cash amount, including fees"
    ),
    fees: str | None = typer.Option(None, "--fees", help="Transaction fees"),
    subtype: str | None = typer.Option(
        None,
        "--subtype",
        help="Type-specific refinement (e.g. qualified, interest, capital_gain)",
    ),
    acquired: str | None = typer.Option(
        None,
        "--acquired",
        help="Original acquisition date YYYY-MM-DD (transfer_in only)",
    ),
    basis: str | None = typer.Option(
        None, "--basis", help="Supplied cost basis (transfer_in only)"
    ),
    event_group: str | None = typer.Option(
        None,
        "--event-group",
        help="Link this event to an existing economic-event group",
    ),
    currency: str = typer.Option("USD", "--currency", help="ISO-4217 currency code"),
    description: str | None = typer.Option(
        None, "--description", help="Free-text description"
    ),
    output: OutputFormat = output_option,
) -> None:
    """Record one investment ledger event.

    ``--type reinvest`` atomically writes the acquisition leg AND a paired
    income row sharing one ``event_group_id`` — reports both
    ``investment_transaction_id``s.
    """
    with handle_cli_errors(
        cli_actor="investments_add", payload_type=InvestmentRecordPayload
    ):
        with get_database(read_only=False) as db:
            ids = InvestmentService(db).record_event(
                account_ref=account,
                security_ref=security,
                type_=type_,
                subtype=subtype,
                trade_date=date_cls.fromisoformat(date_str),
                quantity=_parse_decimal(quantity),
                price=_parse_decimal(price),
                amount=_parse_decimal(amount),
                fees=_parse_decimal(fees),
                acquired=_parse_date(acquired),
                basis=_parse_decimal(basis),
                event_group_id=event_group,
                currency_code=currency,
                description=description,
                actor="cli",
                created_by="cli",
            )

    if output == OutputFormat.JSON:
        # No explicit sensitivity: render_or_json derives the tier from the
        # typed payload's Annotated metadata, mirroring the MCP tool.
        render_or_json(
            build_envelope(
                data=InvestmentRecordPayload(investment_transaction_ids=ids)
            ),
            output,
            cli_actor="investments_add",
        )
        return
    for txn_id in ids:
        typer.echo(f"✅ Recorded {txn_id}")


@app.command("list")
def investments_list(
    account: str | None = typer.Option(
        None, "--account", help="Account ID or free-text reference"
    ),
    security: str | None = typer.Option(
        None, "--security", help="Ticker, CUSIP, ISIN, or catalog name"
    ),
    type_: str | None = typer.Option(None, "--type", help="Filter by event type"),
    from_: str | None = typer.Option(
        None, "--from", help="Start trade date YYYY-MM-DD (inclusive)"
    ),
    to: str | None = typer.Option(
        None, "--to", help="End trade date YYYY-MM-DD (inclusive)"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — list has no informational chatter; only data
) -> None:
    """List ledger events from the canonical investment-transaction fact table."""
    with handle_cli_errors(
        cli_actor="investments_list", payload_type=InvestmentEventsPayload
    ):
        with get_database(read_only=True) as db:
            result = InvestmentService(db).list_events(
                account_ref=account,
                security_ref=security,
                type_filter=type_,
                date_from=_parse_date(from_),
                date_to=_parse_date(to),
            )
    if output == OutputFormat.JSON:
        # No explicit sensitivity: InvestmentEventsPayload carries TXN_AMOUNT
        # (HIGH) fields; render_or_json derives the tier from the typed
        # payload's Annotated metadata — identical to the MCP tool.
        render_or_json(
            build_envelope(data=InvestmentEventsPayload.from_result(result)),
            output,
            cli_actor="investments_list",
        )
        return
    for row in result.rows:
        sec = row.security_id or "-"
        typer.echo(
            f"{row.trade_date}  {row.type:<12} {sec:<10} qty={row.quantity} "
            f"amt={row.amount} {row.currency_code}"
        )


# ---------------------------------------------------------------------------
# Positions: holdings, gains
# ---------------------------------------------------------------------------


@app.command("holdings")
def investments_holdings(
    account: str | None = typer.Option(
        None, "--account", help="Account ID or free-text reference"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Current positions: quantity, cost basis, average cost, market value.

    Market value and unrealized gain come from the most recent close at or
    before today. A position with no usable price, or one whose share count is
    known wrong, shows ``-`` rather than a zero — its ``status`` says which.
    """
    with handle_cli_errors(
        cli_actor="investments_holdings", payload_type=InvestmentHoldingsPayload
    ):
        with get_database(read_only=True) as db:
            result = InvestmentService(db).holdings(account_ref=account)
    if output == OutputFormat.JSON:
        # No explicit sensitivity: InvestmentHoldingsPayload carries BALANCE
        # (HIGH) fields; render_or_json derives the tier from the typed
        # payload's Annotated metadata — identical to the MCP tool.
        render_or_json(
            build_envelope(data=InvestmentHoldingsPayload.from_result(result)),
            output,
            cli_actor="investments_holdings",
        )
        return
    for row in result.rows:
        # "-" for an absent figure, matching avg_cost's existing NULL rendering:
        # a blank column reads as zero, and NULL here means "no number", not
        # "worth nothing". Signs come through as the Decimal carries them —
        # unrealized_gain is negative below cost (same as `gains`' gain_loss).
        avg = row.average_cost if row.average_cost is not None else "-"
        value = row.market_value if row.market_value is not None else "-"
        gain = row.unrealized_gain if row.unrealized_gain is not None else "-"
        as_of = (
            f" as_of={row.price_date} ({row.days_since_observed}d)"
            if row.price_date is not None
            else ""
        )
        typer.echo(
            f"{row.security_id:<10} qty={row.quantity} "
            f"cost_basis={row.cost_basis} avg_cost={avg} "
            f"market_value={value} unrealized_gain={gain} "
            f"status={row.valuation_status}{as_of}"
        )
    if not quiet:
        for w in result.warnings:
            typer.echo(f"⚠️  {w}", err=True)


@app.command("gains")
def investments_gains(
    account: str | None = typer.Option(
        None, "--account", help="Account ID or free-text reference"
    ),
    security: str | None = typer.Option(
        None, "--security", help="Ticker, CUSIP, ISIN, or catalog name"
    ),
    from_: str | None = typer.Option(
        None, "--from", help="Start disposal date YYYY-MM-DD (inclusive)"
    ),
    to: str | None = typer.Option(
        None, "--to", help="End disposal date YYYY-MM-DD (inclusive)"
    ),
    term: str | None = typer.Option(
        None, "--term", help="Filter by holding term: short or long"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Realized gain/loss (the 1099-B surface) from the realized-gains fact table."""
    with handle_cli_errors(
        cli_actor="investments_gains", payload_type=InvestmentGainsPayload
    ):
        with get_database(read_only=True) as db:
            result = InvestmentService(db).gains(
                account_ref=account,
                security_ref=security,
                date_from=_parse_date(from_),
                date_to=_parse_date(to),
                term=term,
            )
    if output == OutputFormat.JSON:
        # No explicit sensitivity: InvestmentGainsPayload carries BALANCE
        # (HIGH) fields; render_or_json derives the tier from the typed
        # payload's Annotated metadata — identical to the MCP tool.
        render_or_json(
            build_envelope(data=InvestmentGainsPayload.from_result(result)),
            output,
            cli_actor="investments_gains",
        )
        return
    for row in result.rows:
        typer.echo(
            f"{row.disposal_date}  {row.security_id:<8} qty={row.quantity} "
            f"proceeds={row.proceeds} basis={row.cost_basis} "
            f"gain_loss={row.gain_loss} term={row.term}"
        )
    if not quiet:
        for w in result.warnings:
            typer.echo(f"⚠️  {w}", err=True)


app.add_typer(lots.app, name="lots")
app.add_typer(securities.app, name="securities")
