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
from collections.abc import Callable
from datetime import date as date_cls
from decimal import Decimal

import typer

from moneybin.cli.output import (
    OutputFormat,
    currency_label,
    echo_applied_rates,
    output_option,
    quiet_option,
    render_or_json,
    wide_option,
)
from moneybin.cli.render import (
    Money,
    column_view,
    format_money,
    render_note,
    render_rows,
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
from moneybin.services.investment_service import (
    EventRow,
    HoldingRow,
    InvestmentService,
    RealizedGainRow,
)

from . import lots, prices, securities

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
    currency: str | None = typer.Option(
        None,
        "--currency",
        help="ISO-4217 currency code; defaults to the account's own currency",
    ),
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


_EVENTS_COLUMNS: tuple[tuple[str, Callable[[EventRow], object]], ...] = (
    ("date", lambda r: r.trade_date),
    ("type", lambda r: r.type),
    ("security", lambda r: r.security_id or "-"),
    ("quantity", lambda r: r.quantity),
    ("amount", lambda r: r.amount),
    ("currency", lambda r: currency_label(r.currency_code)),
)

_EVENTS_DEFAULT = ("date", "type", "security", "quantity", "amount")
"""Currency drops first: it repeats down a single-currency ledger, and the
amount it qualifies sits right beside it under `--wide`."""


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
    wide: bool = wide_option,
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
    if result.rows:
        view = column_view(
            _EVENTS_COLUMNS, result.rows, default=_EVENTS_DEFAULT, wide=wide
        )
        render_rows(
            view.names,
            view.rows,
            # An event's amount is cash moving in or out, so it signs itself. The
            # quantity is a share count, not an amount, and is left as stored.
            money={"amount": Money("flow")},
            total_columns=view.total,
        )


# ---------------------------------------------------------------------------
# Positions: holdings, gains
# ---------------------------------------------------------------------------

_HOLDINGS_COLUMNS: tuple[tuple[str, Callable[[HoldingRow], object]], ...] = (
    ("security", lambda r: r.security_id),
    ("quantity", lambda r: r.quantity),
    ("cost basis", lambda r: r.cost_basis),
    # A per-unit cost is DECIMAL(28,10); `format_money` rounds to two places,
    # which renders a sub-cent price as 0.00. It is left as stored for the same
    # reason `fx list` leaves a rate.
    ("avg cost", lambda r: r.average_cost if r.average_cost is not None else "-"),
    ("market value", lambda r: r.market_value),
    ("unrealized", lambda r: r.unrealized_gain),
    ("currency", lambda r: currency_label(r.currency_code)),
    ("status", lambda r: r.valuation_status),
    (
        "as of",
        lambda r: (
            f"{r.price_date} ({r.days_since_observed}d)"
            if r.price_date is not None
            else ""
        ),
    ),
)

_HOLDINGS_DEFAULT = ("security", "quantity", "market value", "unrealized", "currency")
"""What the command is for: what you hold, what it is worth, whether you are up.

Cost basis, average cost, valuation status and the observation date are the
audit trail behind those numbers rather than the answer itself, so `--wide`
carries them. Chosen by hand, not by width: a fit that measures columns would
drop `market value` — the figure the command exists to report — because it sits
in the middle.
"""


@app.command("holdings")
def investments_holdings(
    account: str | None = typer.Option(
        None, "--account", help="Account ID or free-text reference"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    wide: bool = wide_option,
) -> None:
    """Current positions: what you hold, what it is worth, and whether you are up.

    Market value and unrealized gain come from the most recent close at or
    before today. A position with no usable price, or one whose share count is
    known wrong, shows ``-`` rather than a zero; ``--wide`` adds the ``status``
    column that says which, along with cost basis, average cost, and the date
    the price was observed.

    The closing portfolio line reports ``max_days_since_observed``: the age in
    days of the stalest close behind any figure above, or ``-`` when no
    position priced. It totals market value only when every priced position
    shares one currency; otherwise it shows the per-currency split, because no
    single total exists across currencies.
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
    if result.rows:
        view = column_view(
            _HOLDINGS_COLUMNS, result.rows, default=_HOLDINGS_DEFAULT, wide=wide
        )
        render_rows(
            view.names,
            view.rows,
            # Both totals are positions, so they render unsigned and uncoloured;
            # `format_money` already spells an absent one `-` rather than a zero
            # that would read as "worth nothing". The unrealized figure is the one
            # signed number here — above cost is the good direction.
            money={
                "cost basis": Money("balance"),
                "market value": Money("balance"),
                "unrealized": Money("delta", polarity="income"),
            },
            total_columns=view.total,
        )
        # Portfolio-level disclosure, not a status line — `-q` keeps it, the
        # same rule that keeps result rows.
        stalest = (
            result.max_days_since_observed
            if result.max_days_since_observed is not None
            else "-"
        )
        by_ccy = result.market_value_by_currency
        if result.total_market_value is not None:
            total = (
                f"market_value={format_money(result.total_market_value, 'balance')} "
                f"{result.total_market_value_currency}"
            )
            if len(by_ccy) > 1:
                # A converted total is an inference, so it never appears alone:
                # the originals it was computed from print beside it.
                split = " ".join(
                    f"{code}={format_money(amount, 'balance')}"
                    for code, amount in by_ccy.items()
                )
                total += f" (converted from {split})"
        elif by_ccy:
            # No single figure exists across currencies: print the split so no
            # reader can mistake one number for the portfolio's value. Both
            # causes are named because the remedies differ and the result cannot
            # tell them apart — an unset home currency wants `moneybin profile
            # set`, a missing rate wants `moneybin refresh`.
            split = " ".join(
                f"{code}={format_money(amount, 'balance')}"
                for code, amount in by_ccy.items()
            )
            total = (
                "market_value=- (mixed currencies, no home currency or no rate) "
                f"{split}"
            )
        else:
            total = "market_value=- (no position is priced)"
        typer.echo(f"portfolio {total} max_days_since_observed={stalest}")
        # The originals above say what was converted; this says what converted
        # it. Not gated on `quiet`, for the same reason the total is not: it is
        # part of the disclosure, not a status line. Empty unless a rate was
        # actually applied, so the single-currency case stays silent.
        echo_applied_rates(result.applied_rates, result.total_market_value_currency)
    for w in result.warnings:
        render_note(f"⚠️  {w}", quiet=quiet, warn=True)


_GAINS_COLUMNS: tuple[tuple[str, Callable[[RealizedGainRow], object]], ...] = (
    ("disposed", lambda r: r.disposal_date),
    ("security", lambda r: r.security_id),
    ("quantity", lambda r: r.quantity),
    ("proceeds", lambda r: r.proceeds),
    ("basis", lambda r: r.cost_basis),
    ("gain", lambda r: r.gain_loss),
    ("term", lambda r: r.term),
)

_GAINS_DEFAULT = ("disposed", "security", "proceeds", "gain", "term")
"""When it sold, what it was, what it fetched, what you made, how it is taxed.

Quantity and cost basis are the arithmetic behind the gain rather than the
answer, so `--wide` carries them.
"""


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
    wide: bool = wide_option,
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
    if result.rows:
        view = column_view(
            _GAINS_COLUMNS, result.rows, default=_GAINS_DEFAULT, wide=wide
        )
        render_rows(
            view.names,
            view.rows,
            # Proceeds and basis are positive by construction; the gain is the
            # signed answer, and up is the good direction on a realized gain.
            money={
                "proceeds": Money("magnitude"),
                "basis": Money("balance"),
                "gain": Money("delta", polarity="income"),
            },
            total_columns=view.total,
        )
    for w in result.warnings:
        render_note(f"⚠️  {w}", quiet=quiet, warn=True)


app.add_typer(lots.app, name="lots")
app.add_typer(prices.app, name="prices")
app.add_typer(securities.app, name="securities")
