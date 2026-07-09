"""``investments securities`` sub-group: list, add, and partial-update the catalog."""

from __future__ import annotations

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
    InvestmentSecuritiesPayload,
    InvestmentSecuritySetPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.investment_service import InvestmentService

app = typer.Typer(
    help="Manually-maintained securities catalog",
    no_args_is_help=True,
)


@app.command("list")
def investments_securities_list(
    type_: str | None = typer.Option(
        None,
        "--type",
        help="Filter by security_type (equity, etf, mutual_fund, bond, crypto, cash, other)",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — list has no informational chatter; only data
) -> None:
    """List the securities catalog."""
    with handle_cli_errors(
        cli_actor="investments_securities_list",
        payload_type=InvestmentSecuritiesPayload,
    ):
        with get_database(read_only=True) as db:
            result = InvestmentService(db).list_securities(security_type=type_)
    if output == OutputFormat.JSON:
        # No explicit sensitivity: render_or_json derives the tier from the
        # typed payload's Annotated metadata (resolves to "low" — reference
        # data only) instead of a hardcoded literal that bypasses the
        # audit-trail classes_returned walk (cli.md).
        render_or_json(
            build_envelope(data=InvestmentSecuritiesPayload.from_result(result)),
            output,
            cli_actor="investments_securities_list",
        )
        return
    for row in result.rows:
        typer.echo(
            f"{row.security_id:<12} {row.ticker or '-':<8} "
            f"{row.name:<30} {row.security_type}"
        )


@app.command("add")
def investments_securities_add(
    name: str = typer.Option(..., "--name", help="Display name"),
    type_: str = typer.Option(
        ...,
        "--type",
        help="equity, etf, mutual_fund, bond, crypto, cash, or other",
    ),
    ticker: str | None = typer.Option(None, "--ticker", help="Display/lookup ticker"),
    exchange: str | None = typer.Option(None, "--exchange", help="Listing exchange"),
    cusip: str | None = typer.Option(None, "--cusip", help="CUSIP identifier"),
    isin: str | None = typer.Option(None, "--isin", help="ISIN identifier"),
    figi: str | None = typer.Option(None, "--figi", help="OpenFIGI mapping"),
    coingecko_id: str | None = typer.Option(
        None, "--coingecko-id", help="Crypto price-lookup slug"
    ),
    cash_equivalent: bool = typer.Option(
        False,
        "--cash-equivalent/--no-cash-equivalent",
        help="Treat like cash (money-market/sweep fund)",
    ),
    method: str | None = typer.Option(
        None,
        "--method",
        help=(
            "Cost-basis method: fifo, hifo, specific, or average "
            "(average requires mutual_fund or etf)"
        ),
    ),
    currency: str = typer.Option(
        "USD", "--currency", help="ISO-4217 denominating currency"
    ),
    output: OutputFormat = output_option,
) -> None:
    """Add one security to the catalog."""
    with handle_cli_errors(
        cli_actor="investments_securities_add",
        payload_type=InvestmentSecuritySetPayload,
    ):
        with get_database(read_only=False) as db:
            security_id = InvestmentService(db).upsert_security(
                security_id=None,
                name=name,
                security_type=type_,
                ticker=ticker,
                exchange=exchange,
                cusip=cusip,
                isin=isin,
                figi=figi,
                coingecko_id=coingecko_id,
                is_cash_equivalent=cash_equivalent,
                cost_basis_method=method,
                currency_code=currency,
                actor="cli",
            )
    if output == OutputFormat.JSON:
        # No explicit sensitivity: render_or_json derives the tier from the
        # typed payload's Annotated metadata, mirroring the MCP tool.
        render_or_json(
            build_envelope(data=InvestmentSecuritySetPayload(security_id=security_id)),
            output,
            cli_actor="investments_securities_add",
        )
        return
    typer.echo(f"✅ Added security {security_id}")


@app.command("set")
def investments_securities_set(
    security_id: str = typer.Argument(..., help="Security ID"),
    name: str | None = typer.Option(None, "--name", help="Display name"),
    ticker: str | None = typer.Option(None, "--ticker", help="Display/lookup ticker"),
    exchange: str | None = typer.Option(None, "--exchange", help="Listing exchange"),
    cusip: str | None = typer.Option(None, "--cusip", help="CUSIP identifier"),
    isin: str | None = typer.Option(None, "--isin", help="ISIN identifier"),
    figi: str | None = typer.Option(None, "--figi", help="OpenFIGI mapping"),
    coingecko_id: str | None = typer.Option(
        None, "--coingecko-id", help="Crypto price-lookup slug"
    ),
    method: str | None = typer.Option(
        None,
        "--method",
        help=(
            "Cost-basis method: fifo, hifo, specific, or average "
            "(average requires mutual_fund or etf)"
        ),
    ),
    currency: str | None = typer.Option(
        None, "--currency", help="ISO-4217 denominating currency"
    ),
    output: OutputFormat = output_option,
) -> None:
    """Update one or more fields of an existing security (partial update).

    Unset flags leave the existing value untouched; ``security_type`` is not
    settable here (immutable post-creation in v1).
    """
    if all(
        v is None
        for v in (
            name,
            ticker,
            exchange,
            cusip,
            isin,
            figi,
            coingecko_id,
            method,
            currency,
        )
    ):
        typer.echo("error: at least one --field flag is required", err=True)
        raise typer.Exit(2)

    with handle_cli_errors(
        cli_actor="investments_securities_set",
        payload_type=InvestmentSecuritySetPayload,
    ):
        with get_database(read_only=False) as db:
            InvestmentService(db).set_security(
                security_id,
                name=name,
                ticker=ticker,
                exchange=exchange,
                cusip=cusip,
                isin=isin,
                figi=figi,
                coingecko_id=coingecko_id,
                cost_basis_method=method,
                currency_code=currency,
                actor="cli",
            )
    if output == OutputFormat.JSON:
        # No explicit sensitivity: render_or_json derives the tier from the
        # typed payload's Annotated metadata, mirroring the MCP tool.
        render_or_json(
            build_envelope(data=InvestmentSecuritySetPayload(security_id=security_id)),
            output,
            cli_actor="investments_securities_set",
        )
        return
    typer.echo(f"✅ Updated security {security_id}")
