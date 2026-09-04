"""``fx`` group: resolve an exchange rate, correct one, remove the correction.

Shaped after ``investments prices``, which solves the same problem one layer
over: a provider supplies most values, the user overrides a few, and a
precedence rule decides which one a date reports. ``rate`` is the single-date
read, ``list`` the series, ``set`` and ``delete`` the correction and its
reversal.

``delete`` is not CRUD symmetry. An override outranks every cached provider row
for its own pair and date, and ``set`` can only replace the number — so without
``delete`` a correction is unreachable once written and its date can never
return to provider pricing.

Nothing here converts an amount. PR 1 produces and stores rates; the display
currency that spends them lands with the reports surface.
"""

from __future__ import annotations

import logging
from datetime import date

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import render_rows
from moneybin.cli.utils import handle_cli_errors, parse_cli_date, parse_cli_decimal
from moneybin.database import get_database
from moneybin.privacy.payloads.currency import (
    FxOverridePayload,
    FxRatePayload,
    FxRateRow,
    FxRatesPayload,
)
from moneybin.protocol.envelope import build_envelope

app = typer.Typer(
    help="Exchange rates: inspect cached reference rates and record corrections",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


@app.command("rate")
def fx_rate(
    from_currency: str = typer.Argument(..., help="ISO-4217 code to price from"),
    to_currency: str = typer.Argument(..., help="ISO-4217 code to price into"),
    rate_date: str | None = typer.Argument(
        None, help="Date the rate applies to (YYYY-MM-DD). Default: today."
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — the one line printed is the answer, not chatter
) -> None:
    """Show the rate for one currency pair on one date, and where it came from.

    Precedence is your own correction, then the cached provider rate, then the
    Friday a weekend resolves back to, then one live fetch. Nothing is ever
    substituted: a pair with no rate fails rather than reporting a nearby one,
    because a wrong rate looks exactly like a right one.

    A fetched rate is cached in raw.exchange_rates, so the same question costs
    one network call at most. Record your own with 'moneybin fx set'.
    """
    # Parsed before the connection so a typo costs no database open, and so it
    # exits 2 (usage) rather than 1 (the command ran and failed).
    requested = parse_cli_date(rate_date, "RATE_DATE") if rate_date else date.today()  # noqa: DTZ011  # a calendar date, not an instant
    with handle_cli_errors(cli_actor="fx_rate", payload_type=FxRatePayload):
        from moneybin.services.currency_service import (  # noqa: PLC0415  # polars is not cold-start cheap
            build_currency_service,
        )

        # Not read_only: resolving a rate nothing has cached yet fetches it and
        # writes it to raw.exchange_rates, which is the "seed" half of this
        # command. A read-only connection would fail on the first question
        # asked about any new pair.
        with get_database(read_only=False) as db:
            service = build_currency_service(db, actor="fx_rate")
            resolved = service.resolve_rate(from_currency, to_currency, requested)

    payload = FxRatePayload.from_resolved(resolved)
    if output == OutputFormat.JSON:
        render_or_json(build_envelope(data=payload), output, cli_actor="fx_rate")
        return
    line = (
        f"1 {payload.from_currency} = {payload.rate} {payload.to_currency} "
        f"on {payload.rate_date}"
    )
    if payload.rate_date != payload.requested_date:
        # Silent carry-forward is the failure this whole command guards against.
        # A weekend priced with Friday's rate is correct; reporting it as the
        # weekend's own rate is not.
        line += f", the last rate published on or before {payload.requested_date}"
    typer.echo(f"{line}  ({payload.source})")


@app.command("list")
def fx_list(
    from_currency: str = typer.Argument(..., help="ISO-4217 code to price from"),
    to_currency: str = typer.Argument(..., help="ISO-4217 code to price into"),
    since: str | None = typer.Option(
        None, "--since", help="Only show rates from this ISO date forward"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — list has no informational chatter; only data
) -> None:
    """Show the stored rate series for one pair, newest first.

    Reads what is already on disk and never fetches, so an empty result means
    nothing has been cached or corrected for this pair yet — ask 'moneybin fx
    rate' for a specific date to seed one.

    One row per date: the rate that actually applied, not every candidate that
    competed for it. 'source' names the provider that supplied it, or
    'override' where your own correction won.
    """
    start = parse_cli_date(since, "--since") if since else None
    with handle_cli_errors(cli_actor="fx_list", payload_type=FxRatesPayload):
        from moneybin.services.currency_service import (  # noqa: PLC0415  # polars is not cold-start cheap
            build_currency_service,
            canonical_currency,
        )

        with get_database(read_only=True) as db:
            service = build_currency_service(db, actor="fx_list")
            resolved = service.list_rates(from_currency, to_currency, since=start)

    payload = FxRatesPayload(
        from_currency=canonical_currency(from_currency),
        to_currency=canonical_currency(to_currency),
        rows=[
            FxRateRow(rate_date=row.rate_date, rate=row.rate, source=row.source)
            for row in resolved
        ],
    )
    if output == OutputFormat.JSON:
        render_or_json(build_envelope(data=payload), output, cli_actor="fx_list")
        return
    # A rate is not an amount: `format_money` rounds to two places, which turns
    # 0.87138 into 0.87 and loses the precision the series exists to record. It
    # therefore declares no money column and renders each rate as stored.
    if payload.rows:
        render_rows(
            ["date", "rate", "source"],
            [(row.rate_date, row.rate, row.source) for row in payload.rows],
            numeric=("rate",),
        )


@app.command("set")
def fx_set(
    from_currency: str = typer.Argument(..., help="ISO-4217 code to price from"),
    to_currency: str = typer.Argument(..., help="ISO-4217 code to price into"),
    rate_date: str = typer.Argument(..., help="Date the rate applies to (YYYY-MM-DD)"),
    rate: str = typer.Argument(..., help="Units of TO per one FROM, e.g. 0.87138"),
    note: str | None = typer.Option(
        None, "--note", help="Why this rate was recorded (e.g. the bank's own rate)"
    ),
    output: OutputFormat = output_option,
) -> None:
    """Record your own rate for one pair and date, outranking the provider.

    A correction beats every cached provider rate for its own date and leaves
    other dates untouched. Writes app.exchange_rate_overrides with an audit-log
    row; reverse it with 'moneybin fx delete', not by recording a zero.

    The rate is stored to 8 decimal places and must be positive: a zero rate
    would convert every balance in that currency to nothing, and since this
    outranks the provider nothing downstream would contradict it.

    FROM and TO must differ. A currency prices itself at exactly 1, and 'fx
    rate' answers that without reading this table at all.
    """
    parsed_date = parse_cli_date(rate_date, "RATE_DATE")
    parsed_rate = parse_cli_decimal(rate, "RATE")
    with handle_cli_errors(cli_actor="fx_set", payload_type=FxOverridePayload):
        from moneybin.services.currency_service import (  # noqa: PLC0415  # polars is not cold-start cheap
            build_currency_service,
            canonical_currency,
        )

        with get_database(read_only=False) as db:
            service = build_currency_service(db, actor="fx_set")
            service.set_override(
                from_currency, to_currency, parsed_date, parsed_rate, note=note
            )

    # No --refresh, unlike `investments prices set`: CurrencyService performs
    # the narrow FX-accounting restatement before this command reports success.
    payload = FxOverridePayload(
        from_currency=canonical_currency(from_currency),
        to_currency=canonical_currency(to_currency),
        rate_date=parsed_date,
        rate=parsed_rate,
        removed=False,
    )
    if output == OutputFormat.JSON:
        render_or_json(build_envelope(data=payload), output, cli_actor="fx_set")
        return
    typer.echo(
        f"✅ Recorded 1 {payload.from_currency} = {payload.rate} "
        f"{payload.to_currency} on {payload.rate_date}"
    )


@app.command("delete")
def fx_delete(
    from_currency: str = typer.Argument(..., help="ISO-4217 code to price from"),
    to_currency: str = typer.Argument(..., help="ISO-4217 code to price into"),
    rate_date: str = typer.Argument(
        ..., help="Date of the correction to remove (YYYY-MM-DD)"
    ),
    output: OutputFormat = output_option,
) -> None:
    """Remove a rate correction, returning that date to provider pricing.

    Load-bearing rather than CRUD symmetry: a correction outranks every cached
    provider rate for its date, and 'set' can only change the number, so without
    this a correction is unreachable once written. Removing one is permanent —
    the audit log records it, but the previous value is not restored by
    re-running anything.
    """
    parsed_date = parse_cli_date(rate_date, "RATE_DATE")
    with handle_cli_errors(cli_actor="fx_delete", payload_type=FxOverridePayload):
        from moneybin.services.currency_service import (  # noqa: PLC0415  # polars is not cold-start cheap
            build_currency_service,
            canonical_currency,
        )

        with get_database(read_only=False) as db:
            service = build_currency_service(db, actor="fx_delete")
            removed = service.delete_override(from_currency, to_currency, parsed_date)

    payload = FxOverridePayload(
        from_currency=canonical_currency(from_currency),
        to_currency=canonical_currency(to_currency),
        rate_date=parsed_date,
        rate=None,
        removed=removed,
    )
    if output == OutputFormat.JSON:
        render_or_json(build_envelope(data=payload), output, cli_actor="fx_delete")
        return
    pair = f"{payload.from_currency}/{payload.to_currency}"
    if removed:
        typer.echo(f"✅ Removed the {pair} correction for {payload.rate_date}")
    else:
        # Not an error: the end state the caller wanted already holds. Saying so
        # keeps "there is no correction" from reading as "yours was deleted".
        typer.echo(f"No override existed for {pair} on {payload.rate_date}")
