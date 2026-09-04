"""``investments prices`` sub-group: refresh feeds, mark prices by hand, inspect.

Sensitivity derives from the payload, and for a per-UNIT close that is `low`:
a market close is public reference data (what a security was worth on a date),
not a personal fact about the user — unlike
`core.fct_investment_transactions.price`, which is what the user actually paid.
The elevated figure is `market_value` (quantity x close), which reveals position
size and stays `high` on the holdings surface. See the registry entries for
`core.fct_security_prices.close` and `app.security_price_overrides.close`.

`delete` is not CRUD symmetry. Source precedence ranks an override above every
provider row for its own date, and `set` can only replace the value while keeping
`source = 'override'` provenance — so without `delete` a mark is unreachable once
written and its date can never return to provider-derived valuation.

`token` is CLI-only under the secret-material exemption (`mcp.md` category 1):
routing an API credential through an LLM-mediated channel is a security-model
violation, not a capability gap.
"""

from __future__ import annotations

import logging
from enum import StrEnum

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
from moneybin.privacy.payloads.investments import (
    InvestmentFailedSourceEntry,
    InvestmentPriceMarkPayload,
    InvestmentPricePullPayload,
    InvestmentPricesPayload,
    InvestmentUnpricedEntry,
)
from moneybin.protocol.envelope import build_envelope

app = typer.Typer(
    help="Market prices for held securities: refresh feeds, mark by hand, inspect",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


class PriceSourceChoice(StrEnum):
    """The sources that can win a date in ``core.fct_security_prices``.

    Rejecting an unknown source at parse time surfaces a usage error (exit 2).
    Without it ``--source tiingoo`` reaches ``list_prices``' equality predicate
    and returns an ordinary empty series — indistinguishable from a security
    that genuinely has no Tiingo close, which is the one answer a typo must
    never be able to fake.
    """

    PLAID = "plaid"
    TIINGO = "tiingo"
    COINGECKO = "coingecko"
    OVERRIDE = "override"
    TRADE_IMPLIED = "trade_implied"


def _report_refresh_failure(error: str | None) -> None:
    """Surface a failed apply and exit non-zero, in both output modes.

    ``refresh()`` soft-fails so the pull's already-committed rows survive, which
    leaves the exit code as the only thing a script can gate on. The retry is a
    bare ``refresh run``: ``raw.security_prices`` is append-only and the closes
    are already stored, so re-pulling would spend the provider's rate limit
    re-fetching rows that are sitting there. Mirrors ``sync pull``.
    """
    if not error:
        return
    logger.warning(
        f"⚠️  transforms failed ({error}); the new closes landed in raw and are "
        "not valuing holdings yet. Retry with 'moneybin refresh run'."
    )
    raise typer.Exit(1)


def _echo_refresh_hint(what: str, *, stale: bool) -> None:
    """Name the apply that makes a just-written price visible.

    Every write in this group lands in raw or app while every consumer reads
    ``core.fct_security_prices``, so without this a user who writes and
    immediately lists sees the pre-write series with nothing explaining why.
    One construction for all three commands: the boundary is the same, and two
    hint strings for one condition drift.

    Silent when the command already propagated the change, and when nothing
    changed — a hint naming work with no effect is noise.
    """
    if not stale:
        return
    typer.echo(
        f"💡 {what} once the models rebuild — run 'moneybin refresh run', "
        "or pass --refresh next time"
    )


@app.command("pull")
def investments_prices_pull(
    securities: list[str] = typer.Option(  # noqa: B008  # typer declares defaults in the signature
        [],
        "--security",
        help="Limit to these securities (ticker, CUSIP, ISIN, name, or id). Repeatable.",
    ),
    since: str | None = typer.Option(
        None,
        "--since",
        help="Fetch closes from this ISO date forward. Default: the last few days.",
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Rebuild the price models so the new closes value holdings right away",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Refresh stored prices for held securities from the configured feeds.

    Fetch scope comes from open positions: a closed position's price answers no
    question, and fetching the whole catalog exhausts a provider's rate limit.

    A security whose feed key cannot be derived with confidence is queued for
    review rather than bound to a guess — a ticker is not an identifier, so the
    same symbol can name a different security at the provider.

    Closes land in raw.security_prices; holdings value from core. Pass --refresh
    to rebuild the models in the same command, or run 'moneybin refresh run'
    afterwards.
    """
    with handle_cli_errors(
        cli_actor="investments_prices_pull",
        payload_type=InvestmentPricePullPayload,
    ):
        # Deferred: the adapters pull in httpx, which every CLI invocation would
        # otherwise pay for at import time (cli.md cold-start hygiene).
        from moneybin.orchestration import refresh as refresh_module  # noqa: PLC0415
        from moneybin.services.price_service import build_price_service  # noqa: PLC0415

        start = parse_cli_date(since, "--since") if since else None
        with get_database(read_only=False) as db:
            service = build_price_service(db, actor="investments_prices_pull")
            security_ids = [service.resolve_security(ref) for ref in securities] or None
            result = service.pull(security_ids=security_ids, since=start)
            # Unconditional when asked for, unlike sync's rows_changed gate: that
            # gate exists because sync refreshes on its own, so a no-op apply is
            # pure cost. Here the user named the flag, and a pull writing nothing
            # this time can still be the one that follows a pull left unapplied.
            refreshed = (
                refresh_module.refresh(db, steps=["transform"]) if refresh else None
            )

    payload = InvestmentPricePullPayload(
        rows_written=result.rows_written,
        observations=result.observations,
        securities_priced=result.securities_priced,
        queued_for_review=result.queued_for_review,
        unpriced=[
            InvestmentUnpricedEntry(security_id=u.security_id, reason=u.reason)
            for u in result.unpriced
        ],
        failed_sources=[
            InvestmentFailedSourceEntry(source_type=f.source_type, message=f.message)
            for f in result.failed_sources
        ],
        refreshed=bool(refreshed and refreshed.applied),
        refresh_error=refreshed.error if refreshed else None,
    )
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=payload), output, cli_actor="investments_prices_pull"
        )
        _report_refresh_failure(payload.refresh_error)
        return
    if not quiet:
        typer.echo(
            f"✅ {result.rows_written} new price rows "
            f"({result.securities_priced} securities priced)"
        )
        if result.queued_for_review:
            typer.echo(
                f"👀 {result.queued_for_review} feed key(s) need review — "
                "run 'moneybin investments securities links pending'"
            )
        _echo_refresh_hint(
            "These closes value holdings",
            stale=bool(result.rows_written) and refreshed is None,
        )
    # A whole-source failure no longer aborts the run, so it has to be visible
    # here or the only trace is every one of that source's securities reporting
    # 'price_feed_error' with nothing saying what to fix. stderr because it is a
    # diagnostic about a degraded run, not part of the refresh's result.
    for failure in result.failed_sources:
        typer.echo(f"⚠️  {failure.source_type}: {failure.message}", err=True)
    if result.unpriced:
        render_rows(
            ["unpriced security", "reason"],
            [(entry.security_id, entry.reason) for entry in result.unpriced],
        )
    _report_refresh_failure(payload.refresh_error)


@app.command("set")
def investments_prices_set(
    security: str = typer.Argument(
        ..., help="Security reference: ticker, CUSIP, ISIN, name, or id"
    ),
    price_date: str = typer.Argument(
        ..., help="Date the price applies to (YYYY-MM-DD)"
    ),
    price: str = typer.Argument(..., help="Price of one unit, e.g. 42.50"),
    currency: str | None = typer.Option(
        None,
        "--currency",
        help=(
            "ISO-4217 quote currency. Defaults to the currency the position is "
            "held in; required when nothing is held or two denominations are in "
            "use."
        ),
    ),
    note: str | None = typer.Option(
        None, "--note", help="Why this price was set (e.g. a 409A valuation)"
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Rebuild the price models so the mark values holdings right away",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Record your own price for one security and date.

    A mark outranks every provider close for its own date and leaves other dates
    untouched. Writes app.security_price_overrides with an audit-log row; reverse
    it with 'investments prices delete', not by setting a zero.

    A non-positive price is refused: a worthless position is a ledger event — a
    disposal or write-off — not a zero price. Admitting zero would make
    "worthless" and "unknown" two states every downstream total must tell apart.

    Without --currency the mark takes the currency the position is held in. A
    mark only values a holding quoted in the same currency, so a fixed default
    would write successfully and value nothing for any non-USD position.

    The mark lands in app; holdings value from core. Pass --refresh to rebuild
    the models in the same command, or run 'moneybin refresh run' afterwards.
    """
    parsed_date = parse_cli_date(price_date, "DATE")
    parsed_price = parse_cli_decimal(price, "PRICE")
    with handle_cli_errors(
        cli_actor="investments_prices_set",
        payload_type=InvestmentPriceMarkPayload,
    ):
        from moneybin.orchestration import refresh as refresh_module  # noqa: PLC0415
        from moneybin.services.price_service import build_price_service  # noqa: PLC0415

        with get_database(read_only=False) as db:
            service = build_price_service(db, actor="investments_prices_set")
            security_id = service.resolve_security(security)
            quote_currency = service.resolve_quote_currency(security_id, currency)
            service.set_mark(
                security_id,
                parsed_date,
                parsed_price,
                quote_currency=quote_currency,
                note=note,
            )
            refreshed = (
                refresh_module.refresh(db, steps=["transform"]) if refresh else None
            )

    payload = InvestmentPriceMarkPayload(
        security_id=security_id,
        price_date=parsed_date,
        quote_currency=quote_currency,
        close=parsed_price,
        removed=False,
        refreshed=bool(refreshed and refreshed.applied),
        refresh_error=refreshed.error if refreshed else None,
    )
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=payload), output, cli_actor="investments_prices_set"
        )
        _report_refresh_failure(payload.refresh_error)
        return
    typer.echo(
        f"✅ Marked {security_id} at {parsed_price} {quote_currency} on {parsed_date}"
    )
    _echo_refresh_hint("This mark values holdings", stale=refreshed is None)
    _report_refresh_failure(payload.refresh_error)


@app.command("delete")
def investments_prices_delete(
    security: str = typer.Argument(
        ..., help="Security reference: ticker, CUSIP, ISIN, name, or id"
    ),
    price_date: str = typer.Argument(
        ..., help="Date of the mark to remove (YYYY-MM-DD)"
    ),
    currency: str | None = typer.Option(
        None,
        "--currency",
        help=(
            "ISO-4217 quote currency. Defaults to the currency the position is "
            "held in; required when nothing is held or two denominations are in "
            "use."
        ),
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh",
        help="Rebuild the price models so the removal values holdings right away",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Remove a price mark, returning that date to provider-derived valuation.

    Load-bearing rather than CRUD symmetry: an override outranks every provider
    row for its date, and 'set' can only change the number, so without this a
    mark is unreachable once written. Removing one is permanent — the audit log
    records it, but the previous value is not restored by re-running anything.

    The removal lands in app; holdings value from core. Pass --refresh to rebuild
    the models in the same command, or run 'moneybin refresh run' afterwards.
    """
    parsed_date = parse_cli_date(price_date, "DATE")
    with handle_cli_errors(
        cli_actor="investments_prices_delete",
        payload_type=InvestmentPriceMarkPayload,
    ):
        from moneybin.orchestration import refresh as refresh_module  # noqa: PLC0415
        from moneybin.services.price_service import build_price_service  # noqa: PLC0415

        with get_database(read_only=False) as db:
            service = build_price_service(db, actor="investments_prices_delete")
            security_id = service.resolve_security(security)
            quote_currency = service.resolve_quote_currency(security_id, currency)
            removed = service.delete_mark(
                security_id, parsed_date, quote_currency=quote_currency
            )
            # Gated on `removed`, unlike `pull --refresh`. A pull stays
            # unconditional because an earlier pull may have been left
            # unapplied; a delete that removed nothing cannot have stranded
            # anything, so an apply here would be pure cost.
            refreshed = (
                refresh_module.refresh(db, steps=["transform"])
                if refresh and removed
                else None
            )

    payload = InvestmentPriceMarkPayload(
        security_id=security_id,
        price_date=parsed_date,
        quote_currency=quote_currency,
        close=None,
        removed=removed,
        refreshed=bool(refreshed and refreshed.applied),
        refresh_error=refreshed.error if refreshed else None,
    )
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=payload), output, cli_actor="investments_prices_delete"
        )
        _report_refresh_failure(payload.refresh_error)
        return
    if removed:
        typer.echo(f"✅ Removed the mark on {security_id} for {parsed_date}")
    else:
        # Not an error: the end state the caller wanted already holds. Saying so
        # keeps "the override is gone" from reading as "your mark was deleted".
        typer.echo(f"No mark existed for {security_id} on {parsed_date}")
    _echo_refresh_hint(
        "This date returns to provider pricing",
        stale=removed and refreshed is None,
    )
    _report_refresh_failure(payload.refresh_error)


@app.command("list")
def investments_prices_list(
    security: str = typer.Argument(
        ..., help="Security reference: ticker, CUSIP, ISIN, name, or id"
    ),
    since: str | None = typer.Option(
        None, "--since", help="Only show prices from this ISO date forward"
    ),
    source: PriceSourceChoice | None = typer.Option(
        None,
        "--source",
        help="Filter by the source that supplied each close",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — list has no informational chatter; only data
) -> None:
    """Show the resolved price series for one security, newest first.

    Reads the resolved winner per date, not every observation that competed for
    it, so 'source' names which feed or mark actually supplied each close.
    """
    start = parse_cli_date(since, "--since") if since else None
    with handle_cli_errors(
        cli_actor="investments_prices_list",
        payload_type=InvestmentPricesPayload,
    ):
        from moneybin.services.price_service import build_price_service  # noqa: PLC0415

        with get_database(read_only=True) as db:
            service = build_price_service(db, actor="investments_prices_list")
            security_id = service.resolve_security(security)
            result = service.list_prices(
                security_id,
                since=start,
                # StrEnum members compare equal to their string values, but the
                # service takes ``str | None`` and binds it as a query parameter.
                source_type=source.value if source is not None else None,
            )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=InvestmentPricesPayload.from_result(result)),
            output,
            cli_actor="investments_prices_list",
        )
        return
    # A close is a per-unit price, not an amount: it is stored `DECIMAL(28, 10)`
    # and `format_money` rounds to two places, which renders a sub-cent crypto
    # price as 0.00. It declares no money column and prints as stored — the same
    # call `fx list` makes about a rate.
    if result.rows:
        render_rows(
            ["date", "close", "currency", "source", "basis"],
            [
                (
                    row.price_date,
                    row.close,
                    row.quote_currency,
                    row.source_type,
                    row.price_basis,
                )
                for row in result.rows
            ],
            numeric=("close",),
        )


@app.command("token")
def investments_prices_token(
    token: str | None = typer.Option(
        None,
        "--token",
        help="Tiingo API token. Omit to be prompted without echoing.",
    ),
) -> None:
    """Store the Tiingo API token in the OS keychain.

    CLI-only by the secret-material policy: an API credential must not travel
    through an LLM-mediated channel. Tiingo's free tier covers 1,000 requests/day
    and 33,000 mutual-fund NAVs; create a token at tiingo.com.

    Prefer the prompt over --token, which lands the credential in shell history.
    """
    value = token or typer.prompt("Tiingo API token", hide_input=True)
    if not value.strip():
        typer.echo("error: token must not be empty", err=True)
        raise typer.Exit(2)
    with handle_cli_errors(cli_actor="investments_prices_token"):
        from moneybin.secrets import (  # noqa: PLC0415  # keyring import is not cold-start cheap
            TIINGO_API_TOKEN_KEY,
            SecretStore,
        )

        SecretStore().set_key(TIINGO_API_TOKEN_KEY, value.strip())
    # Never echo the token back, not even masked — the value is now at rest.
    typer.echo("✅ Stored the Tiingo API token")
