"""`moneybin demo` — one-command evaluator preset (synthetic profile + answer)."""

import logging
from decimal import Decimal

import typer

from moneybin.cli.output import (
    OutputFormat,
    currency_label,
    output_option,
    quiet_option,
)
from moneybin.cli.render import format_money, render_summary

logger = logging.getLogger(__name__)

_PERSONAS = ("basic", "family", "freelancer", "international")


def _opt_str(value: Decimal | None) -> str | None:
    """Stringify a Decimal for JSON, preserving null rather than spelling it."""
    return None if value is None else str(value)


_NEXT_STEPS = (
    "\nTry next:\n"
    "  moneybin reports spending\n"
    "  moneybin reports cashflow\n"
    "  moneybin review\n"
    "Or ask your AI assistant (MCP):\n"
    '  "What did I spend on dining last month?"\n'
    '  "Show my net-worth trend."'
)


def demo_command(
    persona: str = typer.Option(
        "basic", "--persona", help=f"Data shape: one of {', '.join(_PERSONAS)}"
    ),
    seed: int | None = typer.Option(
        None, "--seed", min=1, max=9999, help="Deterministic seed (default: fixed)"
    ),
    years: int | None = typer.Option(
        None,
        "--years",
        min=1,
        max=10,
        help="Years of history (default: the persona's own)",
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Auto-accept the rebuild if the demo profile exists"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Set up the demo profile with synthetic data and show a first answer.

    Always targets the dedicated ``demo`` profile — it can never be pointed at a
    real financial profile. Re-running rebuilds that profile's database from
    scratch and regenerates. For a differently-named synthetic sandbox, use
    ``moneybin synthetic generate``.
    """
    from moneybin.cli.output import render_or_json
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.demo_service import (
        DEMO_DEFAULT_SEED,
        DEMO_PROFILE,
        DemoService,
    )

    if persona not in _PERSONAS:
        raise typer.BadParameter(f"persona must be one of {', '.join(_PERSONAS)}")

    svc = DemoService()
    resolved_seed = seed if seed is not None else DEMO_DEFAULT_SEED

    with handle_cli_errors(cli_actor="demo"):
        # Own the rebuild confirmation (magic stays visible) before destroying it.
        reset_confirmed = yes
        if not yes and svc.profile_has_data():
            reset_confirmed = typer.confirm(
                f"Profile {DEMO_PROFILE!r} already has demo data. "
                f"Rebuild it and regenerate?"
            )
            if not reset_confirmed:
                raise typer.Abort()

        result = svc.run(
            persona=persona,
            seed=resolved_seed,
            years=years,
            reset_confirmed=reset_confirmed,
        )

        if output == OutputFormat.JSON:
            render_or_json(
                build_envelope(
                    data={
                        "profile": result.profile,
                        "persona": result.persona,
                        "seed": result.seed,
                        "account_count": result.account_count,
                        "transaction_count": result.transaction_count,
                        "categorized_count": result.categorized_count,
                        "doctor_failing": result.doctor_failing,
                        "doctor_failing_names": result.doctor_failing_names,
                        # null, never "None": a multi-currency profile has no
                        # single total, and a consumer must be able to tell that
                        # from a string that happens to spell it.
                        "net_worth": _opt_str(result.net_worth),
                        "total_assets": _opt_str(result.total_assets),
                        "total_liabilities": _opt_str(result.total_liabilities),
                        "per_currency": [
                            {
                                "currency_code": segment.currency_code,
                                "net_worth": _opt_str(segment.net_worth),
                                "total_assets": _opt_str(segment.total_assets),
                                "total_liabilities": _opt_str(
                                    segment.total_liabilities
                                ),
                                "account_count": segment.account_count,
                            }
                            for segment in result.per_currency
                        ],
                        "previous_default_profile": result.previous_default,
                    },
                    sensitivity="low",
                ),
                output,
                cli_actor="demo",
            )
        else:
            if not quiet:
                typer.echo(
                    f"✅ Demo profile {result.profile!r} ready "
                    f"({result.account_count} accounts, "
                    f"{result.transaction_count} transactions, "
                    f"{result.categorized_count} categorized).",
                    err=True,
                )
            # The one obvious answer (stdout), through the same renderer and
            # money formatter the sibling `reports networth` command uses
            # (coherence). Holding more than one currency, there is no one
            # answer to give — print each currency's own rather than a total
            # that would mean nothing.
            if result.net_worth is not None:
                render_summary([
                    ("Net worth", format_money(result.net_worth, "balance"))
                ])
            else:
                # Both fields are nullable: reports.net_worth pools every
                # account whose currency is unknown into one NULL-coded
                # segment. `currency_label` names that slot; `format_money`
                # spells the absent amount `-`, which is the token this CLI
                # already prints for a missing figure — `UNKNOWN_CURRENCY`
                # belongs to the currency slot it is named for.
                render_summary(
                    [
                        (
                            currency_label(segment.currency_code),
                            format_money(segment.net_worth, "balance"),
                        )
                        for segment in result.per_currency
                    ],
                    title="Net worth by currency:",
                )
            if not quiet:
                if result.doctor_failing == 0:
                    typer.echo("✅ system doctor clean", err=True)
                else:
                    typer.echo(
                        f"❌ system doctor: {result.doctor_failing} failing "
                        f"({', '.join(result.doctor_failing_names)})",
                        err=True,
                    )
                # Demo repoints every later command at itself. Say so, and name the
                # way back — a silent default switch is magic that must stay visible.
                # A failing doctor is a failed run, and the service leaves the
                # default alone in that case, so say nothing.
                if result.doctor_failing == 0:
                    switch_back = (
                        f" Switch back with: moneybin profile switch "
                        f"{result.previous_default}"
                        if result.previous_default
                        else ""
                    )
                    typer.echo(
                        f"⚙️  Default profile is now {result.profile!r}.{switch_back}",
                        err=True,
                    )
                typer.echo(_NEXT_STEPS, err=True)

        # A demo that boots dirty is a real signal, not a warning to swallow.
        if result.doctor_failing > 0:
            raise typer.Exit(1)
