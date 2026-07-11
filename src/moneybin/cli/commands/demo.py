"""`moneybin demo` — one-command evaluator preset (synthetic profile + answer)."""

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option

logger = logging.getLogger(__name__)

_PERSONAS = ("basic", "family", "freelancer")

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
    years: int | None = typer.Option(None, "--years", help="Years of history"),
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
                        "doctor_failing": result.doctor_failing,
                        "doctor_failing_names": result.doctor_failing_names,
                        "net_worth": str(result.net_worth),
                        "total_assets": str(result.total_assets),
                        "total_liabilities": str(result.total_liabilities),
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
                    f"{result.transaction_count} transactions).",
                    err=True,
                )
            # The one obvious answer (stdout). Bare Decimal matches the sibling
            # `reports networth` command's convention (coherence).
            typer.echo(f"Net worth: {result.net_worth}")
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
