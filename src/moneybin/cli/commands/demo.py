"""`moneybin demo` — one-command evaluator preset (synthetic profile + answer)."""

import json
import logging

import typer

from moneybin.cli.output import OutputFormat, output_option

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
    profile: str = typer.Option("demo", "--profile", help="Target profile name"),
    seed: int | None = typer.Option(
        None, "--seed", min=1, max=9999, help="Deterministic seed (default: fixed)"
    ),
    years: int | None = typer.Option(None, "--years", help="Years of history"),
    yes: bool = typer.Option(
        False, "--yes", "-y", help="Auto-accept the reset if the demo profile exists"
    ),
    output: OutputFormat = output_option,
    quiet: bool = typer.Option(False, "-q", "--quiet", help="Suppress status lines"),
) -> None:
    """Populate a demo profile with synthetic data and show a first answer.

    Creates (or refreshes) an isolated ``demo`` profile, generates persona data,
    runs the full pipeline to a clean ``system doctor``, activates the profile,
    and prints net worth plus next steps. Nothing here touches real financial
    data — the demo profile only ever holds synthetic rows.
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.services.demo_service import DEMO_DEFAULT_SEED, DemoService

    if persona not in _PERSONAS:
        raise typer.BadParameter(f"persona must be one of {', '.join(_PERSONAS)}")

    svc = DemoService()
    resolved_seed = seed if seed is not None else DEMO_DEFAULT_SEED

    with handle_cli_errors(cli_actor="demo"):
        # Own the reset confirmation (magic stays visible) before mutating.
        reset_confirmed = yes
        if not yes and svc.profile_has_data(profile):
            reset_confirmed = typer.confirm(
                f"Profile {profile!r} already has demo data. Reset and regenerate?"
            )
            if not reset_confirmed:
                raise typer.Abort()

        result = svc.run(
            persona=persona,
            profile=profile,
            seed=resolved_seed,
            years=years,
            reset_confirmed=reset_confirmed,
        )

        if output == OutputFormat.JSON:
            typer.echo(
                json.dumps({
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
                })
            )
        else:
            if not quiet:
                typer.echo(
                    f"✅ Demo profile {result.profile!r} ready "
                    f"({result.account_count} accounts, "
                    f"{result.transaction_count} transactions).",
                    err=True,
                )
            # The one obvious answer (stdout — it's the data the user asked for).
            nw = result.net_worth
            typer.echo(f"Net worth: {'-' if nw < 0 else ''}${abs(nw):,.2f}")
            if not quiet:
                if result.doctor_failing == 0:
                    typer.echo("✅ system doctor clean", err=True)
                else:
                    typer.echo(
                        f"❌ system doctor: {result.doctor_failing} failing "
                        f"({', '.join(result.doctor_failing_names)})",
                        err=True,
                    )
                typer.echo(_NEXT_STEPS, err=True)

        # A demo that boots dirty is a real signal, not a warning to swallow.
        if result.doctor_failing > 0:
            raise typer.Exit(1)
