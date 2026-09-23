"""`moneybin demo` — one-command evaluator preset (synthetic profile + answer)."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

import typer

from moneybin.cli.output import (
    OutputFormat,
    currency_label,
    output_option,
    quiet_option,
)
from moneybin.cli.render import build_summary, compose_human_result, format_money

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from moneybin.services.demo_service import DemoResult

_PERSONAS = ("basic", "family", "freelancer", "international")


def _opt_str(value: Decimal | None) -> str | None:
    """Stringify a Decimal for JSON, preserving null rather than spelling it."""
    return None if value is None else str(value)


def _render_demo_receipt(result: DemoResult, *, quiet: bool) -> None:
    """Render the demo's saved dataset and first answer as one unpaged receipt."""
    from moneybin.cli.output import emit_human_result
    from moneybin.cli.utils import get_terminal_policy

    terminal = get_terminal_policy(no_pager=True)
    demo_result = result
    history = (
        f"{demo_result.start_date} through {demo_result.end_date}"
        if demo_result.start_date is not None and demo_result.end_date is not None
        else "Not available"
    )
    facts = [
        ("Profile", demo_result.profile),
        ("Persona", demo_result.persona),
        ("Seed", str(demo_result.seed)),
        ("History", history),
        ("Accounts saved", str(demo_result.account_count)),
        ("Transactions saved", str(demo_result.transaction_count)),
        ("Categorized", str(demo_result.categorized_count)),
    ]
    if demo_result.net_worth is not None:
        facts.append((
            "Net worth",
            format_money(demo_result.net_worth, "balance", minus=terminal.minus),
        ))
    else:
        facts.extend(
            (
                f"Net worth ({currency_label(segment.currency_code)})",
                format_money(segment.net_worth, "balance", minus=terminal.minus),
            )
            for segment in demo_result.per_currency
        )
    if demo_result.doctor_failing:
        doctor = (
            f"{demo_result.doctor_failing} failing: "
            f"{', '.join(demo_result.doctor_failing_names)}"
        )
    else:
        doctor = "Clean"
    facts.append(("Doctor", doctor))

    disclosures: list[str] = []
    if demo_result.net_worth is None:
        disclosures.append(
            "No combined net worth is shown because currencies were not converted."
        )
    if not quiet:
        if demo_result.doctor_failing == 0 and demo_result.previous_default:
            disclosures.append(
                "Default profile is now demo. Switch back with: "
                f"moneybin profile switch {demo_result.previous_default}"
            )
        disclosures.append(_NEXT_STEPS.strip())

    outcome = (
        "Demo profile needs attention"
        if demo_result.doctor_failing
        else "Demo profile ready"
    )
    emit_human_result(
        compose_human_result(
            [build_summary(facts, title=outcome)], disclosures=disclosures
        ),
        policy=terminal,
        finite_read=False,
        no_pager=True,
        receipt=True,
    )


_NEXT_STEPS = (
    "\nTry next:\n"
    "  moneybin reports spending-trend\n"
    "  moneybin reports cash-flow\n"
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
    from moneybin import error_codes
    from moneybin.cli.output import render_or_json
    from moneybin.cli.utils import get_terminal_policy, handle_cli_errors
    from moneybin.config import (
        clear_current_profile,
        get_current_profile,
        set_current_profile,
    )
    from moneybin.errors import UserError
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
    try:
        original_profile: str | None = get_current_profile(auto_resolve=False)
    except RuntimeError:
        original_profile = None

    try:
        with handle_cli_errors(cli_actor="demo"):
            # Own the rebuild confirmation (magic stays visible) before destroying it.
            reset_confirmed = yes
            if not yes and svc.profile_has_data():
                terminal = get_terminal_policy()
                if output == OutputFormat.JSON or not terminal.interactive:
                    raise UserError(
                        "Rebuilding the demo profile requires --yes outside an interactive terminal.",
                        code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
                    )
                reset_confirmed = typer.confirm(
                    f"Profile {DEMO_PROFILE!r} already has demo data. "
                    "Rebuild it and regenerate?",
                    default=False,
                    err=True,
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
            _render_demo_receipt(result, quiet=quiet)

        # A demo that boots dirty is a real signal, not a warning to swallow.
        if result.doctor_failing > 0:
            raise typer.Exit(1)
    finally:
        if original_profile is None:
            clear_current_profile()
        else:
            set_current_profile(original_profile)
