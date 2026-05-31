"""doctor — pipeline integrity checks."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.doctor_service import DoctorService

logger = logging.getLogger(__name__)

verbose_option: bool = typer.Option(
    False,
    "--verbose",
    "-V",
    help="Show affected transaction IDs for each failing invariant.",
)

full_option: bool = typer.Option(
    False,
    "--full",
    help=(
        "Scan every protected app.* row for audit coverage instead of the "
        "sampled, recent-rows-only default."
    ),
)


def doctor_command(
    verbose: bool = verbose_option,
    full: bool = full_option,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Run pipeline integrity checks across all invariants.

    Checks that all fct_transactions resolve to known accounts, amounts
    are non-zero, transfer pairs balance, categorization is healthy, and every
    recent protected app.* mutation has a paired audit row. Exits 0 when all
    invariants pass or warn; exits 1 when any fail.
    """
    with handle_cli_errors():
        with get_database() as db:
            report = DoctorService(db).run_all(verbose=verbose, full=full)

    status_icon = {"pass": "✅", "fail": "❌", "warn": "⚠️ ", "skipped": "⏭️ "}

    failing = report.failing
    warning = report.warning
    passing = report.passing
    skipped = report.skipped

    if output == OutputFormat.JSON:
        data = {
            "passing": passing,
            "failing": failing,
            "warning": warning,
            "skipped": skipped,
            "transaction_count": report.transaction_count,
            "invariants": [
                {
                    "name": r.name,
                    "status": r.status,
                    "detail": r.detail,
                    "affected_ids": r.affected_ids,
                    "recovery_actions": [
                        a.model_dump() for a in (r.recovery_actions or [])
                    ],
                }
                for r in report.invariants
            ],
        }
        actions: list[str] = []
        if failing > 0:
            actions.append("Run with --verbose to see affected transaction IDs")
        base = build_envelope(data=data, sensitivity="low", actions=actions)
        envelope = (
            ResponseEnvelope(
                summary=base.summary,
                data=data,
                actions=base.actions,
                error=UserError(
                    f"{failing} invariant(s) failing",
                    code="invariant_failure",
                ),
            )
            if failing > 0
            else base
        )
        render_or_json(envelope, output, cli_actor="doctor_command")
        if failing > 0:
            raise typer.Exit(1)
        return

    for result in report.invariants:
        icon = status_icon.get(result.status, "?")
        line = f"{icon} {result.name}"
        if result.detail:
            line += f" — {result.detail}"
        typer.echo(line)
        if verbose and result.affected_ids:
            typer.echo(f"   Affected: {', '.join(result.affected_ids)}")
        for action in result.recovery_actions or []:
            # Render arguments as Python kwargs (key=repr(value)) so an agent
            # reading this line can paste it directly into a follow-up call.
            # `dict.__repr__` would produce single-quoted Python-literal syntax
            # that's neither valid JSON nor valid kwargs.
            kwargs = ", ".join(f"{k}={v!r}" for k, v in action.arguments.items())
            typer.echo(
                f"   💡 [{action.confidence}] {action.tool}({kwargs}) "
                f"— {action.rationale}"
            )

    if not quiet:
        n = len(report.invariants)
        summary = (
            f"\n{n} invariants checked across {report.transaction_count:,} transactions"
        )
        if failing:
            summary += f" — {failing} failing"
            if warning or skipped:
                summary += f" ({warning} warn, {skipped} skipped)"
            if not verbose:
                summary += " — run --verbose for affected IDs"
        elif warning or skipped:
            summary += f" — {passing} passing, {warning} warn, {skipped} skipped"
        else:
            summary += " — all passing"
        typer.echo(summary)

    if failing > 0:
        raise typer.Exit(1)
