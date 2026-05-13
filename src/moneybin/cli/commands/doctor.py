"""doctor — pipeline integrity checks."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.protocol.envelope import build_envelope
from moneybin.services.doctor_service import DoctorService

logger = logging.getLogger(__name__)

verbose_option: bool = typer.Option(
    False,
    "--verbose",
    "-V",
    help="Show affected transaction IDs for each failing invariant.",
)


def doctor_command(
    verbose: bool = verbose_option,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Run pipeline integrity checks across all invariants.

    Checks that all fct_transactions resolve to known accounts, amounts
    are non-zero, transfer pairs balance, and categorization is healthy.
    Exits 0 when all invariants pass or warn; exits 1 when any fail.
    """
    with handle_cli_errors():
        with get_database() as db:
            report = DoctorService(db).run_all(verbose=verbose)

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
                }
                for r in report.invariants
            ],
        }
        actions: list[str] = []
        if failing > 0:
            actions.append("Run with --verbose to see affected transaction IDs")
        envelope = build_envelope(
            data=data,
            sensitivity="low",
            actions=actions,
        )
        typer.echo(envelope.to_json())
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
