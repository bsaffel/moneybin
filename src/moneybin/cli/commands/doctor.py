"""doctor — pipeline integrity checks."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option
from moneybin.cli.utils import handle_cli_errors
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
) -> None:
    """Run pipeline integrity checks across all invariants.

    Checks that all fct_transactions resolve to known accounts, amounts
    are non-zero, transfer pairs balance, and categorization is healthy.
    Exits 0 when all invariants pass or warn; exits 1 when any fail.
    """
    with handle_cli_errors() as db:
        report = DoctorService(db).run_all(verbose=verbose)

    status_icon = {"pass": "✅", "fail": "❌", "warn": "⚠️ ", "skipped": "⏭️ "}

    failing = sum(1 for r in report.invariants if r.status == "fail")
    warning = sum(1 for r in report.invariants if r.status == "warn")
    passing = sum(1 for r in report.invariants if r.status == "pass")

    from moneybin.metrics.registry import (
        DOCTOR_RUNS_TOTAL,  # noqa: PLC0415 — defer import
    )

    DOCTOR_RUNS_TOTAL.labels(outcome="fail" if failing > 0 else "pass").inc()

    if output == OutputFormat.JSON:
        data = {
            "passing": passing,
            "failing": failing,
            "warning": warning,
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
            total_count=len(report.invariants),
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
        elif result.status == "fail" and not verbose:
            typer.echo("   Run with --verbose for affected IDs")

    n = len(report.invariants)
    summary = (
        f"\n{n} invariants checked across {report.transaction_count:,} transactions"
    )
    if failing:
        summary += f" — {failing} failing"
    else:
        summary += " — all passing"
    typer.echo(summary)

    if failing > 0:
        raise typer.Exit(1)
