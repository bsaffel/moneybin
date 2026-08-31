"""doctor — pipeline integrity checks."""

from __future__ import annotations

import dataclasses
import logging

import typer

from moneybin import error_codes
from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.protocol.envelope import build_envelope, build_error_envelope
from moneybin.services.doctor_service import DoctorService

logger = logging.getLogger(__name__)

verbose_option: bool = typer.Option(
    False,
    "--verbose",
    "-V",
    help=(
        "Show every invariant that ran, not just the ones that need attention, "
        "plus the affected transaction IDs for each failing one."
    ),
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
        # read_only=False matches the MCP system_doctor tool: DoctorService
        # initializes a SQLMesh Context which may write internal state tables
        # on first init; a read-only connection silently marks SQLMesh audits
        # as unavailable.
        with get_database(read_only=False) as db:
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
        if failing > 0:
            envelope = build_error_envelope(
                error=UserError(
                    f"{failing} invariant(s) failing",
                    code=error_codes.AUDIT_INVARIANT_FAILURE,
                ),
                actions=base.actions,
            )
            # Unlike a typical error envelope, doctor's payload IS the diagnosis
            # — the per-invariant results and their recovery actions are what the
            # caller ran the command for. build_error_envelope zeroes `data` by
            # contract, so restore it here. `dataclasses.replace` rather than
            # assignment, for the same reason mark_total_failure uses it: fields
            # derived in __post_init__ only settle when the envelope is rebuilt.
            envelope = dataclasses.replace(envelope, data=data, summary=base.summary)
        else:
            envelope = base
        render_or_json(envelope, output, cli_actor="doctor_command")
        if failing > 0:
            raise typer.Exit(1)
        return

    for result in report.invariants:
        # Requirement 20: a passing invariant is not news, and five ✅ lines are
        # five a reader has to rule out before finding the one ❌ among them.
        # Suppressed by status rather than by "not failing": a warn or a skip is
        # not a pass, the summary counts them without naming them, and hiding
        # one would leave a reader knowing something is off and unable to see
        # what. `--verbose` restores the whole roll (requirement 21).
        if result.status == "pass" and not verbose:
            continue
        icon = status_icon.get(result.status, "?")
        line = f"{icon} {result.name}"
        if result.detail:
            line += f" — {result.detail}"
        typer.echo(line)
        if verbose and result.affected_ids:
            typer.echo(f"   Affected: {', '.join(result.affected_ids)}")
        # Recovery actions carry no --verbose gate. This is asymmetric with
        # affected_ids on purpose: raw IDs are debug-only (operator inspecting
        # the failure), but the actions are the agent's next-step contract —
        # they need to be visible on a plain `moneybin system doctor` call too,
        # since the CLI is a first-class agent surface (AGENTS.md). The 5-action
        # cap below keeps that output bounded when one invariant flags many
        # orphans.
        #
        # They are the one thing `-q` does silence, which is the same line
        # `echo_report_notes` draws: quiet reaches next-step hints and nothing
        # else. A 💡 suggests a command to run next; the invariant above it and
        # the summary below it are the answer, and a flag asking for less
        # chatter is not a claim that anything stopped being wrong.
        recovery = [] if quiet else (result.recovery_actions or [])
        max_actions_rendered = 5
        for action in recovery[:max_actions_rendered]:
            # Render arguments as Python kwargs (key=repr(value)) so an agent
            # reading this line can paste it directly into a follow-up call.
            # `dict.__repr__` would produce single-quoted Python-literal syntax
            # that's neither valid JSON nor valid kwargs.
            kwargs = ", ".join(f"{k}={v!r}" for k, v in action.arguments.items())
            typer.echo(
                f"   💡 [{action.confidence}] {action.tool}({kwargs}) "
                f"— {action.rationale}"
            )
        remaining = len(recovery) - max_actions_rendered
        if remaining > 0:
            typer.echo(
                f"   … and {remaining} more recovery action(s) "
                "(use --output json for the full list)"
            )

    # Ungated by `quiet`, unlike most summary lines: once requirement 20 stops
    # narrating a passing invariant, this is the only thing a clean run prints,
    # and `-q` would otherwise make `moneybin system doctor` succeed in total
    # silence — a command whose entire job is to report on the ledger saying
    # nothing about it. It is doctor's result, not a status line about
    # producing one.
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
