"""doctor — pipeline integrity checks."""

from __future__ import annotations

import dataclasses
import logging
from typing import Any, cast

import typer

from moneybin import error_codes
from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import (
    generated_cli_command,
    get_terminal_policy,
    handle_cli_errors,
)
from moneybin.database import get_database
from moneybin.errors import RecoveryAction, UserError
from moneybin.protocol.envelope import build_envelope, build_error_envelope
from moneybin.services.doctor_service import DoctorService

logger = logging.getLogger(__name__)


#: Maps the five MCP tool names doctor's recipes emit (`src/moneybin/audits/
#: recipes/`) to the CLI argv that performs the same repair, given that
#: action's own ``arguments``. Returns ``None`` when the arguments name
#: nothing the CLI can express (e.g. clearing a transaction's tags to an
#: exact empty set has no CLI primitive) — the rationale alone still prints.
def _recovery_command(action: RecoveryAction) -> tuple[str, ...] | None:
    args = action.arguments
    if action.tool == "refresh_run":
        return ("refresh",)
    if action.tool == "system_status":
        if args.get("sections") == ["doctor"]:
            full = args.get("detail") == "full"
            return ("system", "doctor", "--full") if full else ("system", "doctor")
        return ("system", "status")
    if action.tool == "transactions_categorize_run":
        methods = args.get("methods")
        if methods:
            return (
                "transactions",
                "categorize",
                "run",
                "--methods",
                ",".join(methods),
            )
        return ("transactions", "categorize", "run")
    if action.tool == "import_revert":
        # `import_id` is never known at recovery-construction time (the
        # rationale tells the agent to read it from import_status); the
        # placeholder keeps the command runnable-with-substitution.
        return ("import", "revert", str(args.get("import_id") or "<import_id>"))
    if action.tool == "transactions_annotate":
        requests = cast("list[dict[str, Any]]", args.get("requests") or [])
        if len(requests) != 1:
            return None
        request = requests[0]
        if request.get("kind") == "note_delete" and request.get("note_id"):
            return ("transactions", "notes", "delete", str(request["note_id"]))
        # `tags_set` has no CLI equivalent when it clears to an exact set —
        # `transactions tags remove` takes the tags to remove, not the tags
        # a transaction should end up without.
        return None
    return None


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
    quiet: bool = quiet_option,  # diagnostics and required recovery are results
) -> None:
    """Run pipeline integrity checks across all invariants.

    Checks that all fct_transactions resolve to known accounts, amounts
    are non-zero, transfer pairs balance, categorization is healthy, and every
    recent protected app.* mutation has a paired audit row. Exits 0 when all
    invariants pass or warn; exits 1 when any fail.
    """
    with handle_cli_errors(cli_actor="doctor_command"):
        # read_only=False matches the MCP system_doctor tool: DoctorService
        # initializes a SQLMesh Context which may write internal state tables
        # on first init; a read-only connection silently marks SQLMesh audits
        # as unavailable.
        with get_database(read_only=False) as db:
            report = DoctorService(db).run_all(verbose=verbose, full=full)

    symbols = get_terminal_policy().symbols
    status_icon = {
        "pass": symbols.success,
        "fail": symbols.failure,
        "warn": symbols.attention,
        "skipped": symbols.attention,
    }

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

    lines: list[str] = []
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
        lines.append(line)
        if result.status != "pass" and result.affected_ids:
            lines.append(f"   Affected: {', '.join(result.affected_ids)}")
        # Affected IDs and recovery actions remain visible for non-pass checks;
        # the five-action cap bounds guidance for checks with many affected rows.
        # Quiet preserves recovery guidance needed to act on a failed check.
        recovery = result.recovery_actions or []
        for action in recovery:
            rationale = action.rationale
            if action.confidence == "suggested":
                rationale = f"Consider {rationale}"
            command = _recovery_command(action)
            if command is None:
                lines.append(f"   {symbols.action} {rationale}")
            else:
                lines.append(
                    f"   {symbols.action} {rationale}: "
                    f"{generated_cli_command(*command)}"
                )

    # Ungated by `quiet`, unlike most summary lines: once requirement 20 stops
    # narrating a passing invariant, this is the only thing a clean run prints,
    # and `-q` would otherwise make `moneybin system doctor` succeed in total
    # silence — a command whose entire job is to report on the ledger saying
    # nothing about it. It is doctor's result, not a status line about
    # producing one.
    n = len(report.invariants)
    # A leading blank line separates the summary from the invariant lines above
    # it — but on an all-pass, non-verbose run `lines` is still empty here, and
    # the blank line would be the first thing the command prints (rule 1).
    lead = "\n" if lines else ""
    summary = (
        f"{lead}{n} invariants checked across {report.transaction_count:,} transactions"
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
    lines.append(summary)
    emit_human_result(
        "\n".join(lines),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )

    if failing > 0:
        raise typer.Exit(1)
