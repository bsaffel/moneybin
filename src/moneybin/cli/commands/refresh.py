"""Refresh command for MoneyBin CLI.

CLI peer of the ``refresh_run`` MCP tool. Runs the post-load refresh
pipeline (gsheet pull → matching → SQLMesh apply → categorization → identity
backfill → exchange-rate gather) via ``moneybin.orchestration.refresh.refresh()``.
Idempotent — safe to retry after a failure.
"""

import logging
from enum import StrEnum

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.render import render_note
from moneybin.cli.utils import (
    handle_cli_errors,
    warn_refresh_steps,
    warn_transfers_retired,
)
from moneybin.matching.reconciliation import RETIRED_SIDES_COLLAPSED
from moneybin.services.refresh_outcome import StageOutcome, best_effort

logger = logging.getLogger(__name__)

# One phrase per step, in that step's own vocabulary. A shared template would
# have to call every step's numbers the same thing, and they are not the same
# thing — "12 bound" and "12 rows" answer different questions.
_STAGE_LABELS: dict[str, str] = {
    "gsheet": "Sheets",
    "match": "Matching",
    "transform": "Transforms",
    "categorize": "Categorization",
    "identity": "Identity",
    "rates": "Rates",
}


def _stage_summary(stage: StageOutcome) -> str:
    """One line naming a stage and what it observably did.

    A stage that was reached and declined says so rather than reporting zeros:
    "nothing found" and "nothing examined" send the user to different remedies,
    and only the second is worth re-running.
    """
    label = _STAGE_LABELS.get(stage.step, stage.step)
    if stage.error is not None:
        return f"  {label}: failed"
    if not stage.ran:
        return f"  {label}: skipped (nothing examined)"
    counts = stage.counts
    if stage.step == "gsheet":
        detail = f"{counts['completed']} pulled, {counts['rows']} rows"
        if counts["non_complete"]:
            detail += f", {counts['non_complete']} incomplete"
        return f"  {label}: {detail}"
    if stage.step == "match":
        return (
            f"  {label}: {counts['auto_merged']} merged, "
            f"{counts['pending_review']} to review, "
            f"{counts['pending_transfers']} transfers to review"
        )
    if stage.step == "transform":
        return f"  {label}: rebuilt"
    if stage.step == "categorize":
        return (
            f"  {label}: {counts['total']} categorized "
            f"({counts['merchant']} merchant, {counts['rule']} rule, "
            f"{counts['plaid']} provider)"
        )
    if stage.step == "identity":
        # Each domain reports only if it ran: one can fail while the other
        # succeeds, and a missing key means that domain never answered.
        parts: list[str] = []
        if "accounts_linked" in counts:
            parts.append(f"{counts['accounts_linked']} accounts linked")
        if "merchants_bound" in counts:
            parts.append(f"{counts['merchants_bound']} merchants bound")
        return f"  {label}: {', '.join(parts) if parts else 'no domains reported'}"
    if stage.step == "rates":
        return f"  {label}: {counts['rates_written']} written"
    return f"  {label}: ran"


class RefreshStepChoice(StrEnum):
    """User-selectable subset of ``orchestration.refresh.RefreshStep`` for Typer.

    Rejecting invalid step names at parse time surfaces a usage error
    (exit code 2) rather than a runtime UserError (exit code 1). The
    service-layer ``UNKNOWN_REFRESH_STEP`` check remains as
    defense-in-depth for programmatic callers.

    ``gsheet`` is intentionally omitted: the full ``refresh`` cascade
    auto-pulls connected sheets, and the user-facing CLI path to pull one
    on demand is the dedicated ``moneybin gsheet pull`` command — so a
    ``--step gsheet`` flag would be redundant. The capability stays
    reachable on the CLI (functional parity); only the spelling differs
    from MCP's ``refresh_run(steps=["gsheet"])``.
    """

    MATCH = "match"
    TRANSFORM = "transform"
    CATEGORIZE = "categorize"
    IDENTITY = "identity"
    RATES = "rates"


def refresh_command(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    step: list[RefreshStepChoice] = typer.Option(
        None,
        "--step",
        help=(
            "Limit the cascade to one or more steps "
            "(repeatable; choose from match, transform, categorize, identity, "
            "rates). Default: full cascade. Steps always run in canonical order "
            "(match → transform → categorize → identity → rates) regardless of "
            "flag order."
        ),
    ),
) -> None:
    """Run refresh: matching, apply, categorization, identity, exchange rates.

    Single user-facing entry point for refreshing derived state from raw
    inputs. Idempotent. Matching, categorization and rates are best-effort: a
    real crash in any of them is surfaced (a ⚠️ warning here, that step's own
    `error` inside `stages` plus `recovery_actions` under `--output json`) but
    does not fail the command. Identity failures expose
    only their domain in `identity_errors`. The rates step gathers the exchange
    rates this profile's own transactions, balances and holdings imply, so
    reports can convert without reaching the network; a pair the provider could
    not answer is reported and retried next run. Only a SQLMesh apply error
    exits non-zero.
    """
    from moneybin.adapters.refresh_adapters import (  # noqa: PLC0415
        refresh_envelope,
    )
    from moneybin.cli.output import render_or_json  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.orchestration.refresh import (  # noqa: PLC0415
        expand_steps,
        refresh,
        step_outcome,
    )

    # StrEnum members compare equal to their string values, so downstream
    # service code that accepts ``list[str]`` works unchanged.
    steps: list[str] | None = [s.value for s in step] if step else None

    with (
        handle_cli_errors(cli_actor="refresh_command"),
        get_database(read_only=False, operation_type="transform_apply") as db,
    ):
        result = refresh(db, steps=steps)
    requested = expand_steps(steps)

    # Best-effort step crashes (matcher/categorizer) don't fail the command,
    # but they are warnings (diagnostics → stderr), not informational output.
    # Emit them regardless of output format and regardless of --quiet (per
    # cli.md, -q suppresses status/✅, not warnings; JSON data still goes
    # cleanly to stdout) so a partial-pipeline failure is never silent. In
    # JSON mode the crash is also in the payload (that step's `stages` entry
    # plus recovery_actions); the stderr warning is the human/operator signal.
    steps_outcome = step_outcome(result)
    # Sits with the crash warnings, not with the ✅ status line, for the same
    # reason: this is a decision the *user* made being undone, so it survives
    # --quiet and is emitted under --output json too (where the count is also in
    # the payload). Every refresh reaches the reconciliation through the match
    # step, so an ordinary `moneybin refresh` after an import can hit it.
    warn_transfers_retired(result.transfers_retired, cause=RETIRED_SIDES_COLLAPSED)
    warn_refresh_steps(steps_outcome)
    # Retrying is the right advice for everything here except an unsupported
    # pair, which no number of refreshes will fill — so it suppresses the ✅
    # without earning the "re-run the failed step" hint below, whose own remedy
    # already rode the warning above.
    retryable_error = (
        any(stage.error is not None for stage in best_effort(steps_outcome.stages))
        or bool(steps_outcome.identity_errors)
        or bool(steps_outcome.rate_pairs_failed)
    )
    # The rates step is best-effort like the three above, so an unfilled pair
    # suppresses the ✅ for the same reason they do: a success banner printed
    # directly beneath the warning above contradicts it. A discarded rate joins
    # them without joining `retryable_error`: the provider answered, so the same
    # request returns the same unusable value however many times it is re-sent.
    has_step_error = retryable_error or bool(
        steps_outcome.rate_pairs_unsupported or steps_outcome.rate_pairs_discarded
    )

    if output == OutputFormat.JSON:
        render_or_json(
            refresh_envelope(result, requested=requested),
            output,
            cli_actor="refresh_command",
        )
        if result.error is not None:
            raise typer.Exit(1)
        return

    if quiet:
        if result.error is not None:
            raise typer.Exit(1)
        return

    # Requirement 18: one note per stage, before the summary line that closes
    # the run. Sited after the `-q` return above rather than passing
    # quiet=True, so there is one place that decides a status line is
    # suppressed. A stage the caller never requested is absent from the tuple,
    # so a narrowed `--step` run prints only what it actually ran.
    if result.stages:
        render_note("Pipeline:")
        for stage in result.stages:
            render_note(_stage_summary(stage))

    # Suppress the step-retry hint when apply also failed: the apply error is
    # the blocker (reported by ❌ below), so "re-run the failed step" would
    # misdirect the agent before it resolves the blocking failure.
    if retryable_error and result.error is None:
        logger.info(
            "💡 Re-run the failed step (e.g. `moneybin refresh --step match`) "
            "or run `moneybin system doctor` to diagnose."
        )

    if result.applied:
        duration = result.duration_seconds or 0.0
        # No ✅ when a best-effort step crashed — the warning above already
        # told the truth, and a success banner would contradict it.
        if has_step_error:
            logger.info(
                f"Refresh complete in {duration:.2f}s (best-effort step failures above)"
            )
        else:
            logger.info(f"✅ Refresh complete in {duration:.2f}s")
        return
    if result.error is not None:
        logger.error(f"❌ Refresh failed: {result.error}")
        raise typer.Exit(1)
    steps_str = ", ".join(sorted(requested))
    if has_step_error:
        logger.info(
            f"Partial refresh complete (steps: {steps_str}; best-effort failures above)"
        )
    else:
        logger.info(f"✅ Partial refresh complete (steps: {steps_str})")
