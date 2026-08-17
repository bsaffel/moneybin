"""Adapters: convert refresh service results to MCP/CLI response envelopes.

Both ``refresh_run`` (MCP) and ``moneybin refresh`` (CLI ``--output json``)
present the same payload shape to agent consumers; this module owns the
mapping so the two surfaces cannot drift.
"""

from __future__ import annotations

from moneybin.errors import RecoveryAction
from moneybin.mcp.rematch_report import retired_transfers_action
from moneybin.privacy.payloads.system import RefreshRunPayload, SelfHealActionRow
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.refresh import RefreshResult, step_outcome
from moneybin.services.refresh_outcome import RefreshStepOutcome

REFRESH_APPLY_FAILED_HINT = (
    "SQLMesh apply failed — run `moneybin transform plan` to inspect, "
    "or refresh_run to retry."
)
REFRESH_CATEGORIZE_FOLLOWUP_HINT = (
    "Run refresh_run(steps=['categorize']) to apply rules/merchants "
    "to newly-matched rows."
)
REFRESH_ACCOUNT_LINKS_REVIEW_HINT = (
    'Review pending account identity proposals with reviews(kind="account_links").'
)
REFRESH_MERCHANT_LINKS_REVIEW_HINT = (
    'Review pending merchant identity proposals with reviews(kind="merchant_links").'
)


def _step_crash_recovery_actions(result: RefreshResult) -> list[RecoveryAction]:
    """The step retries for a refresh whose SQLMesh apply survived.

    Returns ``[]`` when the apply itself failed (``result.error``): that is the
    blocking failure, surfaced via the ``error`` field and the apply-failed
    ``actions`` hint. A best-effort step retry here would misdirect the agent to
    chase the secondary crash before the blocker.
    """
    if result.error is not None:
        return []
    return refresh_step_actions(step_outcome(result))


def refresh_step_actions(steps: RefreshStepOutcome | None) -> list[RecoveryAction]:
    """Build recovery actions for best-effort step crashes (match/categorize/rates).

    Ordered most-likely-correct first: the targeted retry(s), then a single
    diagnostic ``system_status`` doctor call. A recovery action must stay
    directly executable.

    Takes the flattened outcome rather than a ``RefreshResult`` so the commands
    that run a refresh *inside* something else — ``sync_pull``, ``import_files``,
    ``import_inbox_sync`` — offer the same retries from the same list. They are
    the callers that need it most: the CLI warns on stderr about a step that
    crashed mid-import, and MCP has no equivalent, so without this the crash
    reaches the agent as a payload string with nothing naming its cure.
    """
    if steps is None:
        return []
    actions: list[RecoveryAction] = []
    if steps.matching_error is not None:
        actions.append(
            RecoveryAction(
                tool="refresh_run",
                arguments={"steps": ["match"]},
                rationale=(
                    "Cross-source matching crashed mid-refresh; re-run just "
                    "the match step to retry."
                ),
                confidence="suggested",
                idempotent=True,
            )
        )
    if steps.categorization_error is not None:
        actions.append(
            RecoveryAction(
                tool="refresh_run",
                arguments={"steps": ["categorize"]},
                rationale=(
                    "Categorization crashed mid-refresh; re-run just the "
                    "categorize step to retry."
                ),
                confidence="suggested",
                idempotent=True,
            )
        )
    if steps.rate_backfill_error is not None or steps.rate_pairs_failed:
        # `pairs_failed` and a step crash, matching the CLI's `retryable_error`.
        # The other two lists name pairs a retry cannot fill: the provider
        # publishes no series at all for an unsupported one, and it *answered*
        # for a discarded one, so re-sending returns the identical unusable
        # value. Handing the agent an executable retry for either is a loop with
        # no terminating condition; their remedies rode their own warnings.
        # One branch, not two: both conditions select the same retry, and a run
        # that crashed after some pairs failed must not queue it twice.
        actions.append(
            RecoveryAction(
                tool="refresh_run",
                arguments={"steps": ["rates"]},
                rationale=(
                    "The rates step crashed, or the provider did not answer "
                    "for at least one currency pair; re-run just the rates "
                    "step to retry."
                ),
                confidence="suggested",
                idempotent=True,
            )
        )
    if actions:
        actions.append(
            RecoveryAction(
                tool="system_status",
                arguments={"sections": ["doctor"], "detail": "full"},
                rationale=(
                    "Run pipeline integrity checks to diagnose what the "
                    "partial refresh left inconsistent."
                ),
                confidence="suggested",
                idempotent=True,
            )
        )
    return actions


def refresh_envelope(
    result: RefreshResult, *, requested: frozenset[str]
) -> ResponseEnvelope[RefreshRunPayload]:
    """Build the standard response envelope for a refresh invocation.

    Args:
        result: Outcome of the service-layer ``refresh()`` call.
        requested: The frozenset of steps the caller asked for (the
            ``expand_steps(...)`` result). Used to decide whether the
            categorize follow-up hint applies.
    """
    actions: list[str] = []
    if not result.applied and result.error is not None:
        actions.append(REFRESH_APPLY_FAILED_HINT)
    # Gate the follow-up on success: when transform was requested but failed,
    # categorize would run against stale outputs — direct the agent to resolve
    # the apply failure first rather than chain categorize after it. Also gate
    # on matching_error being None: when the matcher crashed, recovery_actions
    # already says "retry match", so a "run categorize next" hint would be a
    # contradictory signal pointing the agent at the wrong next step.
    if (
        result.error is None
        and result.matching_error is None
        and "match" in requested
        and "categorize" not in requested
    ):
        actions.append(REFRESH_CATEGORIZE_FOLLOWUP_HINT)
    if result.error is None and "identity" in requested:
        if "accounts" not in result.identity_errors:
            actions.append(REFRESH_ACCOUNT_LINKS_REVIEW_HINT)
        if "merchants" not in result.identity_errors:
            actions.append(REFRESH_MERCHANT_LINKS_REVIEW_HINT)
    # Ahead of every hint above: those are routine next steps, this one says a
    # decision the user already made was undone. Shared with the embedded
    # refresh callers (import, sync pull, inbox drain) — a user who finds the
    # way back on one surface has to find it on the one they actually reach the
    # reconciliation through, and any of them can be that surface.
    if retired := retired_transfers_action(
        result.transfers_retired, operation="refresh"
    ):
        actions.insert(0, retired)
    recovery = _step_crash_recovery_actions(result)
    # `or None` (omit the key when empty) is correct here: refresh always uses
    # build_envelope, whose ResponseEnvelope.error is None, so there is no
    # error.recovery_actions to fall through to — the [] suppress vs None
    # fallthrough distinction (relevant only to build_error_envelope) doesn't
    # apply. Both the clean and apply-failed cases simply omit the key.
    return build_envelope(
        data=RefreshRunPayload(
            applied=result.applied,
            duration_seconds=result.duration_seconds,
            error=result.error,
            matching_error=result.matching_error,
            categorization_error=result.categorization_error,
            identity_errors=list(result.identity_errors),
            matches_auto_merged=result.matches_auto_merged,
            matches_pending_review=result.matches_pending_review,
            matches_pending_transfers=result.matches_pending_transfers,
            matching_skipped=result.matching_skipped,
            transfers_retired=result.transfers_retired,
            self_heal_actions=[
                SelfHealActionRow(
                    recipe_id=r.recipe_id,
                    rows_affected=r.rows_affected,
                    operation_id=r.operation_id,
                    timestamp=r.timestamp,
                )
                for r in result.self_heal_actions
            ],
            rates_written=(
                None
                if result.rate_backfill is None
                else result.rate_backfill.rates_written
            ),
            rate_pairs_failed=(
                []
                if result.rate_backfill is None
                else list(result.rate_backfill.pairs_failed)
            ),
            rate_pairs_unsupported=(
                []
                if result.rate_backfill is None
                else list(result.rate_backfill.pairs_unsupported)
            ),
            rate_pairs_discarded=(
                []
                if result.rate_backfill is None
                else list(result.rate_backfill.pairs_discarded)
            ),
            # Not gated on `rate_backfill is None`: a crash is exactly the case
            # where there is no backfill to read the answer off.
            rate_backfill_error=result.rate_backfill_error,
        ),
        sensitivity="low",
        actions=actions,
        recovery_actions=recovery or None,
    )
