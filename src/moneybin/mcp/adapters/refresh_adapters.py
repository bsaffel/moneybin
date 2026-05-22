"""Adapters: convert refresh service results to MCP/CLI response envelopes.

Both ``refresh_run`` (MCP) and ``moneybin refresh`` (CLI ``--output json``)
present the same payload shape to agent consumers; this module owns the
mapping so the two surfaces cannot drift.
"""

from __future__ import annotations

from dataclasses import asdict

from moneybin.errors import RecoveryAction
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.refresh import RefreshResult

REFRESH_APPLY_FAILED_HINT = (
    "SQLMesh apply failed — run `moneybin transform plan` to inspect, "
    "or refresh_run to retry."
)
REFRESH_CATEGORIZE_FOLLOWUP_HINT = (
    "Run refresh_run(steps=['categorize']) to apply rules/merchants "
    "to newly-matched rows."
)


def _step_crash_recovery_actions(result: RefreshResult) -> list[RecoveryAction]:
    """Build recovery actions for best-effort step crashes (matcher/categorizer).

    Ordered most-likely-correct first: the targeted retry(s), then a single
    diagnostic ``system_doctor`` call. ``system_doctor`` takes no MCP
    parameters, so ``arguments`` is empty — a recovery action must stay
    directly executable.

    Returns ``[]`` when the SQLMesh apply itself failed (``result.error``):
    that is the blocking failure, surfaced via the ``error`` field and the
    apply-failed ``actions`` hint. A best-effort step retry here would
    misdirect the agent to chase the secondary crash before the blocker.
    """
    if result.error is not None:
        return []
    actions: list[RecoveryAction] = []
    if result.matching_error is not None:
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
    if result.categorization_error is not None:
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
    if actions:
        actions.append(
            RecoveryAction(
                tool="system_doctor",
                arguments={},
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
) -> ResponseEnvelope:
    """Build the standard response envelope for a refresh invocation.

    Args:
        result: Outcome of the service-layer ``refresh()`` call.
        requested: The frozenset of steps the caller asked for (the
            ``expand_steps(...)`` result). Used to decide whether the
            categorize follow-up hint applies.
    """
    data: dict[str, object] = {
        "applied": result.applied,
        "duration_seconds": result.duration_seconds,
        # Always emit (empty until the self-heal safelist lands) so agents
        # see a stable key rather than a sometimes-present field.
        "self_heal_actions": [asdict(r) for r in result.self_heal_actions],
    }
    if result.error is not None:
        data["error"] = result.error
    if result.matching_error is not None:
        data["matching_error"] = result.matching_error
    if result.categorization_error is not None:
        data["categorization_error"] = result.categorization_error

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

    recovery = _step_crash_recovery_actions(result)
    return build_envelope(
        data=data,
        sensitivity="low",
        actions=actions,
        recovery_actions=recovery or None,
    )
