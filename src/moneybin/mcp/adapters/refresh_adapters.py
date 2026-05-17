"""Adapters: convert refresh service results to MCP/CLI response envelopes.

Both ``refresh_run`` (MCP) and ``moneybin refresh`` (CLI ``--output json``)
present the same payload shape to agent consumers; this module owns the
mapping so the two surfaces cannot drift.
"""

from __future__ import annotations

from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.refresh import RefreshResult

REFRESH_APPLY_FAILED_HINT = (
    "SQLMesh apply failed — call transform_plan to inspect, or refresh_run to retry."
)
REFRESH_CATEGORIZE_FOLLOWUP_HINT = (
    "Run refresh_run(steps=['categorize']) to apply rules/merchants "
    "to newly-matched rows."
)


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
    }
    if result.error is not None:
        data["error"] = result.error

    actions: list[str] = []
    if not result.applied and result.error is not None:
        actions.append(REFRESH_APPLY_FAILED_HINT)
    # Gate the follow-up on success: when transform was requested but failed,
    # categorize would run against stale outputs — direct the agent to resolve
    # the apply failure first rather than chain categorize after it.
    if result.error is None and "match" in requested and "categorize" not in requested:
        actions.append(REFRESH_CATEGORIZE_FOLLOWUP_HINT)
    return build_envelope(data=data, sensitivity="low", actions=actions)
