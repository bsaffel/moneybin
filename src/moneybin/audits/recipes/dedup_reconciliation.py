"""Recipe for the ``dedup_reconciliation`` audit (fail).

A staging-vs-core row-count mismatch is fixed by rebuilding ``core`` from
the current staging layer: re-run matching to apply any pending dedup
decisions, *then* re-run transform to rebuild ``core.fct_transactions``
against the updated decisions. Match alone updates ``app.match_decisions``
and ``prep.*`` views but does NOT propagate into ``core``, so a match-only
refresh leaves the audit symptom unchanged. The recipe therefore emits the
full default refresh cascade (no ``steps=`` arg).
"""

from __future__ import annotations

from moneybin.audits.recipes.registry import RecipeContext
from moneybin.errors import RecoveryAction


def recipe(
    affected_ids: list[str],  # noqa: ARG001 — dedup audit doesn't carry per-row ids
    context: RecipeContext,  # noqa: ARG001 — pure recipe
) -> list[RecoveryAction]:
    """Emit a full refresh followed by the standard doctor status section."""
    return [
        RecoveryAction(
            tool="refresh_run",
            arguments={},
            rationale=(
                "Run the full refresh cascade (match + transform + categorize) "
                "to apply any pending dedup decisions and rebuild "
                "core.fct_transactions — match alone updates app.match_decisions "
                "but doesn't propagate into core."
            ),
            confidence="suggested",
            idempotent=True,
        ),
        RecoveryAction(
            tool="system_status",
            arguments={"sections": ["doctor"], "detail": "full"},
            rationale=(
                "Re-run all invariants in deep-scan mode to surface the "
                "specific raw/core count delta that caused the mismatch."
            ),
            confidence="suggested",
            idempotent=True,
        ),
    ]
