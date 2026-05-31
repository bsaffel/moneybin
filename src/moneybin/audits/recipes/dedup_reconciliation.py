"""Recipe for the ``dedup_reconciliation`` audit (fail).

A staging-vs-core row-count mismatch usually means either (a) an accepted
dedup decision didn't collapse its rows (rerun matching) or (b) new rows
imported since the last transform — the audit detail already explains the
common cause. Both actions are suggested investigations, not certain fixes.
"""

from __future__ import annotations

from moneybin.audits.recipes.registry import RecipeContext
from moneybin.errors import RecoveryAction


def recipe(
    affected_ids: list[str],  # noqa: ARG001 — dedup audit doesn't carry per-row ids
    context: RecipeContext,  # noqa: ARG001 — pure recipe
) -> list[RecoveryAction]:
    """Emit refresh_run(steps=['match']) + system_doctor(full=True)."""
    return [
        RecoveryAction(
            tool="refresh_run",
            arguments={"steps": ["match"]},
            rationale=(
                "Re-run matching to apply any recorded dedup decision whose "
                "rows didn't collapse — fixes the common cause of staging vs "
                "core count drift."
            ),
            confidence="suggested",
            idempotent=True,
        ),
        RecoveryAction(
            tool="system_doctor",
            arguments={"full": True},
            rationale=(
                "Re-run all invariants in deep-scan mode to surface the "
                "specific raw/core count delta that caused the mismatch."
            ),
            confidence="suggested",
            idempotent=True,
        ),
    ]
