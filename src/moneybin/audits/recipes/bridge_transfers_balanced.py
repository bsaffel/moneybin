"""Recipe for the ``bridge_transfers_balanced`` SQLMesh audit.

A transfer pair whose debit+credit don't cancel is almost always a bad
``app.match_decisions`` row (the wrong pair was accepted). The targeted fix is
``transactions_matches_set(match_id, status='rejected')`` — but the audit's
affected_ids are debit *transaction_ids*, not *match_ids*, so PR4 emits a
suggested deep-scan and defers the per-id rejection action to a future PR
once the match_id resolver lands.
"""

from __future__ import annotations

from moneybin.audits.recipes.registry import RecipeContext
from moneybin.errors import RecoveryAction


def recipe(
    affected_ids: list[str],  # noqa: ARG001 — investigation only; no per-id action today
    context: RecipeContext,  # noqa: ARG001 — pure recipe
) -> list[RecoveryAction]:
    """Emit a single deep-scan investigation hint (suggested)."""
    return [
        RecoveryAction(
            tool="system_doctor",
            arguments={"full": True},
            rationale=(
                "Run all invariants in deep-scan mode to identify every "
                "unbalanced transfer pair and the related match decisions."
            ),
            confidence="suggested",
            idempotent=True,
        ),
    ]
