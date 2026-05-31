"""Recipe for the ``fct_transactions_fk_integrity`` SQLMesh audit.

Orphaned transactions (account_id missing from dim_accounts) usually point at
a bad import. The certain fix is provider-specific (``import_revert`` if the
import is identifiable; manual `dim_accounts` repair otherwise), so PR4
emits a single suggested investigation that runs every invariant in deep mode.
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
                "Run all invariants in deep-scan mode to surface the full set "
                "of orphan transactions and the related app.* integrity state."
            ),
            confidence="suggested",
            idempotent=True,
        ),
    ]
