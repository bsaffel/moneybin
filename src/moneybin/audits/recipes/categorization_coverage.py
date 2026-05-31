"""Recipe for the ``categorization_coverage`` audit (warn).

When >50% of non-transfer transactions are uncategorized, the prescribed fix
is to re-run the deterministic categorization cascade. Both engines are
idempotent (an earlier write blocks a later one via source-precedence), so
running this twice = running it once.
"""

from __future__ import annotations

from moneybin.audits.recipes.registry import RecipeContext
from moneybin.errors import RecoveryAction


def recipe(
    affected_ids: list[str],  # noqa: ARG001 — coverage audit doesn't track ids
    context: RecipeContext,  # noqa: ARG001 — pure recipe
) -> list[RecoveryAction]:
    """Emit a single deterministic-categorize-run action (certain fix)."""
    return [
        RecoveryAction(
            tool="transactions_categorize_run",
            arguments={"methods": ["rules", "merchants"]},
            rationale=(
                "Run the deterministic categorization cascade (rules + merchants) "
                "to raise coverage above the 50% threshold."
            ),
            confidence="certain",
            idempotent=True,
        ),
    ]
