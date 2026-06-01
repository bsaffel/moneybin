"""Recipe for the ``categorization_coverage`` audit (warn).

When >50% of non-transfer transactions are uncategorized, the suggested fix
is to re-run the deterministic categorization cascade. Both engines are
idempotent (an earlier write blocks a later one via source-precedence), so
running this twice = running it once.

``confidence='suggested'`` (not ``'certain'``) because the cascade can
legitimately apply 0 rows when the user has no active rules or merchant
mappings that match the remaining uncategorized transactions — the
coverage warning would fire again on the next doctor run unchanged. Per
the ``RecoveryAction`` semantic, ``'certain'`` means the action *will*
fix the invariant; this one might not, so the agent should verify after.
"""

from __future__ import annotations

from moneybin.audits.recipes.registry import RecipeContext
from moneybin.errors import RecoveryAction


def recipe(
    affected_ids: list[str],  # noqa: ARG001 — coverage audit doesn't track ids
    context: RecipeContext,  # noqa: ARG001 — pure recipe
) -> list[RecoveryAction]:
    """Emit a single deterministic-categorize-run action (suggested first step)."""
    return [
        RecoveryAction(
            tool="transactions_categorize_run",
            arguments={"methods": ["rules", "merchants"]},
            rationale=(
                "Run the deterministic categorization cascade (rules + merchants) "
                "to raise coverage above the 50% threshold. Suggested (not certain) "
                "because the cascade applies 0 rows when no active rules or merchant "
                "mappings match the remaining uncategorized transactions — re-run "
                "the doctor after to verify."
            ),
            confidence="suggested",
            idempotent=True,
        ),
    ]
