"""Recipe for the ``fct_transactions_sign_convention`` SQLMesh audit.

Zero / NULL amounts usually originate at the source import (a malformed CSV
column or an ambiguous OFX field). PR4 emits a suggested deep-scan; a future
PR can pair this with ``import_revert`` once the source import_id is wired
into the affected_ids.
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
                "Run all invariants in deep-scan mode to identify the full "
                "set of zero/NULL-amount transactions and trace them back to "
                "their source imports."
            ),
            confidence="suggested",
            idempotent=True,
        ),
    ]
