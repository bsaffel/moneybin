"""Recipe for the ``orphan_app_state`` audit.

The standard transaction annotation boundary requires a canonical transaction,
so it cannot safely clear app rows whose transaction is absent from core. The
recipe therefore emits no falsely executable recovery action for the audit's
``note:`` and ``tag:`` identifiers and logs the unsupported cleanup explicitly.

Unknown prefixes are surfaced via ``logger.warning`` so audit-recipe drift
(e.g., a future ``split:<split_id>`` branch added to the audit but not the
recipe) is visible in dev/test rather than silently producing fewer actions
than affected ids.
"""

from __future__ import annotations

import logging

from moneybin.audits.recipes.registry import RecipeContext
from moneybin.errors import RecoveryAction

logger = logging.getLogger(__name__)


def recipe(
    affected_ids: list[str],
    context: RecipeContext,  # noqa: ARG001 — pure recipe; signature mandated by registry
) -> list[RecoveryAction]:
    """Warn for orphan state that has no executable standard cleanup."""
    for aid in affected_ids:
        if aid.startswith("note:"):
            note_id = aid[len("note:") :]
            if not note_id:
                logger.warning(f"orphan_app_state recipe: empty note_id in {aid!r}")
                continue
            logger.warning(
                f"orphan_app_state recipe: note {note_id!r} has no standard "
                "cleanup action because its transaction is absent from core"
            )
        elif aid.startswith("tag:"):
            txn_id = aid[len("tag:") :]
            if not txn_id:
                logger.warning(
                    f"orphan_app_state recipe: empty transaction_id in {aid!r}"
                )
                continue
            logger.warning(
                f"orphan_app_state recipe: tags for transaction {txn_id!r} have "
                "no standard cleanup action because the transaction is absent "
                "from core"
            )
        else:
            # Audit-recipe drift guard: a new audit prefix added to
            # _run_orphan_app_state (e.g. 'split:<id>') without updating this
            # recipe would silently produce zero actions for that branch.
            logger.warning(
                f"orphan_app_state recipe: unknown id prefix in {aid!r} "
                "(expected 'note:' or 'tag:'); skipping"
            )
    return []
