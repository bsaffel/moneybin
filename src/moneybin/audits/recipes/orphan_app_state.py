"""Recipe for the ``orphan_app_state`` audit.

Orphan notes and tags are cleared through the standard declarative transaction
annotation boundary. The service permits only empty target states when the
transaction is absent, so this repair path cannot attach new app state to an
unknown transaction.

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
    """Produce one executable declarative clear per orphan transaction."""
    actions: list[RecoveryAction] = []
    for aid in affected_ids:
        if aid.startswith("note:"):
            txn_id = aid[len("note:") :]
            if not txn_id:
                logger.warning(
                    f"orphan_app_state recipe: empty transaction_id in {aid!r}"
                )
                continue
            actions.append(
                RecoveryAction(
                    tool="transactions_annotate",
                    arguments={
                        "requests": [
                            {
                                "kind": "note_set",
                                "transaction_id": txn_id,
                                "note": None,
                            }
                        ]
                    },
                    rationale=(
                        "Clear notes whose transaction is absent from the "
                        "canonical transaction view."
                    ),
                    confidence="certain",
                    idempotent=True,
                )
            )
        elif aid.startswith("tag:"):
            txn_id = aid[len("tag:") :]
            if not txn_id:
                logger.warning(
                    f"orphan_app_state recipe: empty transaction_id in {aid!r}"
                )
                continue
            actions.append(
                RecoveryAction(
                    tool="transactions_annotate",
                    arguments={
                        "requests": [
                            {
                                "kind": "tags_set",
                                "transaction_id": txn_id,
                                "tags": [],
                            }
                        ]
                    },
                    rationale=(
                        "Clear tags whose transaction is absent from the "
                        "canonical transaction view."
                    ),
                    confidence="certain",
                    idempotent=True,
                )
            )
        else:
            # Audit-recipe drift guard: a new audit prefix added to
            # _run_orphan_app_state (e.g. 'split:<id>') without updating this
            # recipe would silently produce zero actions for that branch.
            logger.warning(
                f"orphan_app_state recipe: unknown id prefix in {aid!r} "
                "(expected 'note:' or 'tag:'); skipping"
            )
    return actions
