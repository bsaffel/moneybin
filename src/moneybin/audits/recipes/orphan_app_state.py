"""Recipe for the ``orphan_app_state`` audit.

The audit emits prefixed ids so the recipe can dispatch by entity type
without re-querying the DB:

- ``note:<note_id>``      → ``transactions_notes_delete(note_id=...)``
- ``tag:<transaction_id>`` → ``transactions_tags_set(transaction_id=..., tags=[])``

Tags are cleared wholesale per transaction because *every* tag on an orphan
transaction is itself an orphan — there's no legitimate tag to preserve.

Notes use ``confidence='suggested'`` (not ``'certain'``) because the single-id
``transactions_notes_delete`` is non-idempotent: mid-batch failures across
many orphan notes can't be safely retried (re-dispatching an
already-succeeded delete raises ``LookupError``). PR 8 will add a list form
of ``transactions_notes_delete``; once landed, the recipe can emit a single
atomic action with ``confidence='certain'`` and ``idempotent=True``.

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
    """Produce one executable RecoveryAction per orphan id."""
    actions: list[RecoveryAction] = []
    for aid in affected_ids:
        if aid.startswith("note:"):
            note_id = aid[len("note:") :]
            if not note_id:
                logger.warning(f"orphan_app_state recipe: empty note_id in {aid!r}")
                continue
            actions.append(
                RecoveryAction(
                    tool="transactions_notes_delete",
                    arguments={"note_id": note_id},
                    rationale=(
                        f"Delete orphan note {note_id} — its transaction "
                        "no longer exists in core.fct_transactions. Suggested "
                        "(not certain) because the single-id delete is "
                        "non-idempotent across a multi-orphan batch; PR 8's "
                        "list form will upgrade this to certain."
                    ),
                    confidence="suggested",
                    idempotent=False,
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
                    tool="transactions_tags_set",
                    arguments={"transaction_id": txn_id, "tags": []},
                    rationale=(
                        f"Clear all tags on orphan transaction {txn_id} — "
                        "the transaction itself is gone from core.fct_transactions."
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
