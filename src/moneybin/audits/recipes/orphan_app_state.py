"""Recipe for the ``orphan_app_state`` audit.

The audit emits prefixed ids so the recipe can dispatch by entity type
without re-querying the DB:

- ``note:<note_id>``      → ``transactions_notes_delete(note_id=...)``
- ``tag:<transaction_id>`` → ``transactions_tags_set(transaction_id=..., tags=[])``

Tags are cleared wholesale per transaction because *every* tag on an orphan
transaction is itself an orphan — there's no legitimate tag to preserve.

PR 8 will add a list form of ``transactions_notes_delete``; until then the
recipe emits one action per orphan note id against the existing single-id tool.
"""

from __future__ import annotations

from moneybin.audits.recipes.registry import RecipeContext
from moneybin.errors import RecoveryAction


def recipe(
    affected_ids: list[str],
    context: RecipeContext,  # noqa: ARG001 — pure recipe; signature mandated by registry
) -> list[RecoveryAction]:
    """Produce one executable RecoveryAction per orphan id."""
    actions: list[RecoveryAction] = []
    for aid in affected_ids:
        if aid.startswith("note:"):
            note_id = aid[len("note:") :]
            actions.append(
                RecoveryAction(
                    tool="transactions_notes_delete",
                    arguments={"note_id": note_id},
                    rationale=(
                        f"Delete orphan note {note_id} — its transaction "
                        "no longer exists in core.fct_transactions."
                    ),
                    confidence="certain",
                    idempotent=False,
                )
            )
        elif aid.startswith("tag:"):
            txn_id = aid[len("tag:") :]
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
    return actions
