"""Recipe contract for the `orphan_app_state` audit.

The audit emits ``affected_ids`` with prefixes (``note:<note_id>``,
``tag:<transaction_id>``) so the recipe can dispatch to the right MCP tool
without re-querying the DB. The recipe is therefore a pure function over the
prefixed ids — no database access needed.
"""

from __future__ import annotations

from moneybin.audits.recipes import orphan_app_state, registry
from moneybin.errors import RecoveryAction


def _ctx() -> registry.RecipeContext:
    return registry.RecipeContext(db=None)


def test_note_prefix_emits_notes_delete_action() -> None:
    actions = orphan_app_state.recipe(["note:n1"], _ctx())
    assert len(actions) == 1
    action = actions[0]
    assert isinstance(action, RecoveryAction)
    assert action.tool == "transactions_notes_delete"
    assert action.arguments == {"note_id": "n1"}
    assert action.confidence == "certain"
    assert action.idempotent is False  # hard-delete is not idempotent


def test_tag_prefix_emits_tags_set_clear_action() -> None:
    actions = orphan_app_state.recipe(["tag:txn5"], _ctx())
    assert len(actions) == 1
    action = actions[0]
    assert action.tool == "transactions_tags_set"
    assert action.arguments == {"transaction_id": "txn5", "tags": []}
    assert action.confidence == "certain"
    assert action.idempotent is True  # setting tags to empty list is idempotent


def test_mixed_prefixes_emit_one_action_each() -> None:
    actions = orphan_app_state.recipe(["note:n1", "tag:txn5", "note:n2"], _ctx())
    assert [a.tool for a in actions] == [
        "transactions_notes_delete",
        "transactions_tags_set",
        "transactions_notes_delete",
    ]


def test_unprefixed_id_is_skipped() -> None:
    # Future-proofing: an unprefixed id (some other audit's affected_ids
    # leaking in by mistake) should not crash or produce a malformed action.
    actions = orphan_app_state.recipe(["bare_id"], _ctx())
    assert actions == []
