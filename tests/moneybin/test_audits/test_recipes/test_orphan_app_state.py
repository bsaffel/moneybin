"""Recipe contract for the `orphan_app_state` audit.

The audit emits ``affected_ids`` with prefixes (``note:<note_id>``,
``tag:<transaction_id>``) so the recipe can dispatch to the right MCP tool
without re-querying the DB. The recipe is therefore a pure function over the
prefixed ids — no database access needed.
"""

from __future__ import annotations

import pytest

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
    # Suggested (not certain) because the single-id delete is non-idempotent
    # across a multi-orphan batch — mid-stream retry would raise LookupError
    # on the already-succeeded ids. PR 8's list form will upgrade to certain.
    assert action.confidence == "suggested"
    assert action.idempotent is False


def test_empty_note_id_is_skipped_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # An empty note_id (e.g. a future audit-shape bug emitting a bare 'note:'
    # prefix) must not produce a malformed RecoveryAction the agent can't run.
    with caplog.at_level("WARNING", logger="moneybin.audits.recipes.orphan_app_state"):
        actions = orphan_app_state.recipe(["note:"], _ctx())
    assert actions == []
    assert any("empty note_id" in r.message for r in caplog.records)


def test_empty_transaction_id_is_skipped_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING", logger="moneybin.audits.recipes.orphan_app_state"):
        actions = orphan_app_state.recipe(["tag:"], _ctx())
    assert actions == []
    assert any("empty transaction_id" in r.message for r in caplog.records)


def test_unknown_prefix_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    # Future audit-recipe drift (e.g. a 'split:<id>' prefix added to the audit
    # but not the recipe) must surface in logs rather than silently produce
    # fewer actions than the audit's affected_ids count.
    with caplog.at_level("WARNING", logger="moneybin.audits.recipes.orphan_app_state"):
        actions = orphan_app_state.recipe(["split:s1"], _ctx())
    assert actions == []
    assert any("unknown id prefix" in r.message for r in caplog.records)


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


def test_unprefixed_id_is_skipped_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Future-proofing: an unprefixed id (some other audit's affected_ids
    # leaking in by mistake) does not produce a malformed action, AND the
    # drift surfaces as a warning rather than silent zero-output.
    with caplog.at_level("WARNING", logger="moneybin.audits.recipes.orphan_app_state"):
        actions = orphan_app_state.recipe(["bare_id"], _ctx())
    assert actions == []
    assert any("unknown id prefix" in r.message for r in caplog.records)
