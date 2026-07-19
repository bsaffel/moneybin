"""Recipe contract for the `orphan_app_state` audit.

The audit emits ``affected_ids`` with prefixes (``note:<transaction_id>``,
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


def test_note_prefix_emits_note_clear_action() -> None:
    actions = orphan_app_state.recipe(["note:txn1"], _ctx())
    assert len(actions) == 1
    action = actions[0]
    assert isinstance(action, RecoveryAction)
    assert action.tool == "transactions_annotate"
    assert action.arguments == {
        "requests": [{"kind": "note_set", "transaction_id": "txn1", "note": None}]
    }
    assert action.confidence == "certain"
    assert action.idempotent is True


def test_empty_note_transaction_id_is_skipped_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A bare prefix must not produce an action the agent cannot run.
    with caplog.at_level("WARNING", logger="moneybin.audits.recipes.orphan_app_state"):
        actions = orphan_app_state.recipe(["note:"], _ctx())
    assert actions == []
    assert any("empty transaction_id" in r.message for r in caplog.records)


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


def test_tag_prefix_emits_tags_clear_action() -> None:
    actions = orphan_app_state.recipe(["tag:txn5"], _ctx())
    assert len(actions) == 1
    action = actions[0]
    assert action.tool == "transactions_annotate"
    assert action.arguments == {
        "requests": [{"kind": "tags_set", "transaction_id": "txn5", "tags": []}]
    }
    assert action.confidence == "certain"
    assert action.idempotent is True


def test_mixed_prefixes_emit_one_action_each() -> None:
    actions = orphan_app_state.recipe(["note:n1", "tag:txn5", "note:n2"], _ctx())
    assert [action.tool for action in actions] == [
        "transactions_annotate",
        "transactions_annotate",
        "transactions_annotate",
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
