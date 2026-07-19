"""Doctor recipe registry: lookup contract.

The registry is a `(audit_name) -> Recipe | None` lookup populated at module
import time. Recipes are callables of `(affected_ids, RecipeContext) ->
list[RecoveryAction]`.
"""

from __future__ import annotations

from moneybin.audits.recipes import registry  # importing populates the registry


def test_get_unknown_audit_returns_none() -> None:
    assert registry.get("definitely_not_an_audit_name") is None


def test_get_known_audit_returns_callable() -> None:
    recipe = registry.get("orphan_app_state")
    assert recipe is not None
    assert callable(recipe)


def test_recipe_signature_returns_list_of_recovery_action() -> None:
    recipe = registry.get("orphan_app_state")
    assert recipe is not None
    ctx = registry.RecipeContext(db=None)
    out = recipe([], ctx)
    assert isinstance(out, list)
    # An empty affected_ids list yields zero actions.
    assert out == []


def test_orphan_recipe_with_affected_ids_withholds_unexecutable_actions() -> None:
    recipe = registry.get("orphan_app_state")
    assert recipe is not None
    ctx = registry.RecipeContext(db=None)
    out = recipe(["note:n1"], ctx)
    assert out == []
