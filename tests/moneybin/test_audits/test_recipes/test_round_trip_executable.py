"""Round-trip-executable contract for every registered recipe.

Each ``RecoveryAction`` a recipe emits MUST satisfy two properties:

1. ``action.tool`` resolves to a real MCP tool function in the MoneyBin
   codebase.
2. ``action.arguments`` binds cleanly to that tool's signature — same
   parameter names, no missing required args, no unknown keys.

This is the highest-value test in PR4: it's the one that catches recipe
drift when a tool gets renamed, an argument changes, or a parameter is
added/removed. Without it, recipes can silently emit instructions that fail
the instant an agent dispatches them.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

import pytest

from moneybin.audits.recipes import (
    bridge_transfers_balanced,
    categorization_coverage,
    dedup_reconciliation,
    fct_transactions_fk_integrity,
    fct_transactions_sign_convention,
    orphan_app_state,
    registry,
)
from moneybin.mcp.tools.curation import (
    transactions_notes_delete,
    transactions_tags_set,
)
from moneybin.mcp.tools.refresh import refresh_run
from moneybin.mcp.tools.system import system_doctor
from moneybin.mcp.tools.transactions_categorize import transactions_categorize_run

# All MCP tool functions a PR4 recipe may name. New tools cited in future
# recipes MUST be added here — that requirement is the whole point of this
# fixture; the test fails fast if a recipe references an unregistered name.
_TOOLS: dict[str, Callable[..., Any]] = {
    "transactions_notes_delete": transactions_notes_delete,
    "transactions_tags_set": transactions_tags_set,
    "transactions_categorize_run": transactions_categorize_run,
    "refresh_run": refresh_run,
    "system_doctor": system_doctor,
}


def _underlying(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Strip the ``@mcp_tool`` decorator wrapper to get the real signature."""
    return getattr(fn, "__wrapped__", fn)


# (audit_name, sample affected_ids) — enough to exercise every branch in each
# recipe. Empty list also tested to confirm recipes don't choke on it.
_RECIPE_CASES = [
    pytest.param("orphan_app_state", [], id="orphan_app_state-empty"),
    pytest.param("orphan_app_state", ["note:n1"], id="orphan_app_state-note"),
    pytest.param("orphan_app_state", ["tag:txn5"], id="orphan_app_state-tag"),
    pytest.param(
        "orphan_app_state", ["note:n1", "tag:txn5"], id="orphan_app_state-mixed"
    ),
    pytest.param("categorization_coverage", [], id="categorization_coverage"),
    pytest.param("dedup_reconciliation", [], id="dedup_reconciliation"),
    pytest.param("fct_transactions_fk_integrity", [], id="fk_integrity-empty"),
    pytest.param(
        "fct_transactions_fk_integrity",
        ["txn_orphan_1", "txn_orphan_2"],
        id="fk_integrity-verbose",
    ),
    pytest.param(
        "fct_transactions_sign_convention", ["zero_amount_txn"], id="sign_convention"
    ),
    pytest.param("bridge_transfers_balanced", ["debit_txn_1"], id="bridge_balanced"),
]


@pytest.mark.parametrize(("audit_name", "affected_ids"), _RECIPE_CASES)
def test_recipe_emits_only_tool_names_that_exist(
    audit_name: str, affected_ids: list[str]
) -> None:
    recipe = registry.get(audit_name)
    assert recipe is not None
    actions = recipe(affected_ids, registry.RecipeContext(db=None))
    for action in actions:
        assert action.tool in _TOOLS, (
            f"Recipe '{audit_name}' names tool '{action.tool}', but no such MCP "
            f"tool is registered in the round-trip test fixture. Either the tool "
            f"was renamed (update the recipe) or it's missing from _TOOLS "
            f"(add it)."
        )


@pytest.mark.parametrize(("audit_name", "affected_ids"), _RECIPE_CASES)
def test_recipe_arguments_bind_to_tool_signature(
    audit_name: str, affected_ids: list[str]
) -> None:
    recipe = registry.get(audit_name)
    assert recipe is not None
    actions = recipe(affected_ids, registry.RecipeContext(db=None))
    for action in actions:
        tool_fn = _underlying(_TOOLS[action.tool])
        sig = inspect.signature(tool_fn)
        try:
            sig.bind(**action.arguments)
        except TypeError as e:
            pytest.fail(
                f"Recipe '{audit_name}' emitted invalid arguments for "
                f"'{action.tool}': {action.arguments!r}. "
                f"Signature: {sig}. Error: {e}"
            )


def test_every_explicit_recipe_module_is_registered() -> None:
    """Every recipe module listed here must register its function.

    Guards against a refactor that adds a new recipe file but forgets the
    matching ``register(...)`` call in ``__init__.py``.
    """
    modules_to_audit_names = {
        bridge_transfers_balanced: "bridge_transfers_balanced",
        categorization_coverage: "categorization_coverage",
        dedup_reconciliation: "dedup_reconciliation",
        fct_transactions_fk_integrity: "fct_transactions_fk_integrity",
        fct_transactions_sign_convention: "fct_transactions_sign_convention",
        orphan_app_state: "orphan_app_state",
    }
    for module, name in modules_to_audit_names.items():
        registered = registry.get(name)
        assert registered is module.recipe, (
            f"Recipe module {module.__name__} is not registered under '{name}'. "
            f"Add `register('{name}', {module.__name__.split('.')[-1]}.recipe)` "
            f"in moneybin/audits/recipes/__init__.py."
        )
