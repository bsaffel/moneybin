"""Round-trip-executable contract for every registered recipe.

Each ``RecoveryAction`` a recipe emits MUST satisfy three properties:

1. ``action.tool`` resolves to a real MCP tool function in the MoneyBin
   codebase.
2. ``action.arguments`` binds cleanly to that tool's signature — same
   parameter names, no missing required args, no unknown keys.
3. Argument values whose parameter is typed ``Literal[...]`` are members
   of that literal — catches the case where ``sig.bind()`` accepts a typo
   like ``methods=['rule']`` that the live tool would Pydantic-reject.

This is the highest-value test in PR4: it's the one that catches recipe
drift when a tool gets renamed, an argument changes, or a literal value
is misspelled. Without it, recipes can silently emit instructions that
fail the instant an agent dispatches them.
"""

from __future__ import annotations

import inspect
import types
import typing
from collections.abc import Callable
from typing import Any, Literal, get_args, get_origin, get_type_hints

import pytest

from moneybin.audits.recipes import (
    categorization_coverage,
    dedup_reconciliation,
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
    """Strip the ``@mcp_tool`` decorator wrapper to get the real signature.

    Asserts ``__wrapped__`` is present rather than silently falling back —
    a decorator change that dropped ``functools.wraps`` would otherwise
    make this test silently degrade (the wrapper's ``*args, **kwargs``
    signature accepts every call).
    """
    wrapped = getattr(fn, "__wrapped__", None)
    assert wrapped is not None, (
        f"{fn.__name__} has no __wrapped__ attribute — the @mcp_tool "
        "decorator must use functools.wraps so signature inspection sees "
        "the real parameters. Without it this test silently passes for "
        "everything."
    )
    return wrapped


def _literal_members(annotation: object) -> tuple[Any, ...] | None:
    """If the annotation is (or wraps) ``Literal[...]``, return its members.

    Handles ``Literal['a','b']``, ``list[Literal['a','b']]``, and
    ``Literal['a','b'] | None`` (and the ``Optional`` equivalent). Returns
    ``None`` for any other shape — caller skips the membership check.
    """
    origin = get_origin(annotation)
    if origin is Literal:
        return get_args(annotation)
    # list[Literal[...]] — descend into the element type.
    if origin in (list, tuple, set, frozenset):
        args = get_args(annotation)
        if args:
            return _literal_members(args[0])
    # Literal[...] | None  (Union form — both typing.Union and PEP-604 X | Y)
    if origin is typing.Union or origin is types.UnionType:
        for arg in get_args(annotation):
            if arg is type(None):
                continue
            inner = _literal_members(arg)
            if inner is not None:
                return inner
    return None


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


@pytest.mark.parametrize(("audit_name", "affected_ids"), _RECIPE_CASES)
def test_recipe_literal_arguments_are_valid_members(
    audit_name: str, affected_ids: list[str]
) -> None:
    """Literal-member check on every emitted argument value.

    For parameters typed ``Literal`` (or ``list[Literal]``), assert the
    emitted values are members. Catches typos like ``methods=['rule']``
    (vs ``'rules'``) that ``sig.bind`` would accept but the live tool
    would Pydantic-reject.
    """
    recipe = registry.get(audit_name)
    assert recipe is not None
    actions = recipe(affected_ids, registry.RecipeContext(db=None))
    for action in actions:
        tool_fn = _underlying(_TOOLS[action.tool])
        # Resolve string-form annotations (from `from __future__ import annotations`).
        hints = get_type_hints(tool_fn)
        for arg_name, arg_value in action.arguments.items():
            annotation = hints.get(arg_name)
            if annotation is None:
                continue
            members = _literal_members(annotation)
            if members is None:
                continue
            # arg_value may be a list (for list[Literal[...]]) or scalar.
            values_to_check = arg_value if isinstance(arg_value, list) else [arg_value]
            for v in values_to_check:
                assert v in members, (
                    f"Recipe '{audit_name}' emitted invalid Literal value for "
                    f"'{action.tool}({arg_name}=...)': {v!r} is not in "
                    f"{members!r}. The live tool would reject this."
                )


def test_every_explicit_recipe_module_is_registered() -> None:
    """Every recipe module listed here must register its function.

    Guards against a refactor that adds a new recipe file but forgets the
    matching ``register(...)`` call in ``__init__.py``.
    """
    modules_to_audit_names = {
        categorization_coverage: "categorization_coverage",
        dedup_reconciliation: "dedup_reconciliation",
        orphan_app_state: "orphan_app_state",
    }
    for module, name in modules_to_audit_names.items():
        registered = registry.get(name)
        assert registered is module.recipe, (
            f"Recipe module {module.__name__} is not registered under '{name}'. "
            f"Add `register('{name}', {module.__name__.split('.')[-1]}.recipe)` "
            f"in moneybin/audits/recipes/__init__.py."
        )
