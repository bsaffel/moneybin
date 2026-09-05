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
    investment_source_overlap,
    orphan_app_state,
    registry,
)
from moneybin.mcp.tools.import_tools import import_revert_coarse
from moneybin.mcp.tools.refresh import refresh_run
from moneybin.mcp.tools.system import system_status_coarse
from moneybin.mcp.tools.transactions import transactions_annotate_coarse
from moneybin.mcp.tools.transactions_categorize import transactions_categorize_run

# All MCP tool functions a PR4 recipe may name. New tools cited in future
# recipes MUST be added here — that requirement is the whole point of this
# fixture; the test fails fast if a recipe references an unregistered name.
_TOOLS: dict[str, Callable[..., Any]] = {
    "transactions_annotate": transactions_annotate_coarse,
    "transactions_categorize_run": transactions_categorize_run,
    "refresh_run": refresh_run,
    "system_status": system_status_coarse,
    "import_revert": import_revert_coarse,
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
    pytest.param("orphan_app_state", ["note:txn1"], id="orphan_app_state-note"),
    pytest.param("orphan_app_state", ["tag:txn5"], id="orphan_app_state-tag"),
    pytest.param(
        "orphan_app_state", ["note:txn1", "tag:txn5"], id="orphan_app_state-mixed"
    ),
    pytest.param("categorization_coverage", [], id="categorization_coverage"),
    pytest.param("dedup_reconciliation", [], id="dedup_reconciliation"),
    pytest.param("investment_source_overlap", [], id="investment_source_overlap-empty"),
    pytest.param(
        "investment_source_overlap", ["acc_1"], id="investment_source_overlap"
    ),
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
        investment_source_overlap: "investment_source_overlap",
        orphan_app_state: "orphan_app_state",
    }
    for module, name in modules_to_audit_names.items():
        registered = registry.get(name)
        assert registered is module.recipe, (
            f"Recipe module {module.__name__} is not registered under '{name}'. "
            f"Add `register('{name}', {module.__name__.split('.')[-1]}.recipe)` "
            f"in moneybin/audits/recipes/__init__.py."
        )


def test_dedup_reconciliation_requests_full_doctor_detail() -> None:
    actions = dedup_reconciliation.recipe([], registry.RecipeContext(db=None))
    doctor = next(action for action in actions if action.tool == "system_status")

    assert doctor.arguments == {
        "sections": ["doctor"],
        "detail": "full",
    }


def test_source_overlap_offers_only_a_remedy_that_clears_it() -> None:
    """Every offered action must be able to end the state it is offered for.

    ``sync_disconnect`` cannot. It is a remote operation — ``SyncService.
    disconnect_confirmed`` calls ``client.disconnect`` and deletes nothing
    locally, and the tool's own confirmation says "Previously pulled local rows
    remain". Both readers of the overlap keep reading exactly those rows: the
    check joins ``raw.plaid_investment_transactions``, and
    ``core.dim_holdings``'s ``source_overlap_accounts`` counts the ledger they
    feed. A user who followed it would permanently lose the connection AND keep
    the failing check and the withheld holdings.

    ``import_revert`` really does clear it: ``REVERT_TABLES['manual']`` includes
    ``raw.manual_investment_transactions``, so the batch's rows are deleted and
    the account is left with one ledger.
    """
    actions = investment_source_overlap.recipe(
        ["acc_1"], registry.RecipeContext(db=None)
    )

    assert [a.tool for a in actions] == ["import_revert"]
    assert all(a.confidence == "suggested" for a in actions)
    assert not any(a.idempotent for a in actions)
    (revert,) = actions
    # The audit carries account ids, not an import_id, so the missing argument
    # is named in the rationale rather than guessed.
    assert "import_id" not in revert.arguments
    assert "import_id" in revert.rationale


def test_source_overlap_says_a_disconnect_does_not_clear_it() -> None:
    """Dropping the action must not drop the fact a user needs to act on.

    Someone whose file import is the ledger they want will reach for
    ``sync_disconnect`` on their own. The remedy prose is where they find out
    that it stops future pulls without removing the rows already pulled — the
    one thing that keeps this check red.
    """
    (revert,) = investment_source_overlap.recipe(
        ["acc_1"], registry.RecipeContext(db=None)
    )

    assert "sync_disconnect" in revert.rationale
