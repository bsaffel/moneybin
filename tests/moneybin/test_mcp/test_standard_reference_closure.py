"""Static closure guards for the bounded standard MCP surface."""

from __future__ import annotations

import ast
import inspect
import json
import re
from pathlib import Path

from fastmcp.tools import FunctionTool

from moneybin.audits.recipes import registry as recipe_registry
from moneybin.mcp import prompts
from moneybin.mcp.surface import STANDARD_TOOL_NAMES

_ROOT = Path(__file__).parents[3]
_TOOLS_DIR = _ROOT / "src/moneybin/mcp/tools"
_BASELINE = _ROOT / "tests/fixtures/mcp_surface/baseline-2026-07-17.json"


def _decorator_name(decorator: ast.expr) -> str | None:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    if isinstance(target, ast.Name):
        return target.id
    if isinstance(target, ast.Attribute):
        return target.attr
    return None


def _decorated_callbacks() -> set[str]:
    callbacks: set[str] = set()
    for path in sorted(_TOOLS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            if any(_decorator_name(item) == "mcp_tool" for item in node.decorator_list):
                callbacks.add(node.name)
    return callbacks


async def _live_callback_names() -> set[str]:
    from moneybin.mcp.server import init_db, mcp

    init_db()
    callbacks: set[str] = set()
    for name in STANDARD_TOOL_NAMES:
        tool = await mcp.get_tool(name)
        assert isinstance(tool, FunctionTool)
        closure = inspect.getclosurevars(tool.fn).nonlocals
        callbacks.add(closure["fn"].__name__)
    return callbacks


def _known_tool_names() -> frozenset[str]:
    payload = json.loads(_BASELINE.read_text())
    baseline = {row["name"] for row in payload["tools"]}
    return frozenset(baseline | set(STANDARD_TOOL_NAMES))


def _tool_references(text: str) -> set[str]:
    known = _known_tool_names()
    tokens = set(re.findall(r"\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b", text))
    return tokens & known


def _prompt_texts() -> dict[str, str]:
    source = Path(prompts.__file__)
    tree = ast.parse(source.read_text(), filename=str(source))
    names = {
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and any(_decorator_name(item) == "prompt" for item in node.decorator_list)
    }
    return {name: getattr(prompts, name)() for name in names}


def _emitted_tool_strings() -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for path in sorted(_TOOLS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        docstrings = {
            id(node.body[0].value)
            for node in ast.walk(tree)
            if isinstance(
                node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
            )
            and node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)
        }
        strings = [
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
            and _tool_references(node.value)
        ]
        if strings:
            result[str(path.relative_to(_ROOT))] = strings
    return result


async def test_only_live_standard_callbacks_are_decorated_as_mcp_tools() -> None:
    decorated = _decorated_callbacks()
    live = await _live_callback_names()

    assert decorated == live, (
        f"obsolete={sorted(decorated - live)!r}; "
        f"missing_decorators={sorted(live - decorated)!r}"
    )


def test_shipped_prompts_reference_only_standard_tools() -> None:
    unresolved = {
        prompt_name: sorted(_tool_references(text) - STANDARD_TOOL_NAMES)
        for prompt_name, text in _prompt_texts().items()
        if _tool_references(text) - STANDARD_TOOL_NAMES
    }

    assert unresolved == {}


def test_emitted_tool_strings_reference_only_standard_tools() -> None:
    unresolved = {
        path: sorted({
            reference
            for text in strings
            for reference in _tool_references(text) - STANDARD_TOOL_NAMES
        })
        for path, strings in _emitted_tool_strings().items()
    }
    unresolved = {path: names for path, names in unresolved.items() if names}

    assert unresolved == {}


def test_recovery_recipes_reference_only_standard_tools() -> None:
    cases = {
        "orphan_app_state": ["note:n1", "tag:txn1"],
        "categorization_coverage": [],
        "dedup_reconciliation": [],
    }
    unresolved: dict[str, list[str]] = {}
    for name, affected_ids in cases.items():
        recipe = recipe_registry.get(name)
        assert recipe is not None
        actions = recipe(
            affected_ids,
            recipe_registry.RecipeContext(db=None),
        )
        stale = sorted({
            action.tool for action in actions if action.tool not in STANDARD_TOOL_NAMES
        })
        if stale:
            unresolved[name] = stale

    assert unresolved == {}
