"""Static closure guards for the bounded standard MCP surface."""

from __future__ import annotations

import ast
import inspect
import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock, patch

from fastmcp.tools import FunctionTool
from pydantic import JsonValue

from moneybin.audits.recipes import registry as recipe_registry
from moneybin.database import Database
from moneybin.mcp import prompts
from moneybin.mcp.surface import STANDARD_TOOL_NAMES
from moneybin.mcp.tools.reports import reports
from moneybin.reports._framework.catalog import get_report_catalog
from moneybin.reports._framework.contract import ReportSpec

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


def _mentioned_names(text: str, names: set[str] | frozenset[str]) -> set[str]:
    """Return exact public names mentioned in executable-looking prose."""
    return {
        name
        for name in names
        if re.search(rf"(?<![a-z0-9_]){re.escape(name)}(?![a-z0-9_])", text)
    }


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


def test_review_auto_rules_describes_only_persisted_rule_operations() -> None:
    text = prompts.review_auto_rules()

    assert "transactions_categorize_rules(view='active')" in text
    assert "transactions_categorize_rules(view='history')" in text
    assert "transactions_categorize_rules_set" in text
    assert "transactions_categorize_run" in text
    for unsupported_claim in (
        "proposal",
        "sample transaction",
        "trigger count",
        "auto_accept",
    ):
        assert unsupported_claim not in text.lower()


async def test_report_catalog_examples_use_executable_standard_calls() -> None:
    response = await reports()

    unresolved: dict[str, list[str]] = {}
    for entry in response.data.reports:
        invalid = [
            example
            for example in entry.examples
            if (
                not _mentioned_names(example, STANDARD_TOOL_NAMES)
                or _tool_references(example) - STANDARD_TOOL_NAMES
            )
        ]
        if invalid:
            unresolved[entry.report_id] = invalid

    assert unresolved == {}


def test_report_result_actions_use_executable_standard_calls() -> None:
    parameters: dict[str, dict[str, JsonValue]] = {
        "core:balance_drift": {},
        "core:cashflow": {},
        "core:large_transactions": {},
        "core:merchants": {},
        "core:networth": {},
        "core:networth_history": {
            "from_date": "2026-01-01",
            "to_date": "2026-07-01",
        },
        "core:recurring": {},
        "core:spending": {},
    }
    unresolved: dict[str, list[str]] = {}
    catalog = get_report_catalog()
    networth = MagicMock()
    networth.current.return_value = SimpleNamespace(
        balance_date=None,
        net_worth=0,
        total_assets=0,
        total_liabilities=0,
        account_count=0,
        per_account=[],
    )
    networth.history.return_value = SimpleNamespace(points=[])
    with patch(
        "moneybin.reports.service_reports.NetworthService",
        return_value=networth,
    ):
        for report_id, supplied in parameters.items():
            spec = catalog.resolve(report_id)
            db_mock = MagicMock(spec=Database)
            if isinstance(spec, ReportSpec):
                cursor = MagicMock()
                cursor.description = [(column.name,) for column in spec.columns]
                cursor.fetchmany.return_value = []
                db_mock.execute.return_value = cursor
            db = cast(Database, db_mock)
            result = catalog.execute(
                db,
                report_id=report_id,
                parameters=supplied,
                limit=0,
            )
            invalid = [
                action
                for action in result.actions
                if (
                    not _mentioned_names(action, STANDARD_TOOL_NAMES)
                    or _tool_references(action) - STANDARD_TOOL_NAMES
                )
            ]
            if invalid:
                unresolved[report_id] = invalid

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
