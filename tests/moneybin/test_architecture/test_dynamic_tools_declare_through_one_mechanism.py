"""Every dynamically classified tool declares through a sanctioned mechanism.

`test_classified_envelope_is_the_only_pipeline.py` catches a *re-pasted*
primitive: a surface module that calls `extract_data_classes`, `derive_tier`,
or `redact_typed` itself. It cannot see the other shape — a
``dynamic_classification=True`` tool that hand-builds its envelope and calls
none of the three. Such a tool trips no source scan while setting its own
``summary.sensitivity`` and ``classes_returned``: the two fields an agent reads
before it ever sees the payload, and the two that land in the privacy audit row.

So this guard starts from the decorator's registry rather than from source
calls. Every registered dynamic tool must reach ``build_classified_envelope``,
which derives both fields from the payload's declared classes. A tool that
sets either field by hand — anywhere in its call tree, through any callee — is
declaring outside that mechanism and must be listed in ``SELF_DECLARING_TOOLS``
with the mechanism it uses instead. The list is asserted by set equality in
both directions, so a sixth such tool fails until someone adds it deliberately,
and an entry that stops declaring fails until someone removes it.

Two boundaries, stated rather than implied:

- Reachability follows calls by name through ``src/moneybin/mcp`` only, the
  same roots the sibling guard scans. A call it cannot resolve (a method, a
  callback passed as an argument, a helper outside those roots) ends that
  branch, so a tool that moves its envelope building out of the package reads
  as reaching no mechanism and fails. That is the intended direction: a false
  alarm is a review, a missed hand-declaration is an unmasked payload.
- The unit is the tool, not the call site. A finer (module, function) list
  would pin each branch, but these files are refactored often enough that the
  churn would land on unrelated PRs; the tool is what an agent calls and what
  the registry names.
- The scan is deliberately conservative about *where* the declaration sits: a
  hand-declared field inside a helper counts even where the tool later rebuilds
  the envelope through the builder, because that arrangement is one refactor
  away from returning the helper's envelope instead. Two entries below are that
  shape, and each says why the hand-declared value does not reach the wire.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastmcp.tools import FunctionTool

from moneybin.privacy.classified_envelope import build_classified_envelope
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta, build_envelope

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_ROOT = REPO_ROOT / "src" / "moneybin" / "mcp"

# The one builder that derives both declared fields from the payload contract.
# Taken from the symbol so a rename cannot leave the scan hunting a dead name.
BUILDER = build_classified_envelope.__name__

# Passing either as a keyword IS the hand-declaration, whatever the callee is
# named — `build_envelope`, an error builder, or a decorator factory that
# stamps the finished envelope. Naming the fields rather than the constructors
# is what keeps the rule from buying exactly one round against the next shape.
DECLARED_FIELDS = frozenset({"sensitivity", "classes_returned"})

# Registered dynamic tools whose call tree sets a declared field by hand, and
# the mechanism each one declares from instead of the payload's annotations.
SELF_DECLARING_TOOLS: dict[str, str] = {
    "reports": (
        "The `@report` declared-class contract. A report's rows are arbitrary "
        "SQL-produced columns that payload-type introspection cannot classify, "
        "so the catalog branch declares from `catalog_sensitivity` / "
        "`catalog_classes_returned` and the execute branch from `result.tier` / "
        "`result.classes_returned`. `.claude/rules/reports.md` binds that "
        "surface to declared privacy classes with its own CI verification."
    ),
    "sql_query": (
        "The SQL lineage classifier. `execute_sql_query` resolves every output "
        "column to a data class through sqlglot and returns the tier and class "
        "list with the records; there is no payload type to introspect, because "
        "the shape is the user's query."
    ),
    "sql_schema": (
        "Declared constants for a curated schema document — table names, column "
        "comments, and example queries. The payload carries no user data and no "
        "typed contract, so there is nothing for the builder to classify."
    ),
    "import_preview": (
        "Its preview helpers declare `sensitivity` themselves — the PDF "
        "confirmation branches on a bare dict, the tabular branch beside a "
        "typed payload. The registered tool consumes those envelopes as "
        "intermediate values and returns, at its single exit, one the builder "
        "produced from the payload it assembles, so the hand-declared value "
        "does not reach the wire through this tool."
    ),
    "gsheet_connect": (
        "Its internal helpers carry `@internal_envelope_adapter(sensitivity=…)`, "
        "which stamps a static sensitivity on what they return. The registered "
        "tool rebuilds all three success branches through the builder and "
        "passes a helper envelope through only when it carries an error."
    ),
}

# Where the sanctioned mechanism is a *derivation*, a literal is the silent
# downgrade set equality cannot see: swapping `result.tier` for "low" keeps the
# tool on this list while turning its declaration into an assertion.
MUST_DERIVE_ITS_DECLARATION = frozenset({"reports", "sql_query"})


@dataclass(frozen=True, slots=True)
class Declaration:
    """One call that sets a declared field by hand, and where it lives."""

    module: str
    function: str
    callee: str
    fields: tuple[str, ...]
    literal: bool

    def __str__(self) -> str:
        """Render the site the way the assertion message needs to name it."""
        rendered = ", ".join(f"{field}=" for field in self.fields)
        return f"{self.module}:{self.function} calls {self.callee}({rendered})"


@dataclass(frozen=True, slots=True)
class Mechanism:
    """What a tool's call tree does about its envelope's declared fields."""

    reaches_builder: bool
    declarations: tuple[Declaration, ...]

    @property
    def self_declares(self) -> bool:
        return bool(self.declarations)

    @property
    def reaches_no_mechanism(self) -> bool:
        return not self.reaches_builder and not self.declarations


def _call_name(node: ast.Call) -> str | None:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _is_literal(node: ast.expr) -> bool:
    """Whether an argument is spelled out rather than derived at runtime."""
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.List | ast.Tuple):
        return all(isinstance(element, ast.Constant) for element in node.elts)
    return False


def _declaration(node: ast.Call, *, module: str, function: str) -> Declaration | None:
    fields = tuple(
        sorted(
            keyword.arg for keyword in node.keywords if keyword.arg in DECLARED_FIELDS
        )
    )
    if not fields:
        return None
    return Declaration(
        module=module,
        function=function,
        callee=_call_name(node) or "<computed>",
        fields=fields,
        literal=all(
            _is_literal(keyword.value)
            for keyword in node.keywords
            if keyword.arg in DECLARED_FIELDS
        ),
    )


_ModuleIndex = tuple[
    dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
    dict[str, tuple[Path, str]],
]
_INDEX_CACHE: dict[Path, _ModuleIndex] = {}


def _mcp_module_path(dotted: str) -> Path | None:
    """Resolve a ``moneybin.mcp`` dotted name to the file that defines it."""
    if not dotted.startswith("moneybin.mcp"):
        return None
    relative = dotted.removeprefix("moneybin.mcp").lstrip(".").replace(".", "/")
    if not relative:
        return MCP_ROOT / "__init__.py"
    for candidate in (MCP_ROOT / f"{relative}.py", MCP_ROOT / relative / "__init__.py"):
        if candidate.exists():
            return candidate
    return None


def _index(path: Path) -> _ModuleIndex:
    """Return a module's function definitions and its ``moneybin.mcp`` imports."""
    cached = _INDEX_CACHE.get(path)
    if cached is not None:
        return cached
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    imports: dict[str, tuple[Path, str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.level or node.module is None:
            continue
        source = _mcp_module_path(node.module)
        if source is None:
            continue
        for alias in node.names:
            imports[alias.asname or alias.name] = (source, alias.name)
    index = (functions, imports)
    _INDEX_CACHE[path] = index
    return index


def mechanism_of(path: Path, function: str) -> Mechanism:
    """Analyze what ``function`` in ``path`` does about its declared fields.

    Walks the call tree by name through ``src/moneybin/mcp``. A function's
    decorators are read for declarations but not followed: a decorator factory
    can stamp the finished envelope, while its own body belongs to whichever
    module defines it.
    """
    functions, _ = _index(path)
    assert function in functions, (
        f"{path.name} does not define {function}(); the registry and the source "
        "disagree, so the guard cannot tell which mechanism the tool uses."
    )

    reaches_builder = False
    declarations: list[Declaration] = []
    seen: set[tuple[Path, str]] = set()
    pending: list[tuple[Path, str]] = [(path, function)]

    while pending:
        current_path, current_name = pending.pop()
        if (current_path, current_name) in seen:
            continue
        seen.add((current_path, current_name))
        current_functions, imports = _index(current_path)
        node = current_functions.get(current_name)
        if node is None:
            continue

        body_calls = [
            child
            for statement in node.body
            for child in ast.walk(statement)
            if isinstance(child, ast.Call)
        ]
        decorator_calls = [
            child
            for decorator in node.decorator_list
            for child in ast.walk(decorator)
            if isinstance(child, ast.Call)
        ]
        for call in body_calls + decorator_calls:
            declaration = _declaration(
                call, module=current_path.name, function=current_name
            )
            if declaration is not None:
                declarations.append(declaration)

        for call in body_calls:
            callee = _call_name(call)
            if callee is None:
                continue
            if callee == BUILDER:
                reaches_builder = True
            if callee in current_functions:
                pending.append((current_path, callee))
            elif callee in imports:
                pending.append(imports[callee])

    return Mechanism(
        reaches_builder=reaches_builder,
        declarations=tuple(dict.fromkeys(declarations)),
    )


async def _dynamic_tool_mechanisms() -> dict[str, Mechanism]:
    """Analyze every registered ``dynamic_classification=True`` tool."""
    from moneybin.mcp.server import init_db, mcp

    init_db()
    mechanisms: dict[str, Mechanism] = {}
    for listed in await mcp.list_tools():
        tool = await mcp.get_tool(listed.name)
        assert isinstance(tool, FunctionTool), f"{listed.name} is not a FunctionTool"
        registered = tool.fn
        # `functools.wraps` copies the decorator's `_mcp_*` attributes out to
        # every wrapper, so the registered callable carries them directly.
        if not getattr(registered, "_mcp_dynamic_classification", False):
            continue
        callback = inspect.unwrap(registered)
        source = inspect.getsourcefile(callback)
        assert source is not None, f"{listed.name}: callback has no source file"
        mechanisms[listed.name] = mechanism_of(Path(source), callback.__name__)
    return mechanisms


async def test_every_dynamic_tool_reaches_a_sanctioned_mechanism() -> None:
    """A tool that neither builds nor declares has no classification at all."""
    mechanisms = await _dynamic_tool_mechanisms()
    assert mechanisms, "no dynamic-classification tools found; the guard is inert"

    # Deliberately not gated on SELF_DECLARING_TOOLS: a tool that declares by
    # hand is the sibling test's subject, listed or not, so keeping it out here
    # leaves each test with a fixture only it can catch.
    stranded = sorted(
        name for name, mechanism in mechanisms.items() if mechanism.reaches_no_mechanism
    )
    assert not stranded, (
        "These dynamic_classification=True tools reach neither "
        f"{BUILDER}() nor a declaration of their own, so summary.sensitivity "
        "and classes_returned are whatever build_envelope() defaulted to. "
        "Route them through the builder, or — if the payload genuinely cannot "
        "be classified from its type — declare the mechanism in "
        f"SELF_DECLARING_TOOLS: {stranded}"
    )


async def test_self_declaring_tools_are_exactly_the_sanctioned_set() -> None:
    """Set equality both ways: a new hand-declaration is a deliberate act."""
    mechanisms = await _dynamic_tool_mechanisms()
    actual = {name for name, m in mechanisms.items() if m.self_declares}

    unlisted = sorted(actual - set(SELF_DECLARING_TOOLS))
    assert not unlisted, (
        "These dynamic_classification=True tools set summary.sensitivity or "
        "classes_returned by hand instead of deriving both from the payload's "
        f"declared classes via {BUILDER}(). Route them through the builder, or "
        "add a SELF_DECLARING_TOOLS entry naming the mechanism they declare "
        "from: "
        + "; ".join(
            f"{name} — " + ", ".join(str(d) for d in mechanisms[name].declarations)
            for name in unlisted
        )
    )

    stale = sorted(set(SELF_DECLARING_TOOLS) - actual)
    assert not stale, (
        "SELF_DECLARING_TOOLS names tools that no longer declare a field by "
        "hand (or are no longer registered as dynamic). Drop the entries so "
        f"the exemption cannot cover a future one silently: {stale}"
    )


async def test_derived_declarations_are_not_quietly_replaced_by_literals() -> None:
    """The two derived mechanisms must keep deriving, not assert a constant."""
    mechanisms = await _dynamic_tool_mechanisms()
    assert MUST_DERIVE_ITS_DECLARATION <= set(SELF_DECLARING_TOOLS), (
        "MUST_DERIVE_ITS_DECLARATION names a tool that is not self-declaring"
    )

    asserted = sorted(
        str(declaration)
        for name in MUST_DERIVE_ITS_DECLARATION
        for declaration in mechanisms[name].declarations
        if declaration.literal
    )
    assert not asserted, (
        "These declarations are spelled out rather than derived from the "
        "mechanism SELF_DECLARING_TOOLS credits them with, which downgrades "
        "the field an agent reads before the payload without changing any "
        f"list this guard checks: {asserted}"
    )


def _probe_reaching_no_mechanism() -> ResponseEnvelope[Any]:
    """Stand-in for a tool that hand-builds and classifies nothing."""
    return build_envelope(data={"probe": True})


def _probe_declaring_its_own_fields() -> ResponseEnvelope[Any]:
    """Stand-in for a tool that declares both fields itself."""
    return build_envelope(
        data={"probe": True},
        sensitivity="low",
        classes_returned=["aggregate"],
    )


def test_declared_fields_still_name_real_envelope_fields() -> None:
    """A renamed field would leave the scan blind instead of loud."""
    envelope_fields = {field.name for field in dataclasses.fields(SummaryMeta)} | {
        field.name for field in dataclasses.fields(ResponseEnvelope)
    }
    missing = sorted(DECLARED_FIELDS - envelope_fields)
    assert not missing, (
        "DECLARED_FIELDS names something the envelope no longer carries, so the "
        f"scan would stop seeing hand-declarations of it: {missing}"
    )


def test_the_analysis_can_return_a_failing_verdict() -> None:
    """A guard that has never gone red is indistinguishable from an inert one."""
    stranded = mechanism_of(Path(__file__), _probe_reaching_no_mechanism.__name__)
    assert stranded.reaches_no_mechanism
    assert not stranded.self_declares

    declaring = mechanism_of(Path(__file__), _probe_declaring_its_own_fields.__name__)
    assert declaring.self_declares
    assert not declaring.reaches_builder
    assert [d.fields for d in declaring.declarations] == [
        ("classes_returned", "sensitivity")
    ]
    assert all(d.literal for d in declaring.declarations)
