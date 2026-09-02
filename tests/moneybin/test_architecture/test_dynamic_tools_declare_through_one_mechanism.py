"""Every dynamically classified tool declares through a sanctioned mechanism.

`test_classified_envelope_is_the_only_pipeline.py` catches a *re-pasted*
primitive: a module under `src/moneybin/{mcp,cli}` that calls
`extract_data_classes`, `derive_tier`, or `redact_typed` itself. It cannot see
the other shape — a ``dynamic_classification=True`` tool that hand-builds its
envelope and calls none of the three. Such a tool trips no source scan while
setting its own ``summary.sensitivity`` and ``classes_returned``: the two fields
an agent reads before it ever sees the payload, and the two that land in the
privacy audit row.

So this guard starts from the decorator's registry rather than from source
calls, and asks two things of every registered dynamic tool.

**It must reach ``build_classified_envelope``**, which derives both fields from
the payload's declared classes.

**Its call tree must not build an envelope any other way.** Not "must build one
correctly somewhere" — a tool whose success branch classifies properly and whose
second branch returns a bare ``build_envelope(data=…)`` ships that branch with
whatever the constructor defaults, and one builder call elsewhere is not
coverage for it. So *any* reachable `build_envelope` / `build_error_envelope` /
`ResponseEnvelope(...)` counts, and so does passing ``sensitivity=`` or
``classes_returned=`` to any callee at all — including a decorator factory that
stamps a finished envelope. Naming the fields and the constructors, rather than
one blessed call site, is what stops the rule from buying exactly one round
against the next shape.

A tool that does either is listed in ``BUILDS_OUTSIDE_THE_CLASSIFIER`` with the
mechanism it uses instead. The list is asserted by set equality in both
directions, so a seventh such tool fails until someone adds it deliberately, and
an entry that stops applying fails until someone removes it.

Three boundaries, stated rather than implied:

- Reachability follows calls by name through ``src/moneybin/mcp``. The sibling
  guard scans `cli` too; this one does not, because an MCP tool's envelope is
  built on the MCP surface and `_mcp_module_path` resolves nothing outside it. A
  call it cannot resolve (a method, a callback passed as an argument, a helper
  in another package) ends that branch, so a tool that moves its envelope
  building out of `mcp/` reads as reaching nothing and fails. That is the
  intended direction: a false alarm is a review, a missed hand-declaration is an
  unmasked payload.
- The unit is the tool, not the call site. A finer (module, function) list would
  pin each branch, but these files are refactored often enough that the churn
  would land on unrelated PRs; the tool is what an agent calls and what the
  registry names.
- Where the construction sits does not excuse it. A raw envelope built inside a
  helper counts even where the tool later rebuilds through the classifier,
  because that arrangement is one refactor away from returning the helper's
  envelope instead. Three entries below are that shape, and each records why the
  unclassified value does not reach the wire today.
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

# Building an envelope through any of these is building it without the
# classifier, whatever the branch does with the result afterwards.
RAW_CONSTRUCTORS = frozenset({
    build_envelope.__name__,
    "build_error_envelope",
    ResponseEnvelope.__name__,
})

# Passing either as a keyword IS the hand-declaration, whatever the callee is
# named — a constructor, or a decorator factory that stamps the finished
# envelope.
DECLARED_FIELDS = frozenset({"sensitivity", "classes_returned"})

# Registered dynamic tools whose call tree builds an envelope outside the
# classifier or sets a declared field by hand, and the mechanism each uses.
BUILDS_OUTSIDE_THE_CLASSIFIER: dict[str, str] = {
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
        "typed contract, so there is nothing for the classifier to read."
    ),
    "import_preview": (
        "Its preview helpers build and declare their own envelopes — the PDF "
        "confirmation branches on a bare dict, the tabular branch beside a "
        "typed payload. The registered tool consumes those as intermediate "
        "values and returns, at its single exit, one the classifier produced "
        "from the payload it assembles."
    ),
    "gsheet_connect": (
        "Its internal helpers build raw envelopes and carry "
        "`@internal_envelope_adapter(sensitivity=…)`, which stamps a static "
        "sensitivity on what they return. The registered tool rebuilds all "
        "three success branches through the classifier and passes a helper "
        "envelope through only when it carries an error."
    ),
    "sync_status": (
        "`sync_status` and `sync_link_status` build raw envelopes that declare "
        "nothing, so they take the constructor's `low` default. The registered "
        "coarse tool reads their payloads apart and returns, at its single "
        "exit, one the classifier built — the defaulted envelopes are never "
        "the returned value."
    ),
}

# Where the sanctioned mechanism is a *derivation*, a literal is the silent
# downgrade set equality cannot see: swapping `result.tier` for "low" keeps the
# tool on this list while turning its declaration into an assertion. Tracked per
# field, because hardcoding one of the two while the other stays derived is the
# same defect on half the contract.
MUST_DERIVE_ITS_DECLARATION = frozenset({"reports", "sql_query"})


@dataclass(frozen=True, slots=True)
class Construction:
    """One envelope built without the classifier, and where it lives."""

    module: str
    function: str
    constructor: str

    def __str__(self) -> str:
        """Render the site the way the assertion message needs to name it."""
        return f"{self.module}:{self.function} builds {self.constructor}()"


@dataclass(frozen=True, slots=True)
class Declaration:
    """One call that sets a declared field by hand, and where it lives."""

    module: str
    function: str
    callee: str
    fields: tuple[str, ...]
    literal_fields: tuple[str, ...]

    def __str__(self) -> str:
        """Render the site the way the assertion message needs to name it."""
        rendered = ", ".join(f"{field}=" for field in self.fields)
        site = f"{self.module}:{self.function} calls {self.callee}({rendered})"
        if not self.literal_fields:
            return site
        return f"{site} with {', '.join(self.literal_fields)} spelled out"


@dataclass(frozen=True, slots=True)
class Mechanism:
    """What a tool's call tree does about its envelope's declared fields."""

    reaches_builder: bool
    constructions: tuple[Construction, ...]
    declarations: tuple[Declaration, ...]

    @property
    def builds_outside_the_classifier(self) -> bool:
        return bool(self.constructions or self.declarations)

    @property
    def reaches_nothing(self) -> bool:
        """No classifier call, no construction, no declaration anywhere."""
        return not self.reaches_builder and not self.builds_outside_the_classifier

    def sites(self) -> list[str]:
        return sorted(
            [str(site) for site in self.constructions]
            + [str(site) for site in self.declarations]
        )


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
    declared = [keyword for keyword in node.keywords if keyword.arg in DECLARED_FIELDS]
    if not declared:
        return None
    return Declaration(
        module=module,
        function=function,
        callee=_call_name(node) or "<computed>",
        fields=tuple(sorted(str(keyword.arg) for keyword in declared)),
        literal_fields=tuple(
            sorted(
                str(keyword.arg) for keyword in declared if _is_literal(keyword.value)
            )
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
    """Return a module's top-level functions and its ``moneybin.mcp`` imports.

    Only module-level ``def``s are indexed. Keying every nested and method
    definition by bare name would silently resolve a call to whichever
    same-named body happened to be walked last, and this tree already carries
    such collisions. A nested helper is not lost by the restriction: it sits
    inside its enclosing function's body, which the caller walks whole.
    """
    cached = _INDEX_CACHE.get(path)
    if cached is not None:
        return cached
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions = {
        node.name: node
        for node in tree.body
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
        f"{path.name} does not define a module-level {function}(); the registry "
        "and the source disagree, so the guard cannot tell which mechanism the "
        "tool uses."
    )

    reaches_builder = False
    constructions: list[Construction] = []
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
            elif callee in RAW_CONSTRUCTORS:
                constructions.append(
                    Construction(
                        module=current_path.name,
                        function=current_name,
                        constructor=callee,
                    )
                )
            if callee in current_functions:
                pending.append((current_path, callee))
            elif callee in imports:
                pending.append(imports[callee])

    return Mechanism(
        reaches_builder=reaches_builder,
        constructions=tuple(dict.fromkeys(constructions)),
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


async def test_every_dynamic_tool_reaches_a_classification_mechanism() -> None:
    """A tool whose tree resolves to nothing has no classification at all."""
    mechanisms = await _dynamic_tool_mechanisms()
    assert mechanisms, "no dynamic-classification tools found; the guard is inert"

    # Deliberately not gated on the exemption list: a tool that builds an
    # envelope outside the classifier is the sibling test's subject, listed or
    # not, so keeping it out here leaves each test a fixture only it can catch.
    stranded = sorted(
        name for name, mechanism in mechanisms.items() if mechanism.reaches_nothing
    )
    assert not stranded, (
        "These dynamic_classification=True tools build no envelope this guard "
        f"can see — no {BUILDER}(), no constructor, no declaration — so their "
        "summary.sensitivity and classes_returned come from somewhere it cannot "
        "check. Build the envelope on the MCP surface through the builder, or "
        f"say in the module docstring why the tree ends where it does: {stranded}"
    )


async def test_envelopes_built_outside_the_classifier_are_the_sanctioned_set() -> None:
    """Set equality both ways: a new unclassified build is a deliberate act."""
    mechanisms = await _dynamic_tool_mechanisms()
    actual = {name for name, m in mechanisms.items() if m.builds_outside_the_classifier}

    unlisted = sorted(actual - set(BUILDS_OUTSIDE_THE_CLASSIFIER))
    assert not unlisted, (
        "These dynamic_classification=True tools build an envelope without "
        f"{BUILDER}(), or set summary.sensitivity / classes_returned by hand, "
        "somewhere in their call tree. One correct branch elsewhere does not "
        "cover the branch that does it. Route them through the builder, or add "
        "a BUILDS_OUTSIDE_THE_CLASSIFIER entry naming the mechanism: "
        + "; ".join(
            f"{name} — " + ", ".join(mechanisms[name].sites()) for name in unlisted
        )
    )

    stale = sorted(set(BUILDS_OUTSIDE_THE_CLASSIFIER) - actual)
    assert not stale, (
        "BUILDS_OUTSIDE_THE_CLASSIFIER names tools that no longer build an "
        "envelope outside the classifier (or are no longer registered as "
        "dynamic). Drop the entries so the exemption cannot cover a future one "
        f"silently: {stale}"
    )


async def test_every_dynamic_tool_reaches_the_classifier_or_is_listed() -> None:
    """A tool declaring from another mechanism is on the list; the rest build."""
    mechanisms = await _dynamic_tool_mechanisms()
    missing = sorted(
        name
        for name, mechanism in mechanisms.items()
        if not mechanism.reaches_builder and name not in BUILDS_OUTSIDE_THE_CLASSIFIER
    )
    assert not missing, (
        f"These dynamic_classification=True tools never call {BUILDER}(), so "
        "nothing derives their declared fields from the payload's classes: "
        f"{missing}"
    )


async def test_derived_declarations_are_not_quietly_replaced_by_literals() -> None:
    """The two derived mechanisms must keep deriving — per field, not in bulk."""
    mechanisms = await _dynamic_tool_mechanisms()
    assert MUST_DERIVE_ITS_DECLARATION <= set(BUILDS_OUTSIDE_THE_CLASSIFIER), (
        "MUST_DERIVE_ITS_DECLARATION names a tool that is not on the list"
    )

    asserted = sorted(
        str(declaration)
        for name in MUST_DERIVE_ITS_DECLARATION
        for declaration in mechanisms[name].declarations
        if declaration.literal_fields
    )
    assert not asserted, (
        "These fields are spelled out rather than derived from the mechanism "
        "BUILDS_OUTSIDE_THE_CLASSIFIER credits the tool with, which downgrades "
        "what an agent reads before the payload without changing any list this "
        f"guard checks: {asserted}"
    )


def _probe_reaching_nothing() -> ResponseEnvelope[Any]:
    """Stand-in for a tool whose envelope comes from outside the scanned tree."""
    return _elsewhere()


def _elsewhere() -> Any:
    """Unresolvable to the scan on purpose — it builds no envelope itself."""
    raise NotImplementedError


def _probe_building_outside_the_classifier() -> ResponseEnvelope[Any]:
    """Stand-in for a branch that builds a raw envelope and declares nothing."""
    if _elsewhere():
        return build_classified_envelope({"probe": True})
    return build_envelope(data={"probe": True})


def _probe_declaring_one_field_by_hand() -> ResponseEnvelope[Any]:
    """Stand-in for a partial swap: one field derived, the other spelled out."""
    return build_envelope(
        data={"probe": True},
        sensitivity=_elsewhere(),
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
    stranded = mechanism_of(Path(__file__), _probe_reaching_nothing.__name__)
    assert stranded.reaches_nothing

    # The finding both reviewers raised on the first cut: one correct branch
    # must not cover a sibling branch that builds the envelope raw.
    mixed = mechanism_of(
        Path(__file__), _probe_building_outside_the_classifier.__name__
    )
    assert mixed.reaches_builder
    assert mixed.builds_outside_the_classifier
    assert not mixed.reaches_nothing
    assert [site.constructor for site in mixed.constructions] == [
        build_envelope.__name__
    ]

    # Literalness is tracked per field, so hardcoding one of the two is caught
    # while the other stays derived.
    partial = mechanism_of(Path(__file__), _probe_declaring_one_field_by_hand.__name__)
    assert [d.fields for d in partial.declarations] == [
        ("classes_returned", "sensitivity")
    ]
    assert [d.literal_fields for d in partial.declarations] == [("classes_returned",)]
