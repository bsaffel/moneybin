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

Four boundaries, stated rather than implied:

- Reachability follows calls by name through ``src/moneybin/mcp``. The sibling
  guard scans `cli` too; this one does not, because an MCP tool's envelope is
  built on the MCP surface and `_mcp_module_path` resolves nothing outside it. A
  call it cannot resolve — a method, a callback passed as an argument, a helper
  in another package — ends that branch. Ending a branch is not the same as
  clearing it: a sibling branch reaching the builder must not vouch for one the
  scan never read. So every ``return`` in a function *annotated* as returning a
  ``ResponseEnvelope`` has to land on something this scan can name, and a tool
  whose envelope arrives through a call it cannot follow is listed in
  ``ENVELOPE_FROM_AN_UNFOLLOWABLE_CALL`` rather than passing silently.
- The literal check reads the expression as written, resolving a local alias
  back to its assignment first. It cannot tell a named constant reached through
  an attribute chain (``Sensitivity.LOW.value``) from a runtime derivation
  (``result.tier``), so it bounds the spelled-out case only. Say so rather than
  imply more: a guard that overstates its reach is the defect this file exists
  to fix.
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
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    SummaryMeta,
    build_envelope,
    build_error_envelope,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_ROOT = REPO_ROOT / "src" / "moneybin" / "mcp"

# The one builder that derives both declared fields from the payload contract.
# Taken from the symbol so a rename cannot leave the scan hunting a dead name.
BUILDER = build_classified_envelope.__name__

# Building an envelope through any of these is building it without the
# classifier, whatever the branch does with the result afterwards.
RAW_CONSTRUCTORS = frozenset({
    build_envelope.__name__,
    build_error_envelope.__name__,
    ResponseEnvelope.__name__,
})

# Passing either as a keyword IS the hand-declaration, whatever the callee is
# named — a constructor, or a decorator factory that stamps the finished
# envelope.
DECLARED_FIELDS = frozenset({"sensitivity", "classes_returned"})

# The same two fields, plus the section that carries one of them, as
# assignment targets: overwriting a finished envelope declares just as
# surely as passing a keyword, and passes no keyword while doing it.
MUTABLE_DECLARATION_TARGETS = DECLARED_FIELDS | {"summary"}

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

# Registered dynamic tools whose envelope arrives from a call this scan cannot
# follow, so the branch returning it is unread rather than approved.
ENVELOPE_FROM_AN_UNFOLLOWABLE_CALL: dict[str, str] = {
    "import_status": (
        "The `formats` section calls `inspect.unwrap(import_formats)` and "
        "invokes the result through a local `body` variable, so the producing "
        "function is reached by value rather than by name. It is the "
        "`import_formats` tool body in this same module, which the decorator "
        "classifies statically."
    ),
    "system_audit": (
        "`_run_tool_body` unwraps a tool callback passed as an argument and "
        "invokes it through `asyncio.to_thread`, so the producer is reached by "
        "value. The callbacks it receives are this module's own tool bodies, "
        "which the decorator classifies statically."
    ),
    "import_confirm": (
        "Its retry loop returns what `asyncio.to_thread(_run_import_confirm_"
        "attempt, …)` produced. The producer is passed as an argument, so the "
        "scan cannot follow it; `_run_import_confirm_attempt` lives in this "
        "same module and builds through the classifier."
    ),
}

# Of the tools above, the ones whose entry says the raw envelope is safe
# *because the registered tool rebuilds it through the classifier*. That
# justification is a condition, not a note: if one of them starts returning a
# helper's raw envelope directly, its reason has stopped being true and the
# list must not keep vouching for it. So these stay bound to the classifier;
# only a mechanism that genuinely replaces it is excused.
REBUILDS_THROUGH_THE_CLASSIFIER = frozenset({
    "import_preview",
    "gsheet_connect",
    "sync_status",
})

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
class UnverifiedReturn:
    """One envelope-typed return whose value this scan could not name."""

    module: str
    function: str
    expression: str

    def __str__(self) -> str:
        """Render the site the way the assertion message needs to name it."""
        return f"{self.module}:{self.function} returns {self.expression}"


@dataclass(frozen=True, slots=True)
class Declaration:
    """One call that sets a declared field by hand, and where it lives."""

    module: str
    function: str
    callee: str
    fields: tuple[str, ...]
    literal_fields: tuple[str, ...]
    verb: str = "calls"

    def __str__(self) -> str:
        """Render the site the way the assertion message needs to name it."""
        if self.verb == "calls":
            rendered = ", ".join(f"{field}=" for field in self.fields)
            site = f"{self.module}:{self.function} calls {self.callee}({rendered})"
        else:
            site = f"{self.module}:{self.function} {self.verb} {self.callee}"
        if not self.literal_fields:
            return site
        return f"{site} with {', '.join(self.literal_fields)} spelled out"


@dataclass(frozen=True, slots=True)
class Mechanism:
    """What a tool's call tree does about its envelope's declared fields."""

    reaches_builder: bool
    constructions: tuple[Construction, ...]
    declarations: tuple[Declaration, ...]
    unverified_returns: tuple[UnverifiedReturn, ...]

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
    """The callee's terminal name, receiver discarded — for labels only."""
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _bare_name(node: ast.Call) -> str | None:
    """The callee's name only when it is called as a bare name.

    A method keeps its receiver out of this deliberately: `adapter.
    build_classified_envelope()` shares a terminal name with the sanctioned
    builder and is a different function, so matching on the terminal name
    would let any object with the right method satisfy the requirement. The
    same applies in reverse to resolution — `self.helper()` is not this
    module's `helper()`. An attribute call is therefore never nameable here,
    which fails closed into the unverified-return check.
    """
    return node.func.id if isinstance(node.func, ast.Name) else None


type _Assignments = dict[str, list[ast.expr]]


def _resolve_all(
    node: ast.expr, assignments: _Assignments, seen: set[str] | None = None
) -> list[ast.expr]:
    """Every value a returned expression can hold, after unwrapping aliases.

    A name assigned on two branches has two reaching definitions, and keeping
    only the first would let a classified assignment vouch for an unresolvable
    sibling that merges into the same variable. So this fans out over all of
    them and the callers fail closed across the set.
    """
    seen = set() if seen is None else seen
    while True:
        if isinstance(node, ast.Await):
            node = node.value
        elif (
            isinstance(node, ast.Call)
            and _call_name(node) == "cast"
            and len(node.args) == 2
        ):
            node = node.args[1]
        else:
            break
    if isinstance(node, ast.Name) and node.id in assignments and node.id not in seen:
        seen.add(node.id)
        resolved = [
            candidate
            for value in assignments[node.id]
            for candidate in _resolve_all(value, assignments, seen)
        ]
        return resolved or [node]
    return [node]


def _spelled_out(node: ast.expr) -> bool:
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.List | ast.Tuple):
        return all(isinstance(element, ast.Constant) for element in node.elts)
    return False


def _is_literal(node: ast.expr, assignments: _Assignments) -> bool:
    """Whether an argument is spelled out rather than derived at runtime.

    True when *any* value the expression can hold is spelled out: one branch
    hardcoding the field is the defect, whatever the other branch does.
    """
    return any(_spelled_out(candidate) for candidate in _resolve_all(node, assignments))


def _mutation_field(node: ast.Call) -> str | None:
    """The declared field a call mutates in place, if it mutates one.

    Two shapes reach past a keyword scan and past assignment tracking, because
    neither is an `ast.Assign` and neither passes the field as a keyword:
    `setattr(envelope, "classes_returned", …)` names the field in a string,
    and `envelope.classes_returned.clear()` never names it as a target at all.
    """
    if _bare_name(node) == "setattr" and len(node.args) == 3:
        field = node.args[1]
        if isinstance(field, ast.Constant) and field.value in (
            MUTABLE_DECLARATION_TARGETS
        ):
            return str(field.value)
        return None
    receiver = node.func
    if isinstance(receiver, ast.Attribute) and isinstance(
        receiver.value, ast.Attribute
    ):
        if receiver.value.attr in DECLARED_FIELDS:
            return receiver.value.attr
    return None


def _declaration(
    node: ast.Call,
    *,
    module: str,
    function: str,
    assignments: _Assignments,
) -> Declaration | None:
    mutated = _mutation_field(node)
    if mutated is not None:
        return Declaration(
            module=module,
            function=function,
            callee=f"{ast.unparse(node)[:60]}",
            verb="mutates",
            fields=(mutated,),
            literal_fields=(mutated,),
        )
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
                str(keyword.arg)
                for keyword in declared
                if _is_literal(keyword.value, assignments)
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
    unverified: list[UnverifiedReturn] = []
    # The third element is "whatever this returns is the tool's envelope".
    # It is seeded True for the tool itself and propagated to any helper
    # returned from an envelope-producing one, so a producer annotated `Any`
    # is still held to the return check.
    seen: set[tuple[Path, str, bool]] = set()
    pending: list[tuple[Path, str, bool]] = [(path, function, True)]

    while pending:
        current_path, current_name, produces_envelope = pending.pop()
        if (current_path, current_name, produces_envelope) in seen:
            continue
        seen.add((current_path, current_name, produces_envelope))
        current_functions, imports = _index(current_path)
        node = current_functions.get(current_name)
        if node is None:
            continue

        assignments: _Assignments = {}
        for statement in node.body:
            for child in ast.walk(statement):
                if not isinstance(child, ast.Assign):
                    continue
                for target in child.targets:
                    if isinstance(target, ast.Name):
                        assignments.setdefault(target.id, []).append(child.value)
                    elif (
                        isinstance(target, ast.Attribute)
                        and target.attr in MUTABLE_DECLARATION_TARGETS
                    ):
                        # `ResponseEnvelope` and `SummaryMeta` are not frozen,
                        # so a tool can classify correctly and then overwrite
                        # what the builder derived. No keyword appears at such
                        # a site, which is why it needs its own arm.
                        declarations.append(
                            Declaration(
                                module=current_path.name,
                                function=current_name,
                                callee=ast.unparse(target),
                                verb="overwrites",
                                fields=(target.attr,),
                                literal_fields=(
                                    (target.attr,)
                                    if _is_literal(child.value, assignments)
                                    else ()
                                ),
                            )
                        )

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
                call,
                module=current_path.name,
                function=current_name,
                assignments=assignments,
            )
            if declaration is not None:
                declarations.append(declaration)

        for call in body_calls:
            callee = _bare_name(call)
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
                pending.append((current_path, callee, False))
            elif callee in imports:
                target_path, target_name = imports[callee]
                pending.append((target_path, target_name, False))

        # A branch the scan cannot read is not a branch it has cleared. Only
        # envelope-typed functions are held to this: every other helper in the
        # tree returns rows, cursors, and counts that no classifier owns.
        returns_an_envelope = produces_envelope or (
            node.returns is not None and "ResponseEnvelope" in ast.unparse(node.returns)
        )
        if returns_an_envelope:
            for statement in node.body:
                for child in ast.walk(statement):
                    if not isinstance(child, ast.Return) or child.value is None:
                        continue
                    # An imported name counts only if the module it points at
                    # actually defines it. Otherwise the traversal drops the
                    # call silently and this check would read that silence as
                    # a verified path.
                    resolvable_imports = {
                        local
                        for local, (source, original) in imports.items()
                        if original in _index(source)[0]
                    }
                    nameable = (
                        {BUILDER}
                        | RAW_CONSTRUCTORS
                        | set(current_functions)
                        | resolvable_imports
                    )
                    for value in _resolve_all(child.value, assignments):
                        callee = (
                            _bare_name(value) if isinstance(value, ast.Call) else None
                        )
                        if callee is not None and callee in nameable:
                            # Its value becomes this envelope, so hold it to the
                            # same check even if it is annotated `Any`.
                            if callee in current_functions:
                                pending.append((current_path, callee, True))
                            elif callee in imports:
                                helper_path, helper_name = imports[callee]
                                pending.append((helper_path, helper_name, True))
                            continue
                        unverified.append(
                            UnverifiedReturn(
                                module=current_path.name,
                                function=current_name,
                                expression=ast.unparse(value).splitlines()[0][:60],
                            )
                        )

    return Mechanism(
        reaches_builder=reaches_builder,
        constructions=tuple(dict.fromkeys(constructions)),
        declarations=tuple(dict.fromkeys(declarations)),
        unverified_returns=tuple(dict.fromkeys(unverified)),
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
    assert REBUILDS_THROUGH_THE_CLASSIFIER <= set(BUILDS_OUTSIDE_THE_CLASSIFIER), (
        "REBUILDS_THROUGH_THE_CLASSIFIER names a tool that is not on the list"
    )
    missing = sorted(
        name
        for name, mechanism in mechanisms.items()
        if not mechanism.reaches_builder
        and (
            name not in BUILDS_OUTSIDE_THE_CLASSIFIER
            or name in REBUILDS_THROUGH_THE_CLASSIFIER
        )
    )
    assert not missing, (
        f"These dynamic_classification=True tools never call {BUILDER}(), so "
        "nothing derives their declared fields from the payload's classes. A "
        "tool listed in BUILDS_OUTSIDE_THE_CLASSIFIER because it *rebuilds* a "
        "raw helper envelope has to keep rebuilding one; the list excuses only "
        f"a mechanism that replaces the classifier outright: {missing}"
    )


async def test_envelopes_from_an_unfollowable_call_are_the_sanctioned_set() -> None:
    """A branch the scan cannot read is listed, not vouched for by a sibling."""
    mechanisms = await _dynamic_tool_mechanisms()
    actual = {name for name, m in mechanisms.items() if m.unverified_returns}

    unlisted = sorted(actual - set(ENVELOPE_FROM_AN_UNFOLLOWABLE_CALL))
    assert not unlisted, (
        "These dynamic_classification=True tools return an envelope this scan "
        "cannot trace to a builder, a constructor, or a function it can follow "
        "— a method, or a callee reached by value rather than by name. Another "
        "branch reaching the builder says nothing about this one. Call the "
        "producer by name, or add an ENVELOPE_FROM_AN_UNFOLLOWABLE_CALL entry "
        "saying what builds it: "
        + "; ".join(
            f"{name} — "
            + ", ".join(str(site) for site in mechanisms[name].unverified_returns)
            for name in unlisted
        )
    )

    stale = sorted(set(ENVELOPE_FROM_AN_UNFOLLOWABLE_CALL) - actual)
    assert not stale, (
        "ENVELOPE_FROM_AN_UNFOLLOWABLE_CALL names tools whose envelope-typed "
        "returns are all traceable now (or that are no longer registered as "
        f"dynamic). Drop the entries: {stale}"
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

    # Dropping a field is the same downgrade as hardcoding it: the omitted one
    # takes the constructor's default, and a check that only reads the values
    # present would never see it go.
    # Dropping *both* keywords leaves no declaration to inspect at all, so the
    # checks below would have nothing to say while the constructor supplied
    # both defaults. The presence of a declaration is itself the contract.
    silent = sorted(
        name
        for name in MUST_DERIVE_ITS_DECLARATION
        if not mechanisms[name].declarations
    )
    assert not silent, (
        "These tools are credited with deriving their classification but no "
        "longer declare either field anywhere, so both now take the "
        f"constructor's default: {silent}"
    )

    partial = sorted(
        str(declaration)
        for name in MUST_DERIVE_ITS_DECLARATION
        for declaration in mechanisms[name].declarations
        if frozenset(declaration.fields) != DECLARED_FIELDS
    )
    assert not partial, (
        "These declarations set one of the two fields and leave the other to "
        "the constructor's default. A tool credited with deriving its "
        "classification declares both or neither: "
        f"{partial}"
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


def _probe_returning_through_an_unfollowable_call() -> ResponseEnvelope[Any]:
    """Stand-in for the shape a sibling builder call must not vouch for."""
    if _elsewhere():
        return build_classified_envelope({"probe": True})
    producer = _elsewhere()
    return producer()


def _probe_merging_two_branches_into_one_variable() -> ResponseEnvelope[Any]:
    """Stand-in for two reaching definitions merged into one returned name."""
    if _elsewhere():
        response = build_classified_envelope({"probe": True})
    else:
        response = _elsewhere().make_envelope()
    return response


def _unannotated_producer() -> Any:
    """Annotated `Any`, yet its value becomes the caller's envelope."""
    return _elsewhere().make_envelope()


def _probe_delegating_to_an_unannotated_helper() -> ResponseEnvelope[Any]:
    """Stand-in for a producer whose annotation does not name the envelope."""
    return _unannotated_producer()


class _Impostor:
    """Carries a method sharing the sanctioned builder's terminal name."""

    def build_classified_envelope(self) -> ResponseEnvelope[Any]:
        """Not the sanctioned builder, despite the name."""
        raise NotImplementedError


def _probe_calling_a_method_named_like_the_builder() -> ResponseEnvelope[Any]:
    """Stand-in for a receiver whose method shares the builder's name."""
    adapter = _Impostor()
    return adapter.build_classified_envelope()


def _probe_mutating_the_envelope_through_a_call() -> ResponseEnvelope[Any]:
    """Stand-in for a mutation that is neither a keyword nor an assignment."""
    envelope = build_classified_envelope({"probe": True})
    if _elsewhere() and envelope.classes_returned is not None:
        envelope.classes_returned.clear()
    else:
        setattr(envelope, "classes_returned", ["aggregate"])  # noqa: B010  # the adversarial shape under test
    return envelope


def _probe_hiding_a_literal_behind_an_alias() -> ResponseEnvelope[Any]:
    """Stand-in for a constant spelled into a local before the call."""
    hidden = "low"
    return build_envelope(data={"probe": True}, sensitivity=hidden)


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

    # The second round's finding: a resolvable branch must not clear a sibling
    # branch whose producer the scan reaches by value rather than by name.
    unfollowable = mechanism_of(
        Path(__file__), _probe_returning_through_an_unfollowable_call.__name__
    )
    assert unfollowable.reaches_builder
    assert [site.expression for site in unfollowable.unverified_returns] == [
        "producer()"
    ]

    # A constant put into a local first is still a constant.
    aliased = mechanism_of(
        Path(__file__), _probe_hiding_a_literal_behind_an_alias.__name__
    )
    assert [d.literal_fields for d in aliased.declarations] == [("sensitivity",)]

    # The third round's finding: two reaching definitions merged into one
    # returned name — the classified one must not vouch for its sibling.
    merged = mechanism_of(
        Path(__file__), _probe_merging_two_branches_into_one_variable.__name__
    )
    assert merged.reaches_builder
    assert [site.expression for site in merged.unverified_returns] == [
        "_elsewhere().make_envelope()"
    ]

    # A producer annotated `Any` is still a producer: the expectation follows
    # the value, not the annotation.
    delegated = mechanism_of(
        Path(__file__), _probe_delegating_to_an_unannotated_helper.__name__
    )
    assert [site.expression for site in delegated.unverified_returns] == [
        "_elsewhere().make_envelope()"
    ]

    # A method is not the sanctioned builder just because it ends in its name.
    impostor = mechanism_of(
        Path(__file__), _probe_calling_a_method_named_like_the_builder.__name__
    )
    assert not impostor.reaches_builder
    assert [site.expression for site in impostor.unverified_returns] == [
        "adapter.build_classified_envelope()"
    ]

    # Neither `setattr` nor an in-place method call is an assignment or a
    # keyword, so both would read past a scan that only looks at those.
    mutated = mechanism_of(
        Path(__file__), _probe_mutating_the_envelope_through_a_call.__name__
    )
    assert mutated.reaches_builder
    assert sorted(d.verb for d in mutated.declarations) == ["mutates", "mutates"]
    assert all(d.fields == ("classes_returned",) for d in mutated.declarations)
