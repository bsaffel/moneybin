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

Five boundaries, stated rather than implied:

- Reachability follows calls by name through the roots in ``REACHABLE_ROOTS``:
  ``src/moneybin/mcp``, plus the shared ``moneybin/adapters`` package several
  tools return their envelope from. The sibling guard scans `cli` too; this one
  does not, because an MCP tool's envelope is built on one of those two and
  `_reachable_module_path` resolves nothing outside them. A call it cannot
  resolve — a method, a callback passed as an argument, a helper in another
  package — ends that branch. Ending a branch is not the same as
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
- An unreadable ``**`` mapping is read as a declaration only where one
  could be stamped — an envelope constructor, the builder, or a decorator
  factory. ``**kwargs`` forwarding is ordinary plumbing in this tree and none
  of its call sites builds an envelope, so flagging every one would bury the
  shape this catches. A helper the traversal follows by name declares where it
  declares, and is read there.
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
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastmcp.tools import FunctionTool

from moneybin.privacy.classified_envelope import build_classified_envelope
from moneybin.protocol import envelope as envelope_module
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    SummaryMeta,
    build_envelope,
    build_error_envelope,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src" / "moneybin"

# Where reachability may follow a call. The MCP surface, plus the shared
# adapter package the envelope builders live in — a tool that returns
# `sync_status_envelope(...)` is reaching a builder, not ending its branch,
# and the scan has to be able to say which. Anything else ends the branch.
REACHABLE_ROOTS = ("moneybin.mcp", "moneybin.adapters")

# The one builder that derives both declared fields from the payload contract.
# Taken from the symbol so a rename cannot leave the scan hunting a dead name.
BUILDER = build_classified_envelope.__name__
# Where the real one lives. A module that spells the name without
# importing it from here is spelling something else.
BUILDER_MODULE = build_classified_envelope.__module__

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
    declares_fields: bool = True

    def __str__(self) -> str:
        """Render the site the way the assertion message needs to name it."""
        suffix = "" if self.declares_fields else " declaring neither field"
        return f"{self.module}:{self.function} builds {self.constructor}(){suffix}"


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

    @property
    def replaces_the_summary(self) -> bool:
        """Whether this rewrites the whole section rather than one field.

        `summary` carries `sensitivity`, so overwriting it declares — but it
        declares both derived fields at once, which is the opposite of the
        half-contract the partial check describes.
        """
        return "summary" in self.fields

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


def _own_statements(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.AST]:
    """Every node in the function's own body, stopping at a nested definition."""
    found: list[ast.AST] = []
    stack: list[ast.AST] = list(node.body)
    while stack:
        current = stack.pop()
        if isinstance(current, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            continue
        found.append(current)
        stack.extend(ast.iter_child_nodes(current))
    return found


def _bound_names(target: ast.expr) -> list[str]:
    """Every name a single assignment target binds, destructuring included."""
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, ast.Tuple | ast.List):
        return [name for element in target.elts for name in _bound_names(element)]
    if isinstance(target, ast.Starred):
        return _bound_names(target.value)
    return []


def _own_returns(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.Return]:
    """The function's own `return`s, not those of helpers nested inside it.

    `ast.walk` descends into a nested `def`, and an inner helper's return is
    not the enclosing tool's. Sweeping it in produces a spurious unverified
    path and would force a real tool onto an exemption list for a refactor
    that changed nothing about what it returns.
    """
    found: list[ast.Return] = []
    stack: list[ast.AST] = list(node.body)
    while stack:
        current = stack.pop()
        if isinstance(current, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            continue
        if isinstance(current, ast.Return):
            found.append(current)
        stack.extend(ast.iter_child_nodes(current))
    return found


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


type _Assignments = dict[str, list[tuple[int, ast.expr]]]


def _resolve_all(
    node: ast.expr,
    assignments: _Assignments,
    seen: set[str] | None = None,
    *,
    before: int | None = None,
    casts_are_typing: bool = False,
) -> list[ast.expr]:
    """Every value a returned expression can hold, after unwrapping aliases.

    A name assigned on two branches has two reaching definitions, and keeping
    only the first would let a classified assignment vouch for an unresolvable
    sibling that merges into the same variable. So this fans out over all of
    them and the callers fail closed across the set.

    ``cast`` is unwrapped only where the module imports it from ``typing``: a
    local function spelled `cast` is not the typing helper, and unwrapping it
    would read past whatever it actually does.

    ``before`` restricts the fan-out to assignments that can actually reach the
    expression. An early ``return response`` cannot see a rebinding written
    below it, and pooling every assignment regardless of position would let a
    later classified one vouch for the earlier return.
    """
    seen = set() if seen is None else seen
    while True:
        if isinstance(node, ast.Await):
            node = node.value
        elif (
            isinstance(node, ast.Call)
            and _bare_name(node) == "cast"
            and casts_are_typing
            and len(node.args) == 2
        ):
            node = node.args[1]
        else:
            break
    if isinstance(node, ast.Name) and node.id in assignments and node.id not in seen:
        seen.add(node.id)
        resolved = [
            candidate
            for line, value in assignments[node.id]
            if before is None or line < before
            for candidate in _resolve_all(
                value,
                assignments,
                seen,
                before=before,
                casts_are_typing=casts_are_typing,
            )
        ]
        return resolved or [node]
    return [node]


def _spelled_out(node: ast.expr) -> bool:
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.List | ast.Tuple):
        return all(isinstance(element, ast.Constant) for element in node.elts)
    return False


def _is_literal(
    node: ast.expr, assignments: _Assignments, *, casts_are_typing: bool
) -> bool:
    """Whether an argument is spelled out rather than derived at runtime.

    True when *any* value the expression can hold is spelled out: one branch
    hardcoding the field is the defect, whatever the other branch does.
    """
    return any(
        _spelled_out(candidate)
        for candidate in _resolve_all(
            node, assignments, casts_are_typing=casts_are_typing
        )
    )


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


def _expanded_keywords(node: ast.Call) -> tuple[list[tuple[str, ast.expr]], bool]:
    """Declared fields a ``**`` expansion carries, and whether one is unreadable.

    ``ast.keyword.arg`` is ``None`` for ``**mapping``, so a filter that matches
    on the keyword's name skips the argument entirely and reads a
    hand-declaration as its absence — ``adapter(**{"sensitivity": …})`` sets
    the field a keyword scan just cleared the call of. A literal mapping is
    read as the keywords it stands for; anything else is a mapping this scan
    cannot see into, and saying so is the fail-closed half of the answer.
    """
    resolved: list[tuple[str, ast.expr]] = []
    unreadable = False
    for keyword in node.keywords:
        if keyword.arg is not None:
            continue
        mapping = keyword.value
        if not isinstance(mapping, ast.Dict) or not all(
            isinstance(key, ast.Constant) and isinstance(key.value, str)
            for key in mapping.keys
        ):
            unreadable = True
            continue
        for key, value in zip(mapping.keys, mapping.values, strict=True):
            assert isinstance(key, ast.Constant)  # narrowed by the check above
            if key.value in DECLARED_FIELDS:
                resolved.append((str(key.value), value))
    return resolved, unreadable


def _can_stamp_a_declaration(node: ast.Call, *, is_decorator: bool) -> bool:
    """Whether an unreadable ``**`` mapping here could set a declared field.

    Scoped rather than universal, and the scope is the boundary the module
    docstring states: an envelope constructor, the builder, or a decorator
    factory that rewrites the finished envelope. Every other ``**kwargs``
    forward in this tree is helper plumbing the traversal follows by name, and
    what such a helper declares is recorded where it declares it.
    """
    return is_decorator or _call_name(node) in RAW_CONSTRUCTORS | {BUILDER}


def _declarations(
    node: ast.Call,
    *,
    module: str,
    function: str,
    assignments: _Assignments,
    casts_are_typing: bool,
    is_decorator: bool = False,
) -> list[Declaration]:
    mutated = _mutation_field(node)
    if mutated is not None:
        return [
            Declaration(
                module=module,
                function=function,
                callee=f"{ast.unparse(node)[:60]}",
                verb="mutates",
                fields=(mutated,),
                literal_fields=(mutated,),
            )
        ]
    expanded, unreadable = _expanded_keywords(node)
    declared: list[tuple[str, ast.expr]] = [
        (str(keyword.arg), keyword.value)
        for keyword in node.keywords
        if keyword.arg in DECLARED_FIELDS
    ] + expanded
    found: list[Declaration] = []
    if declared:
        found.append(
            Declaration(
                module=module,
                function=function,
                callee=_call_name(node) or "<computed>",
                fields=tuple(sorted({field for field, _ in declared})),
                literal_fields=tuple(
                    sorted({
                        field
                        for field, value in declared
                        if _is_literal(
                            value, assignments, casts_are_typing=casts_are_typing
                        )
                    })
                ),
            )
        )
    if unreadable and _can_stamp_a_declaration(node, is_decorator=is_decorator):
        found.append(
            Declaration(
                module=module,
                function=function,
                callee=_call_name(node) or "<computed>",
                verb="expands an unread mapping into",
                # Both fields, because the mapping can carry either and the
                # scan cannot say which. Naming one would read to the partial
                # check as a half-declaration, which is a different defect.
                fields=tuple(sorted(DECLARED_FIELDS)),
                literal_fields=(),
            )
        )
    return found


_ModuleIndex = tuple[
    dict[str, ast.FunctionDef | ast.AsyncFunctionDef],
    dict[str, tuple[Path, str]],
    bool,
    bool,
]
_INDEX_CACHE: dict[Path, _ModuleIndex] = {}


def _reachable_module_path(dotted: str) -> Path | None:
    """Resolve a dotted name inside a reachable root to the file defining it.

    Matched on a whole path segment rather than a string prefix, so a future
    ``moneybin.mcpx`` cannot be read as part of the MCP surface.
    """
    if not any(
        dotted == root or dotted.startswith(f"{root}.") for root in REACHABLE_ROOTS
    ):
        return None
    relative = dotted.removeprefix("moneybin").lstrip(".").replace(".", "/")
    for candidate in (SRC_ROOT / f"{relative}.py", SRC_ROOT / relative / "__init__.py"):
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
    # Module level only. An import nested in one function does not put the
    # builder in another function's scope, and pooling them module-wide is the
    # same "spelling is not identity" mistake one level out.
    imports_the_builder = any(
        isinstance(node, ast.ImportFrom)
        and not node.level
        and node.module == BUILDER_MODULE
        and any(alias.name == BUILDER and alias.asname is None for alias in node.names)
        for node in tree.body
    )
    # Module level only, for the same reason as the builder flag above: an
    # import inside one helper does not bind the name in another function.
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or node.level or node.module is None:
            continue
        source = _reachable_module_path(node.module)
        if source is None:
            continue
        for alias in node.names:
            imports[alias.asname or alias.name] = (source, alias.name)
    casts_are_typing = any(
        isinstance(node, ast.ImportFrom)
        and node.module == "typing"
        and any(alias.name == "cast" and alias.asname is None for alias in node.names)
        for node in tree.body
    )
    index = (functions, imports, imports_the_builder, casts_are_typing)
    _INDEX_CACHE[path] = index
    return index


def mechanism_of(path: Path, function: str) -> Mechanism:
    """Analyze what ``function`` in ``path`` does about its declared fields.

    Walks the call tree by name through ``src/moneybin/mcp``. A function's
    decorators are read for declarations but not followed: a decorator factory
    can stamp the finished envelope, while its own body belongs to whichever
    module defines it.
    """
    functions, _, _, _ = _index(path)
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
        (
            current_functions,
            module_imports,
            imports_the_builder,
            casts_are_typing,
        ) = _index(current_path)
        node = current_functions.get(current_name)
        if node is None:
            continue

        # Binding is scoped to the function's own statements. `ast.walk`
        # descends into a nested `def`, whose locals bind in that helper and
        # nowhere else, so pooling them let an unrelated closure's assignment
        # resolve the enclosing tool's `return` — crediting a path this scan
        # never read. `_own_returns` and `own_calls` already stop at a nested
        # definition; these two maps were the ones that did not.
        assignments: _Assignments = {}
        for child in _own_statements(node):
            if isinstance(child, ast.AnnAssign) and isinstance(child.target, ast.Name):
                # `name: Any = value` binds just as a plain assignment does;
                # skipping it left an annotated local shadow unseen.
                if child.value is not None:
                    assignments.setdefault(child.target.id, []).append((
                        child.lineno,
                        child.value,
                    ))
                continue
            if not isinstance(child, ast.Assign):
                continue
            for target in child.targets:
                if isinstance(target, ast.Name):
                    assignments.setdefault(target.id, []).append((
                        child.lineno,
                        child.value,
                    ))
                elif isinstance(target, ast.Tuple | ast.List | ast.Starred):
                    # Destructuring binds every name in the pattern. Which
                    # value each takes is not knowable here, and binding is
                    # all that shadowing needs.
                    for name in _bound_names(target):
                        assignments.setdefault(name, []).append((
                            child.lineno,
                            target,
                        ))

        # Declarations run the other way, on the same split `body_calls` and
        # `own_calls` use below: an overwrite inside a nested closure still
        # counts, because failing closed there means recording more.
        for statement in node.body:
            for child in ast.walk(statement):
                if isinstance(child, ast.AugAssign | ast.AnnAssign):
                    # `envelope.classes_returned += [...]` names no target the
                    # Assign arm sees and passes no keyword.
                    augmented = child.target
                    if (
                        isinstance(augmented, ast.Attribute)
                        and augmented.attr in MUTABLE_DECLARATION_TARGETS
                        and child.value is not None
                    ):
                        declarations.append(
                            Declaration(
                                module=current_path.name,
                                function=current_name,
                                callee=ast.unparse(augmented),
                                verb="augments",
                                fields=(augmented.attr,),
                                literal_fields=(augmented.attr,),
                            )
                        )
                    continue
                if not isinstance(child, ast.Assign):
                    continue
                for target in child.targets:
                    if (
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
                                    if _is_literal(
                                        child.value,
                                        assignments,
                                        casts_are_typing=casts_are_typing,
                                    )
                                    else ()
                                ),
                            )
                        )

        # A name bound inside the function is not the module-level function it
        # spells. A parameter called `build_classified_envelope` impersonates
        # the builder perfectly under a spelling match, which is the receiver
        # bug over again with the receiver omitted rather than discarded. A
        # function-local import is the real thing only when it *names* the real
        # thing: `from … import build_envelope as build_classified_envelope`
        # binds a different function under the builder's name, so the resolved
        # origin decides and the spelling does not. The non-aliased local
        # import of the genuine builder stays credited.
        imports = dict(module_imports)
        builder_name_rebound = False
        for statement in node.body:
            for child in ast.walk(statement):
                if (
                    not isinstance(child, ast.ImportFrom)
                    or child.level
                    or child.module is None
                ):
                    continue
                builder_name_rebound = builder_name_rebound or any(
                    (alias.asname or alias.name) == BUILDER
                    and (child.module, alias.name) != (BUILDER_MODULE, BUILDER)
                    for alias in child.names
                )
                source = _reachable_module_path(child.module)
                if source is None:
                    continue
                for alias in child.names:
                    imports[alias.asname or alias.name] = (source, alias.name)

        builder_in_scope = imports_the_builder or any(
            isinstance(child, ast.ImportFrom)
            and not child.level
            and child.module == BUILDER_MODULE
            and any(
                alias.name == BUILDER and alias.asname is None for alias in child.names
            )
            for statement in node.body
            for child in ast.walk(statement)
        )
        arguments = node.args
        shadowed = {
            argument.arg
            for argument in (
                *arguments.posonlyargs,
                *arguments.args,
                *arguments.kwonlyargs,
                *([arguments.vararg] if arguments.vararg else []),
                *([arguments.kwarg] if arguments.kwarg else []),
            )
        } | set(assignments)
        for child in _own_statements(node):
            target = None
            if isinstance(child, ast.For | ast.AsyncFor | ast.comprehension):
                target = child.target
            elif isinstance(child, ast.withitem):
                target = child.optional_vars
            if isinstance(target, ast.Name):
                shadowed.add(target.id)
        if builder_name_rebound:
            # Same treatment as a parameter of that name: never credited,
            # never traversed, and its returns fall to the unverified check.
            shadowed.add(BUILDER)

        # Two populations, and the direction decides which is used where.
        # `body_calls` is the liberal one: a construction or a declaration
        # inside a nested closure still counts, because failing closed there
        # means recording more. `own_calls` excludes nested definitions, and
        # is what may *credit* the builder or drive the traversal — a helper
        # nobody calls must not vouch for the tool that merely contains it.
        body_calls = [
            child
            for statement in node.body
            for child in ast.walk(statement)
            if isinstance(child, ast.Call)
        ]
        own_calls = [
            found for found in _own_statements(node) if isinstance(found, ast.Call)
        ]
        decorator_calls = [
            child
            for decorator in node.decorator_list
            for child in ast.walk(decorator)
            if isinstance(child, ast.Call)
        ]
        for calls, is_decorator in ((body_calls, False), (decorator_calls, True)):
            for call in calls:
                declarations.extend(
                    _declarations(
                        call,
                        module=current_path.name,
                        function=current_name,
                        assignments=assignments,
                        casts_are_typing=casts_are_typing and "cast" not in shadowed,
                        is_decorator=is_decorator,
                    )
                )

        for call in body_calls:
            callee = _bare_name(call)
            if callee is not None and callee in shadowed:
                continue
            qualified = _call_name(call)
            if (callee is None or callee in RAW_CONSTRUCTORS) and (
                qualified in RAW_CONSTRUCTORS
            ):
                # Direction matters: crediting the builder must be strict, so
                # a qualified call never counts as it. Flagging a raw build is
                # the opposite — failing closed means recording more, not
                # fewer — so `envelope_module.build_envelope(...)` counts here
                # even though the receiver is unread.
                constructions.append(
                    Construction(
                        module=current_path.name,
                        function=current_name,
                        constructor=qualified,
                        declares_fields=any(
                            keyword.arg in DECLARED_FIELDS for keyword in call.keywords
                        ),
                    )
                )
                continue

        for call in own_calls:
            callee = _bare_name(call)
            if callee is None or callee in shadowed:
                continue
            if callee == BUILDER:
                # String equality is not identity: a module that defines its
                # own `build_classified_envelope`, or spells the name without
                # importing the real one, is not calling the sanctioned
                # builder. Fail closed — the call then falls to the
                # unverified-return check like any other unknown callee.
                if builder_in_scope and BUILDER not in current_functions:
                    reaches_builder = True
                else:
                    continue
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
            for child in _own_returns(node):
                if child.value is None:
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
                    ({BUILDER} if builder_in_scope else set())
                    | RAW_CONSTRUCTORS
                    | set(current_functions)
                    | resolvable_imports
                )
                for value in _resolve_all(
                    child.value,
                    assignments,
                    before=child.lineno,
                    casts_are_typing=casts_are_typing and "cast" not in shadowed,
                ):
                    callee = _bare_name(value) if isinstance(value, ast.Call) else None
                    if (
                        callee is not None
                        and callee not in shadowed
                        and callee in nameable
                    ):
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

    # A new branch that builds an envelope and declares nothing takes both
    # defaults. Set equality cannot see it — the tool is already listed — and
    # the checks below read only the declarations that exist, so the branch
    # has to be caught where it is made.
    undeclared = sorted(
        str(construction)
        for name in MUST_DERIVE_ITS_DECLARATION
        for construction in mechanisms[name].constructions
        if not construction.declares_fields
    )
    assert not undeclared, (
        "These tools derive their classification, so every envelope they "
        "build outside the classifier has to carry it. These build one and "
        f"declare neither field, leaving both on the default: {undeclared}"
    )

    # `summary` carries `sensitivity`, so replacing the section wholesale
    # declares both derived fields at once. Reporting that through the partial
    # check below described it as setting "one of the two", which is the one
    # thing it never does.
    replaced = sorted(
        str(declaration)
        for name in MUST_DERIVE_ITS_DECLARATION
        for declaration in mechanisms[name].declarations
        if declaration.replaces_the_summary
    )
    assert not replaced, (
        "These replace the whole summary section, so both declared fields "
        "become whatever the new object carries rather than what the "
        f"mechanism derived: {replaced}"
    )

    partial = sorted(
        str(declaration)
        for name in MUST_DERIVE_ITS_DECLARATION
        for declaration in mechanisms[name].declarations
        if not declaration.replaces_the_summary
        and frozenset(declaration.fields) != DECLARED_FIELDS
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


def _probe_shadowing_the_builder(
    build_classified_envelope: Any,
) -> ResponseEnvelope[Any]:
    """Stand-in for a caller-supplied callable spelled like the builder."""
    return build_classified_envelope({"probe": True})


def _probe_declaring_only_one_field() -> ResponseEnvelope[Any]:
    """Stand-in for a call that sets one declared field and omits the other."""
    return build_envelope(data={"probe": True}, sensitivity=_elsewhere())


def _probe_augmenting_the_envelope() -> ResponseEnvelope[Any]:
    """Stand-in for `+=` against a declared field: no Assign, no keyword."""
    envelope = build_classified_envelope({"probe": True})
    if envelope.classes_returned is not None:
        envelope.classes_returned += ["aggregate"]
    return envelope


def _probe_with_a_nested_helper() -> ResponseEnvelope[Any]:
    """Clean tool whose local helper returns something unrelated."""

    def _rows() -> Any:
        return _elsewhere().fetch()

    _rows()
    return build_classified_envelope({"probe": True})


def _probe_returning_before_the_classified_assignment() -> ResponseEnvelope[Any]:
    """Stand-in for a return that a later rebinding cannot reach backwards."""
    response = _elsewhere().make_envelope()
    if _elsewhere():
        return response
    response = build_classified_envelope({"probe": True})
    return response


def _probe_building_through_a_qualified_constructor() -> ResponseEnvelope[Any]:
    """Stand-in for a raw build reached through its module."""
    return envelope_module.build_envelope(data={"probe": True})


def _probe_rebinding_the_builder_with_an_annotation() -> ResponseEnvelope[Any]:
    """Stand-in for an annotated local shadowing the builder."""
    build_classified_envelope: Any = _elsewhere()
    return build_classified_envelope({"probe": True})


def _probe_rebinding_the_builder_by_destructuring() -> ResponseEnvelope[Any]:
    """Stand-in for a destructured local shadowing the builder."""
    build_classified_envelope, _other = _elsewhere()
    return build_classified_envelope({"probe": True})


def _probe_with_a_dead_nested_builder_call() -> ResponseEnvelope[Any]:
    """Stand-in for a classifier call inside a helper nobody invokes."""

    def _never_called() -> ResponseEnvelope[Any]:
        return build_classified_envelope({"probe": True})

    _ = _never_called
    return _elsewhere().make_envelope()


def _probe_hiding_a_literal_behind_an_alias() -> ResponseEnvelope[Any]:
    """Stand-in for a constant spelled into a local before the call."""
    hidden = "low"
    return build_envelope(data={"probe": True}, sensitivity=hidden)


def _probe_importing_an_impostor_under_the_builders_name() -> ResponseEnvelope[Any]:
    """Stand-in for a local import aliasing another function to the name."""
    from moneybin.protocol.envelope import (
        build_envelope as build_classified_envelope,
    )

    return build_classified_envelope(data={"probe": True})


def _stamping_adapter(**declared: Any) -> Callable[[Any], Any]:
    """Stand-in for a decorator factory that stamps the finished envelope."""
    return lambda decorated: decorated


@_stamping_adapter(**{"sensitivity": "low"})
def _probe_declaring_through_an_expanded_decorator() -> ResponseEnvelope[Any]:
    """Stand-in for a declaration that rides in as a mapping, not a keyword."""
    return build_classified_envelope({"probe": True})


def _probe_expanding_an_unread_declaration() -> ResponseEnvelope[Any]:
    """Stand-in for a raw build whose keywords arrive through a mapping."""
    options: Any = _elsewhere()
    return build_envelope(data={"probe": True}, **options)


def _probe_resolving_a_parameter_through_a_nested_helper(
    response: Any,
) -> ResponseEnvelope[Any]:
    """Stand-in for a nested helper's local leaking into the enclosing scope."""

    def _classify() -> ResponseEnvelope[Any]:
        response = build_classified_envelope({"probe": True})
        return response

    _classify()
    return response


def _probe_binding_a_constructor_name_in_a_nested_helper() -> ResponseEnvelope[Any]:
    """Stand-in for a nested loop target wearing a raw constructor's name."""

    def _unused() -> None:
        for build_envelope in _elsewhere():  # noqa: F402  # the shadow under test
            _ = build_envelope

    _unused()
    return build_envelope(data={"probe": True})


def _probe_replacing_the_whole_summary() -> ResponseEnvelope[Any]:
    """Stand-in for overwriting the section that carries a declared field."""
    envelope = build_classified_envelope({"probe": True})
    envelope.summary = SummaryMeta(total_count=1, returned_count=1)
    return envelope


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


def test_the_builder_is_credited_by_origin_not_by_spelling(tmp_path: Path) -> None:
    """A module that spells the name without importing the real one is not it.

    Written to a scratch module rather than probed in-file: this file imports
    the genuine builder, so the not-imported case cannot be expressed here.
    """
    impostor = tmp_path / "impostor.py"
    impostor.write_text(
        "from typing import Any\n\n\n"
        "def probe() -> Any:\n"
        '    """Spells the sanctioned name, imports nothing."""\n'
        '    return build_classified_envelope({"probe": True})\n',
        encoding="utf-8",
    )
    spelled = mechanism_of(impostor, "probe")
    assert not spelled.reaches_builder
    assert [site.expression for site in spelled.unverified_returns] == [
        "build_classified_envelope({'probe': True})"
    ]

    genuine = tmp_path / "genuine.py"
    genuine.write_text(
        f"from {BUILDER_MODULE} import {BUILDER}\n"
        "from typing import Any\n\n\n"
        "def probe() -> Any:\n"
        '    """Imports the real builder."""\n'
        f'    return {BUILDER}({{"probe": True}})\n',
        encoding="utf-8",
    )
    assert mechanism_of(genuine, "probe").reaches_builder

    local = tmp_path / "local.py"
    local.write_text(
        "from typing import Any\n\n\n"
        "def probe() -> Any:\n"
        '    """Imports the real builder inside the function body."""\n'
        f"    from {BUILDER_MODULE} import {BUILDER}\n\n"
        f'    return {BUILDER}({{"probe": True}})\n',
        encoding="utf-8",
    )
    assert mechanism_of(local, "probe").reaches_builder


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

    # A parameter spelled like the builder is the receiver bug with the
    # receiver left out rather than discarded: same spelling, different
    # function, and nothing about it is the sanctioned mechanism.
    shadowed = mechanism_of(Path(__file__), _probe_shadowing_the_builder.__name__)
    assert not shadowed.reaches_builder
    assert [site.expression for site in shadowed.unverified_returns] == [
        "build_classified_envelope({'probe': True})"
    ]

    # `+=` against a declared field is neither an assignment the overwrite arm
    # sees nor a keyword, so it needs its own arm and its own probe.
    augmented = mechanism_of(Path(__file__), _probe_augmenting_the_envelope.__name__)
    assert augmented.reaches_builder
    assert [d.verb for d in augmented.declarations] == ["augments"]
    assert [d.fields for d in augmented.declarations] == [("classes_returned",)]

    # A nested helper's return is not the tool's. Sweeping it in would force a
    # clean tool onto an exemption list for a refactor that changed nothing
    # about what it returns.
    nested = mechanism_of(Path(__file__), _probe_with_a_nested_helper.__name__)
    assert nested.reaches_builder
    assert nested.unverified_returns == ()

    # An early return cannot see a rebinding written below it. Pooling every
    # assignment regardless of position let the later classified one vouch for
    # the earlier return.
    early = mechanism_of(
        Path(__file__), _probe_returning_before_the_classified_assignment.__name__
    )
    assert early.reaches_builder
    assert [site.expression for site in early.unverified_returns] == [
        "_elsewhere().make_envelope()"
    ]

    # Crediting the builder is strict about receivers; flagging a raw build is
    # the opposite, so a qualified constructor still counts.
    qualified = mechanism_of(
        Path(__file__), _probe_building_through_a_qualified_constructor.__name__
    )
    assert [site.constructor for site in qualified.constructions] == [
        build_envelope.__name__
    ]

    # An annotated or destructured local shadows the builder exactly as a
    # parameter does; both were invisible while only plain assignments and
    # parameters were recorded.
    for probe in (
        _probe_rebinding_the_builder_with_an_annotation,
        _probe_rebinding_the_builder_by_destructuring,
    ):
        rebound = mechanism_of(Path(__file__), probe.__name__)
        assert not rebound.reaches_builder, probe.__name__
        assert rebound.unverified_returns, probe.__name__

    # A classifier call inside a helper nobody invokes is not a path the tool
    # takes, so it must not credit the tool that merely contains it.
    dead = mechanism_of(Path(__file__), _probe_with_a_dead_nested_builder_call.__name__)
    assert not dead.reaches_builder
    assert [site.expression for site in dead.unverified_returns] == [
        "_elsewhere().make_envelope()"
    ]

    # A local import is the sanctioned builder only when it names the
    # sanctioned builder. Aliasing another function to that spelling is the
    # parameter shadow with an import statement in front of it.
    impostor_import = mechanism_of(
        Path(__file__), _probe_importing_an_impostor_under_the_builders_name.__name__
    )
    assert not impostor_import.reaches_builder
    assert [site.expression for site in impostor_import.unverified_returns] == [
        "build_classified_envelope(data={'probe': True})"
    ]

    # `**mapping` carries no keyword name, so a filter matching on the name
    # reads a hand-declaration as its absence. A literal mapping is read as
    # the keywords it stands for; an unreadable one is recorded as unread.
    stamped = mechanism_of(
        Path(__file__), _probe_declaring_through_an_expanded_decorator.__name__
    )
    assert stamped.reaches_builder
    assert [d.fields for d in stamped.declarations] == [("sensitivity",)]
    assert [d.literal_fields for d in stamped.declarations] == [("sensitivity",)]

    unread = mechanism_of(
        Path(__file__), _probe_expanding_an_unread_declaration.__name__
    )
    assert [d.verb for d in unread.declarations] == ["expands an unread mapping into"]
    assert [d.fields for d in unread.declarations] == [
        ("classes_returned", "sensitivity")
    ]

    # A nested helper's locals bind in that helper. Pooling them let an
    # unrelated assignment resolve the enclosing tool's return.
    nested_local = mechanism_of(
        Path(__file__), _probe_resolving_a_parameter_through_a_nested_helper.__name__
    )
    assert not nested_local.reaches_builder
    assert [site.expression for site in nested_local.unverified_returns] == ["response"]

    # A loop target inside a nested helper binds there, not here. Pooling it
    # into `shadowed` suppressed an enclosing raw build — the same defect as
    # the assignment map's, in the fail-open direction.
    nested_binding = mechanism_of(
        Path(__file__),
        _probe_binding_a_constructor_name_in_a_nested_helper.__name__,
    )
    assert [site.constructor for site in nested_binding.constructions] == [
        build_envelope.__name__
    ]
    assert nested_binding.unverified_returns == ()

    # Replacing the section that carries `sensitivity` declares both fields at
    # once, which is not the half-contract the partial check describes.
    replaced = mechanism_of(Path(__file__), _probe_replacing_the_whole_summary.__name__)
    assert [d.fields for d in replaced.declarations] == [("summary",)]
    assert all(d.replaces_the_summary for d in replaced.declarations)

    # Omitting one field is the half-contract case the `partial` check reads.
    one_field = mechanism_of(Path(__file__), _probe_declaring_only_one_field.__name__)
    assert [d.fields for d in one_field.declarations] == [("sensitivity",)]
    assert [d.literal_fields for d in one_field.declarations] == [()]
