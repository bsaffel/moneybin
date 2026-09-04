# ruff: noqa: S101
"""Structural guardrail: orchestrators compose services, not the other way round.

`moneybin.orchestration` sits one layer above `moneybin.services`. The refresh
pipeline lives there because it drives the matcher, SQLMesh apply, the
categorizer, the identity link services and the rate backfill — it is a
composition of the service layer, not a peer of it.

    CLI / MCP  →  moneybin.orchestration  →  moneybin.services  →  DuckDB

Two guards hold that shape, and they answer different questions. The source
scan says *nothing new* reaches upward — from a module top or from inside a
method body, because both execute and a call deferred to call time is a
dependency all the same. Deferral buys import-time cost, not layering
absolution. The behavioural test says the orchestrator's own deferred imports
are still deferred — a fact no source scan can establish, because a hoisted
import looks perfectly ordinary in the diff.

Two further tests guard the guards, because a scan can be weakened into
silence: one pins what the scan looks at, the other pins what it detects.
"""

from __future__ import annotations

import ast
import subprocess  # noqa: S404 — clean-interpreter import check
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Literal

from tests.moneybin.test_architecture._import_graph import package_of, resolved_module

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src" / "moneybin"

# Everything under src/moneybin is guarded except the layers at or above
# orchestration. Named as an exemption list rather than an inventory of guarded
# packages: `src/moneybin/` holds two dozen packages plus top-level modules, an
# enumeration of those goes stale the first time one is added, and a package
# missing from that enumeration is silently unguarded — the failure mode this
# file exists to prevent.
# `adapters` is here because an adapter reads what the pipeline produced — a
# `RefreshResult`, a connector model — and renders it as the response a surface
# returns. That is the orchestrator's consumer, not its peer's dependency, and
# it is shared by both transports rather than owned by either. The package
# holds no writes and no orchestration of its own.
LAYERS_AT_OR_ABOVE_ORCHESTRATION = frozenset({
    "adapters",
    "cli",
    "mcp",
    "orchestration",
})

# The below-orchestration packages that exist today, pinned so that widening
# the exemption list above cannot quietly shrink the scan. Subset-checked, not
# equality-checked, so adding a package does not fail this file — the point is
# that no package silently *leaves*.
GUARDED_PACKAGES: frozenset[str] = frozenset({
    "audits",
    "connectors",
    "db_lock",
    "exports",
    "extractors",
    "investments",
    "loaders",
    "logging",
    "matching",
    "metrics",
    "packages",
    "privacy",
    "protocol",
    "reports",
    "repositories",
    "services",
    "sql",
    "sqlmesh",
    "synthetic",
    "utils",
})

ImportKind = Literal["module", "deferred"]

# Every executing import of `moneybin.orchestration` from below the layer, as
# (path relative to src/moneybin/, kind) pairs — "module" for a module-top
# import, "deferred" for one written inside a function or method body. Each
# entry is a known inversion, not a pattern to copy: the service calls the
# orchestrator at the end of its own write, so the dependency points the wrong
# way whichever line it is written on.
#
# Keyed by (file, kind), not file alone: hoisting a deferred import to module
# level (or the reverse) is itself a layering-relevant change — module-level
# binds the name for `unittest.mock.patch` targets and pays the import cost
# eagerly, deferred does neither — so the two kinds must compare unequal, or
# a hoist would pass this guard silently. A file holding two upward imports of
# the *same* kind is still one entry; a file that legitimately holds one of
# each kind would be two entries, but none below does today.
#
# Draining this list means moving the closing refresh out of the service and
# into the surfaces that call it — a contract change for both CLI and MCP, so
# it is deliberately not part of the relocation that created this file.
KNOWN_INVERSIONS: frozenset[tuple[str, ImportKind]] = frozenset({
    # why: ImportService.import_files() closes a batch by running the full
    # refresh pipeline (`smart-import-transform.md` Req 3). Module-level
    # because `unittest.mock.patch` targets that predate this package bind the
    # rebound names on this module. Inverting it would change the import
    # contract on both surfaces.
    ("services/import_service.py", "module"),
    # why: SyncService.pull() runs refresh once after a sync that changed raw
    # state (`sync-plaid.md` Req 10). Same inversion, module-level for the same
    # patch targets.
    ("services/sync_service.py", "module"),
    # why: AccountLinksService.rematch_after_merge() re-runs the match and
    # transform steps once an account merge has committed, so the ledger the
    # user reads reflects the merged identity. Deferred into the method
    # because the orchestrator's identity step imports this service back: the
    # two modules name each other, so at least one of the pair has to stay off
    # its module top.
    ("services/account_links_service.py", "deferred"),
    # why: DemoService.run() drives generate → load → refresh → answer, and
    # refresh is one step of it. Deferred with the rest of that method's
    # import block, which defers as a unit: hoisting the block costs +311
    # modules over a bare `import demo_service` and pulls polars in through
    # `synthetic.writer`.
    ("services/demo_service.py", "deferred"),
    # why: InboxService.sync() imports `refresh` and `step_outcome` to run the
    # pipeline once at end-of-batch instead of once per file. Deferred as
    # forward cover, not as a saving today: `import_service` already pulls the
    # orchestrator onto the cold-start path at module level, so this deferral
    # only starts paying once that entry above is drained.
    ("services/inbox_service.py", "deferred"),
})

HEAVY_PREFIXES: tuple[str, ...] = ("fastmcp", "sqlmesh", "polars")


def _is_type_checking_test(test: ast.expr) -> bool:
    """True only for a bare ``TYPE_CHECKING`` / ``typing.TYPE_CHECKING`` test.

    Deliberately not a substring search over the unparsed test. ``if not
    TYPE_CHECKING:`` *does* execute at import time, and a search for the name
    would exempt precisely the block that runs — stating the rule backwards
    rather than merely missing a case.
    """
    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    return isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"


def _nested_blocks(stmt: ast.stmt) -> Iterator[list[ast.stmt]]:
    """Every statement list a compound statement can execute.

    ``match`` needs its own line: its bodies hang off ``cases[*].body`` through
    ``ast.match_case``, which is not an ``ast.stmt`` and carries none of the
    three block fields, so the generic sweep below cannot reach it.
    """
    for field in ("body", "orelse", "finalbody"):
        block = getattr(stmt, field, None)
        if isinstance(block, list):
            yield block
    for handler in getattr(stmt, "handlers", []):
        yield handler.body
    for case in getattr(stmt, "cases", []):
        yield case.body


def _runtime_imports(
    body: Sequence[ast.stmt], *, deferred: bool = False
) -> Iterator[tuple[ast.Import | ast.ImportFrom, ImportKind]]:
    """Imports that execute at some point when the program runs, tagged by kind.

    Recurses through every compound statement — ``if``/``else``, ``try``'s
    handlers, ``else`` and ``finally``, ``match`` cases, ``with``, ``for``,
    ``while``, class bodies — and through function and method bodies, because
    a pass over ``tree.body`` alone would let both ``try: ... except
    ImportError: <import>`` and the far commoner ``def f(): import …``
    through. A deferred import still runs; deferring it changes *when* the
    dependency is paid, never whether it exists — which is why both kinds are
    still yielded, just tagged differently.

    ``deferred`` tracks whether recursion has passed through a function or
    method body (``FunctionDef``/``AsyncFunctionDef``). Everything else that
    can wrap an import — ``if``, ``try``, ``class``, ``with``, ``for``,
    ``while``, ``match`` — runs at the moment its enclosing scope does, so it
    does not itself defer anything; only a callable body postpones execution
    to call time.

    One thing is excluded, because it genuinely never runs: the body of an
    ``if TYPE_CHECKING:`` block. That block's ``else`` branch is *not*
    excluded.

    A dynamic ``importlib.import_module(...)`` is invisible here, as it is to
    any AST import scan. That is a known floor, not an oversight.
    """
    for stmt in body:
        if isinstance(stmt, ast.Import | ast.ImportFrom):
            yield stmt, "deferred" if deferred else "module"
            continue
        if isinstance(stmt, ast.If) and _is_type_checking_test(stmt.test):
            yield from _runtime_imports(stmt.orelse, deferred=deferred)
            continue
        stmt_deferred = deferred or isinstance(
            stmt, ast.FunctionDef | ast.AsyncFunctionDef
        )
        for block in _nested_blocks(stmt):
            yield from _runtime_imports(block, deferred=stmt_deferred)


def _imports_orchestration(node: ast.Import | ast.ImportFrom, package: str) -> bool:
    if isinstance(node, ast.ImportFrom):
        module = resolved_module(node, package)
        if module == "moneybin.orchestration" or module.startswith(
            "moneybin.orchestration."
        ):
            return True
        # `from moneybin import orchestration` and its relative twin
        # `from .. import orchestration` both land here.
        return module == "moneybin" and any(
            alias.name == "orchestration" for alias in node.names
        )
    # `ast.Import` is never relative, so `package` does not apply.
    return any(
        alias.name == "moneybin.orchestration"
        or alias.name.startswith("moneybin.orchestration.")
        for alias in node.names
    )


def _guarded_files() -> Iterator[Path]:
    for path in sorted(SRC.rglob("*.py")):
        relative = path.relative_to(SRC)
        if relative.parts[0] in LAYERS_AT_OR_ABOVE_ORCHESTRATION:
            continue
        if "__pycache__" in relative.parts:
            continue
        yield path


def _scan() -> set[tuple[str, ImportKind]]:
    offenders: set[tuple[str, ImportKind]] = set()
    for path in _guarded_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        package = package_of(path)
        relative = path.relative_to(SRC).as_posix()
        for node, kind in _runtime_imports(tree.body):
            if _imports_orchestration(node, package):
                offenders.add((relative, kind))
    return offenders


def test_services_do_not_import_orchestration_at_runtime() -> None:
    """No new upward import; KNOWN_INVERSIONS is the whole exception set.

    Asserted as set equality rather than a subset so the list cannot rot in
    either direction: a new inversion fails, and so does an entry left behind
    after its inversion is fixed.
    """
    assert _scan() == KNOWN_INVERSIONS, (
        "moneybin.orchestration is imported from below the layer. Invert the "
        "call so the orchestrator drives the service. Deferring the import "
        "into a method body does not answer this — the method runs, so the "
        "dependency is real either way. Only add a KNOWN_INVERSIONS entry "
        "(with a `# why`) for a dependency that cannot be inverted without a "
        "contract change."
    )


def test_scan_still_looks_at_every_package_below_orchestration() -> None:
    """Pin what the scan reads, so narrowing it cannot pass as a clean run.

    Without this, widening `LAYERS_AT_OR_ABOVE_ORCHESTRATION` and fixing an
    inversion are indistinguishable: both turn the assertion above green. The
    checks are deliberately against a pinned snapshot rather than against a set
    derived from the exemption list — a derived set makes the assertion
    `exemptions ⊆ packages`, which is true however many packages the exemption
    list swallows.
    """
    guarded = set(_guarded_files())
    scanned_packages = {p.relative_to(SRC).parts[0] for p in guarded}

    assert GUARDED_PACKAGES <= scanned_packages, (
        "Packages below orchestration dropped out of the layering scan: "
        f"{sorted(GUARDED_PACKAGES - scanned_packages)}. A package moves out "
        "of this set only by moving to or above the orchestration layer."
    )

    # File-level narrowing, which a package-name comparison alone cannot see.
    expected_files = {
        path
        for path in SRC.rglob("*.py")
        if path.relative_to(SRC).parts[0] in GUARDED_PACKAGES
        and "__pycache__" not in path.relative_to(SRC).parts
    }
    assert expected_files <= guarded, (
        "Files inside guarded packages are being skipped: "
        f"{sorted(str(p.relative_to(SRC)) for p in expected_files - guarded)}"
    )

    all_packages = {
        path.relative_to(SRC).parts[0]
        for path in SRC.rglob("*.py")
        if "__pycache__" not in path.relative_to(SRC).parts
    }
    assert LAYERS_AT_OR_ABOVE_ORCHESTRATION <= all_packages, (
        "The exemption list names a directory that no longer exists: "
        f"{sorted(LAYERS_AT_OR_ABOVE_ORCHESTRATION - all_packages)}"
    )

    # The scan also reads each file's *package*, because a relative import is
    # measured from it. Pinned on four shapes — a top-level module, a package
    # `__init__`, a module inside a package, and a module one level deeper —
    # since a package resolved one level off silently sends `from
    # ..orchestration import refresh` to some other module and the scan then
    # reports nothing at all.
    expected_packages = {
        "observability.py": "moneybin",
        "services/__init__.py": "moneybin.services",
        "services/import_service.py": "moneybin.services",
        "services/categorization/applier.py": "moneybin.services.categorization",
    }
    scanned = {path.relative_to(SRC).as_posix() for path in guarded}
    assert set(expected_packages) <= scanned, (
        "The files this pins are no longer scanned, so the packages asserted "
        "below prove nothing: "
        f"{sorted(set(expected_packages) - scanned)}"
    )
    resolved_packages = {
        relative: package_of(SRC / relative) for relative in expected_packages
    }
    assert resolved_packages == expected_packages, (
        "A file is being read as the wrong package, which silently breaks "
        f"relative-import resolution: {resolved_packages}"
    )


def test_runtime_import_detection_sees_every_executing_block() -> None:
    """Pin what the scan detects, by shape.

    These are the forms a pass over `tree.body` alone misses. `if not
    TYPE_CHECKING:` is the sharp one: it executes, so a name search over the
    test expression would exempt the very block that must be caught. The
    function and method bodies are the commonest: an upward import written
    inside a method runs every time the method does, and reading only module
    tops leaves that dependency unenumerated.

    Sources here are parsed as if they sat in `moneybin.services`, so the
    relative forms below resolve the way they would in a guarded package.
    """
    package = "moneybin.services"
    executing = {
        "plain": "import moneybin.orchestration.refresh",
        "if-else": (
            "if FLAG:\n    pass\nelse:\n    import moneybin.orchestration.refresh"
        ),
        "try-except": (
            "try:\n    pass\nexcept ImportError:\n"
            "    import moneybin.orchestration.refresh"
        ),
        "try-star": (
            "try:\n    pass\nexcept* ValueError:\n"
            "    import moneybin.orchestration.refresh"
        ),
        "try-else": (
            "try:\n    pass\nexcept ImportError:\n    pass\nelse:\n"
            "    import moneybin.orchestration.refresh"
        ),
        "try-finally": (
            "try:\n    pass\nfinally:\n    import moneybin.orchestration.refresh"
        ),
        "not-type-checking": (
            "if not TYPE_CHECKING:\n    import moneybin.orchestration.refresh"
        ),
        "type-checking-else": (
            "if TYPE_CHECKING:\n    pass\nelse:\n"
            "    import moneybin.orchestration.refresh"
        ),
        "nested-if": "if A:\n    if B:\n        import moneybin.orchestration.refresh",
        "match-case": (
            "match value:\n    case 1:\n        import moneybin.orchestration.refresh"
        ),
        "match-wildcard": (
            "match value:\n    case _:\n        import moneybin.orchestration.refresh"
        ),
        "while-else": (
            "while cond:\n    pass\nelse:\n    import moneybin.orchestration.refresh"
        ),
        "with": "with ctx():\n    import moneybin.orchestration.refresh",
        "for": "for x in xs:\n    import moneybin.orchestration.refresh",
        "class-body": "class C:\n    import moneybin.orchestration.refresh",
        "from-moneybin": "from moneybin import orchestration",
        # Deferred into a callable: runs when the callable does, so it is a
        # dependency the enumeration has to carry.
        "function-body": "def f():\n    import moneybin.orchestration.refresh",
        "method-body": (
            "class C:\n    def f(self):\n        import moneybin.orchestration.refresh"
        ),
        "async-function-body": (
            "async def f():\n    import moneybin.orchestration.refresh"
        ),
        "nested-function-in-class-in-if": (
            "if A:\n    class C:\n        def f(self):\n"
            "            import moneybin.orchestration.refresh"
        ),
        "function-body-from-import": (
            "def f():\n    from moneybin.orchestration.refresh import refresh"
        ),
        # Relative forms. `node.module` holds "orchestration" for the first
        # and None for the second, so an unresolved comparison sees neither.
        "relative-parent": "from ..orchestration import refresh",
        "relative-parent-bare": "from .. import orchestration",
        "relative-parent-submodule": "from ..orchestration.refresh import refresh",
        "relative-in-function": ("def f():\n    from ..orchestration import refresh"),
    }
    # Every label whose import sits inside a function/method body — the shapes
    # `kind` must tag "deferred". Everything else in `executing` is "module".
    # Named explicitly, not inferred from the label string, so this list is a
    # second, independent assertion of intent rather than a restatement of the
    # source above.
    deferred_labels = frozenset({
        "function-body",
        "method-body",
        "async-function-body",
        "nested-function-in-class-in-if",
        "function-body-from-import",
        "relative-in-function",
    })
    for label, source in executing.items():
        found = list(_runtime_imports(ast.parse(source).body))
        matches = [(n, kind) for n, kind in found if _imports_orchestration(n, package)]
        assert matches, f"{label}: an import that runs went undetected"
        expected_kind: ImportKind = "deferred" if label in deferred_labels else "module"
        assert all(kind == expected_kind for _n, kind in matches), (
            f"{label}: expected kind {expected_kind!r}, got "
            f"{sorted({kind for _n, kind in matches})}"
        )

    inert = {
        "type-checking-body": (
            "if TYPE_CHECKING:\n    import moneybin.orchestration.refresh"
        ),
        "typing-qualified": (
            "if typing.TYPE_CHECKING:\n    import moneybin.orchestration.refresh"
        ),
        # A type-only import stays type-only wherever it is written.
        "type-checking-inside-function": (
            "def f():\n    if TYPE_CHECKING:\n"
            "        import moneybin.orchestration.refresh"
        ),
        "type-checking-relative": (
            "if TYPE_CHECKING:\n    from ..orchestration import refresh"
        ),
        # One dot from `moneybin.services` is `moneybin.services.orchestration`
        # — a different module that happens to share a name. Resolution has to
        # place the dots, not match the trailing text.
        "relative-sibling-package": "from . import orchestration",
        "relative-sibling-module": "from .orchestration import refresh",
        "relative-other-package": "from ..matching import engine",
    }
    for label, source in inert.items():
        found = list(_runtime_imports(ast.parse(source).body))
        assert not any(_imports_orchestration(n, package) for n, _kind in found), (
            f"{label}: an import that never reaches the orchestration layer "
            "was reported as an inversion"
        )

    # Depth is measured from the file's own package, not assumed. The same
    # source is an inversion from one package and not from another.
    deeper = "moneybin.services.categorization"
    assert any(
        _imports_orchestration(n, deeper)
        for n, _kind in _runtime_imports(
            ast.parse("from ...orchestration import x").body
        )
    ), "three dots from a second-level package should reach the layer"
    # More dots than the package has parts. Python refuses these outright, so
    # the scan must not resolve them into the layer. Two depths, because the
    # first invalid one zeroes the slice index and the next one drives it
    # negative — different arithmetic, same required answer.
    over_relative = {
        "one-dot-too-many": "from ...orchestration import x",
        "one-dot-too-many-bare": "from ... import orchestration",
        "two-dots-too-many-bare": "from .... import orchestration",
    }
    for label, source in over_relative.items():
        assert not any(
            _imports_orchestration(n, package)
            for n, _kind in _runtime_imports(ast.parse(source).body)
        ), f"{label}: an import Python would refuse was resolved into the layer"


def test_orchestrator_import_stays_light() -> None:
    """The refresh orchestrator must not pull polars, sqlmesh, or fastmcp.

    `import_service` imports this module at module level and the CLI's
    `import inbox` command imports `import_service`, so the orchestrator is on
    the cold-start path that `tests/moneybin/test_cli/test_cold_start.py`
    guards. Every step body therefore imports its own collaborators inside the
    function — `run_rate_backfill` and `GSheetPullService` both reach polars.

    Scoped to this module rather than to `cli.main` on purpose: the cold-start
    test already covers `cli.main`, and would keep passing if a future change
    dropped `import_service`'s import — leaving the deferrals here unguarded
    and free to be "cleaned up" into a regression the moment anything imports
    the orchestrator eagerly again.
    """
    prefixes_repr = repr(list(HEAVY_PREFIXES))
    snippet = (
        "import sys\n"
        "import moneybin.orchestration.refresh  # noqa: F401\n"
        f"prefixes = {prefixes_repr}\n"
        "loaded = sorted(\n"
        "    m for m in sys.modules\n"
        "    if any(m == p or m.startswith(p + '.') for p in prefixes)\n"
        ")\n"
        "print('LOADED:' + ','.join(loaded))\n"
    )
    result = subprocess.run(  # noqa: S603 — controlled snippet, not user input
        [sys.executable, "-c", snippet],
        capture_output=True,
        text=True,
        check=True,
    )
    loaded = result.stdout.strip().removeprefix("LOADED:")
    assert not loaded, (
        "moneybin.orchestration.refresh triggered eager import of heavy "
        f"modules: {loaded}. Move the import back inside the step function "
        "that needs it — see the module docstring for the measured cost."
    )
