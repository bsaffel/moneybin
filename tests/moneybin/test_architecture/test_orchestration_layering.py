# ruff: noqa: S101
"""Structural guardrail: orchestrators compose services, not the other way round.

`moneybin.orchestration` sits one layer above `moneybin.services`. The refresh
pipeline lives there because it drives the matcher, SQLMesh apply, the
categorizer, the identity link services and the rate backfill — it is a
composition of the service layer, not a peer of it.

    CLI / MCP  →  moneybin.orchestration  →  moneybin.services  →  DuckDB

Two guards hold that shape, and they answer different questions. The source
scan says *nothing new* reaches upward. The behavioural test says the
orchestrator's own deferred imports are still deferred — a fact no source scan
can establish, because a hoisted import looks perfectly ordinary in the diff.

Two further tests guard the guards, because a scan can be weakened into
silence: one pins what the scan looks at, the other pins what it detects.
"""

from __future__ import annotations

import ast
import subprocess  # noqa: S404 — clean-interpreter import check
import sys
from collections.abc import Iterator, Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src" / "moneybin"

# Everything under src/moneybin is guarded except the layers at or above
# orchestration. Named as an exemption list rather than an inventory of guarded
# packages: `src/moneybin/` holds two dozen packages plus top-level modules, an
# enumeration of those goes stale the first time one is added, and a package
# missing from that enumeration is silently unguarded — the failure mode this
# file exists to prevent.
LAYERS_AT_OR_ABOVE_ORCHESTRATION = frozenset({"cli", "mcp", "orchestration"})

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

# Module-level imports of `moneybin.orchestration` from below the layer, as
# paths relative to src/moneybin/. Each entry is a known inversion, not a
# pattern to copy: the service calls the orchestrator at the end of its own
# write, so the dependency points the wrong way and the import has to be
# module-level for `unittest.mock.patch` targets that predate this package.
#
# Draining this list means moving the closing refresh out of the service and
# into the surfaces that call it — a contract change for both CLI and MCP, so
# it is deliberately not part of the relocation that created this file.
KNOWN_INVERSIONS: frozenset[str] = frozenset({
    # why: ImportService.import_files() closes a batch by running the full
    # refresh pipeline (`smart-import-transform.md` Req 3). Inverting it would
    # change the import contract on both surfaces.
    "services/import_service.py",
    # why: SyncService.pull() runs refresh once after a sync that changed raw
    # state (`sync-plaid.md` Req 10). Same inversion, same reason.
    "services/sync_service.py",
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


def _runtime_imports(body: Sequence[ast.stmt]) -> Iterator[ast.Import | ast.ImportFrom]:
    """Imports that execute when the module is imported.

    Recurses through every compound statement — ``if``/``else``, ``try``'s
    handlers, ``else`` and ``finally``, ``match`` cases, ``with``, ``for``,
    ``while``, class bodies — because each runs at import time and a pass over
    ``tree.body`` alone would let ``try: ... except ImportError: <import>``
    through.

    Two things are excluded, both because they do not run: function and method
    bodies, where deferring is the sanctioned way to call upward, and the body
    of an ``if TYPE_CHECKING:`` block. That block's ``else`` branch is *not*
    excluded.

    A dynamic ``importlib.import_module(...)`` is invisible here, as it is to
    any AST import scan. That is a known floor, not an oversight.
    """
    for stmt in body:
        if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if isinstance(stmt, ast.Import | ast.ImportFrom):
            yield stmt
            continue
        if isinstance(stmt, ast.If) and _is_type_checking_test(stmt.test):
            yield from _runtime_imports(stmt.orelse)
            continue
        for block in _nested_blocks(stmt):
            yield from _runtime_imports(block)


def _imports_orchestration(node: ast.Import | ast.ImportFrom) -> bool:
    if isinstance(node, ast.ImportFrom):
        module = node.module or ""
        if module == "moneybin.orchestration" or module.startswith(
            "moneybin.orchestration."
        ):
            return True
        # `from moneybin import orchestration` lands here.
        return module == "moneybin" and any(
            alias.name == "orchestration" for alias in node.names
        )
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


def _scan() -> set[str]:
    offenders: set[str] = set()
    for path in _guarded_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        if any(_imports_orchestration(n) for n in _runtime_imports(tree.body)):
            offenders.add(path.relative_to(SRC).as_posix())
    return offenders


def test_services_do_not_import_orchestration_at_module_level() -> None:
    """No new upward import; the two known inversions are the whole exception set.

    Asserted as set equality rather than a subset so the list cannot rot in
    either direction: a new inversion fails, and so does an entry left behind
    after its inversion is fixed.
    """
    assert _scan() == KNOWN_INVERSIONS, (
        "moneybin.orchestration is imported at module level from below the "
        "layer. Defer the import into the method that needs it, or invert the "
        "call so the orchestrator drives the service. Only add a "
        "KNOWN_INVERSIONS entry (with a `# why`) for a dependency that cannot "
        "be inverted without a contract change."
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


def test_runtime_import_detection_sees_every_executing_block() -> None:
    """Pin what the scan detects, by shape.

    These are the forms a pass over `tree.body` alone misses. `if not
    TYPE_CHECKING:` is the sharp one: it executes, so a name search over the
    test expression would exempt the very block that must be caught.
    """
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
    }
    for label, source in executing.items():
        found = list(_runtime_imports(ast.parse(source).body))
        assert any(_imports_orchestration(n) for n in found), (
            f"{label}: an import that runs at import time went undetected"
        )

    inert = {
        "type-checking-body": (
            "if TYPE_CHECKING:\n    import moneybin.orchestration.refresh"
        ),
        "typing-qualified": (
            "if typing.TYPE_CHECKING:\n    import moneybin.orchestration.refresh"
        ),
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
    }
    for label, source in inert.items():
        found = list(_runtime_imports(ast.parse(source).body))
        assert not any(_imports_orchestration(n) for n in found), (
            f"{label}: an import that never runs was reported as an inversion"
        )


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
