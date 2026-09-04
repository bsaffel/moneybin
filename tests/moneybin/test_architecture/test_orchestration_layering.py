# ruff: noqa: S101
"""Structural guardrail: orchestrators compose services, not the other way round.

`moneybin.orchestration` sits one layer above `moneybin.services`. The refresh
pipeline lives there because it drives the matcher, SQLMesh apply, the
categorizer, the identity link services and the rate backfill — it is a
composition of the service layer, not a peer of it.

    CLI / MCP  →  moneybin.orchestration  →  moneybin.services  →  DuckDB

Two tests hold that shape, and they answer different questions. The source scan
below says *nothing new* reaches upward. The behavioural test says the
orchestrator's own deferred imports are still deferred — a fact no source scan
can establish, because a hoisted import looks perfectly ordinary in the diff.
"""

from __future__ import annotations

import ast
import subprocess  # noqa: S404 — clean-interpreter import check
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src" / "moneybin"

# The service layer and everything beneath it. `cli/` and `mcp/` sit *above*
# orchestration and may import it at module level without comment.
GUARDED_ROOTS = (
    "services",
    "repositories",
    "connectors",
    "loaders",
    "extractors",
    "matching",
)

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


def _module_level_imports(tree: ast.Module) -> list[ast.Import | ast.ImportFrom]:
    """Imports executed at import time — TYPE_CHECKING blocks excluded.

    A `TYPE_CHECKING` import never runs, so it cannot create a runtime
    dependency and is not an inversion. Function-body imports are excluded for
    the same reason: deferring is the sanctioned way to call upward.
    """
    found: list[ast.Import | ast.ImportFrom] = []
    for stmt in tree.body:
        if isinstance(stmt, ast.Import | ast.ImportFrom):
            found.append(stmt)
        elif isinstance(stmt, ast.If):
            if "TYPE_CHECKING" in ast.unparse(stmt.test):
                continue
            found.extend(
                s for s in stmt.body if isinstance(s, ast.Import | ast.ImportFrom)
            )
        elif isinstance(stmt, ast.Try):
            found.extend(
                s for s in stmt.body if isinstance(s, ast.Import | ast.ImportFrom)
            )
    return found


def _imports_orchestration(node: ast.Import | ast.ImportFrom) -> bool:
    if isinstance(node, ast.ImportFrom):
        module = node.module or ""
        if module == "moneybin.orchestration" or module.startswith(
            "moneybin.orchestration."
        ):
            return True
        # `from moneybin import orchestration` / `from moneybin.orchestration
        # import refresh` both land here.
        return module == "moneybin" and any(
            alias.name == "orchestration" for alias in node.names
        )
    return any(
        alias.name == "moneybin.orchestration"
        or alias.name.startswith("moneybin.orchestration.")
        for alias in node.names
    )


def _scan() -> set[str]:
    offenders: set[str] = set()
    for root in GUARDED_ROOTS:
        for path in sorted((SRC / root).rglob("*.py")):
            tree = ast.parse(path.read_text(), filename=str(path))
            if any(_imports_orchestration(n) for n in _module_level_imports(tree)):
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
