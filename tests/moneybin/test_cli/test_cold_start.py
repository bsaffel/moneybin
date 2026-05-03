# ruff: noqa: S101
"""Regression guards for CLI cold-start cost.

Every E2E subprocess and every shell autocomplete pays the full module-import
cost for ``moneybin.cli.main``. Eager imports of heavy dependencies (fastmcp,
sqlmesh, polars) at any point reachable from ``moneybin.cli.main``'s
module-top would defeat the lazy-load and deferred-import patterns these
tests guard. See `.claude/rules/cli.md` → "Cold-Start Hygiene".
"""

from __future__ import annotations

import subprocess  # noqa: S404 — clean-interpreter import check
import sys

# Modules that must NOT load when `moneybin.cli.main` is merely imported.
# Substring match — covers parent packages and any fastmcp.* / sqlmesh.* /
# polars.* submodule.
_HEAVY_PREFIXES: tuple[str, ...] = ("fastmcp", "sqlmesh", "polars")


def test_cli_main_import_does_not_load_heavy_deps() -> None:
    """Importing ``moneybin.cli.main`` must not pull fastmcp, sqlmesh, or polars.

    Runs in a clean subprocess so test ordering can't leak modules from
    earlier tests. If this fails, some module on the import chain from
    ``moneybin.cli.main`` is doing an eager ``from <heavy> import …`` at
    its module top — move the import inside the function that needs it
    (see ``src/moneybin/cli/commands/mcp.py`` for the pattern).
    """
    prefixes_repr = repr(list(_HEAVY_PREFIXES))
    snippet = (
        "import sys\n"
        "import moneybin.cli.main  # noqa: F401\n"
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
        "moneybin.cli.main triggered eager import of heavy modules: "
        f"{loaded}. Defer the import inside the command function body."
    )
