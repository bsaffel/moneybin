"""Structural guardrail: nothing outside ``mcp/`` imports ``moneybin.mcp``.

``moneybin.mcp`` is one transport, not a shared layer. Cross-surface machinery
— sensitivity tiers in ``moneybin.privacy``, the response envelope and write
contracts in ``moneybin.protocol``, the adapters that render a domain result
into one in ``moneybin.adapters`` — lives outside it, where the CLI, the
reports framework, and a future HTTP surface reach it without importing a
transport they do not speak. The rule this test enforces is the direction:

    cli, mcp  →  adapters  →  orchestration  →  services  →  DuckDB
                     ↘  privacy, protocol  ↙

Only the reverse edge is forbidden. ``mcp/`` importing anything below it is
normal and unguarded.

Two module bodies legitimately sit on the wrong side and are exempted below by
name: the CLI command group whose whole job is launching and inspecting the MCP
server, and the reports framework's tool-registration glue. Every other import
of ``moneybin.mcp`` from outside ``src/moneybin/mcp/`` is a violation — either
the imported symbol is cross-surface (move it to ``privacy``, ``protocol``, or
``adapters``) or the caller belongs inside ``mcp/``.

The scan covers ``src/moneybin/`` only. ``tests/`` and ``scripts/`` reach the
server the way the exempted CLI command does — as consumers driving it, not as
library code a surface would carry — so guarding them would collect exemptions
without protecting anything.

Three shapes it reads that a grep does not:

- **Deferred imports.** Most of these live inside function bodies; an anchored
  ``^from moneybin.mcp`` grep sees under a quarter of them. ``ast.walk``
  reaches them all, and a deferred import is a real dependency — the function
  runs.
- **Relative imports.** ``from ..mcp.server import mcp`` parks ``mcp.server``
  in ``node.module`` and matches no absolute comparison, so it would be a live
  bypass rather than a blind spot. ``resolved_module`` resolves the level
  first. Relative imports are already an active pattern in ``cli/``.
- **String module paths.** ``cli/commands/mcp.py`` registers the resource and
  prompt modules through ``importlib.import_module``, so an import-node-only
  scan would not see that edge at all.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from tests.moneybin.test_architecture._import_graph import package_of, resolved_module

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = REPO_ROOT / "src" / "moneybin"
MCP_ROOT = SRC_ROOT / "mcp"

# A dotted path naming `moneybin.mcp` or a module under it, and nothing else.
# Anchored so prose mentioning the package in a docstring is not a match.
MCP_MODULE_RE = re.compile(r"^moneybin\.mcp(\.[A-Za-z_][A-Za-z0-9_]*)*$")

# (module_relpath, imported_module, imported_name) triples, relative to
# src/moneybin/. `<dynamic>` is a module named by a string constant rather
# than an import statement.
MCP_IMPORT_EXEMPTIONS: frozenset[tuple[str, str, str]] = frozenset({
    # --- The CLI command group that runs the server ----------------------
    # why: `moneybin mcp serve` IS the server's entry point — it boots the
    # FastMCP app, so it necessarily names it. Deferred to keep fastmcp off
    # the CLI cold-start path.
    ("cli/commands/mcp.py", "moneybin.mcp.server", "check_schema_at_boot"),
    ("cli/commands/mcp.py", "moneybin.mcp.server", "close_db"),
    ("cli/commands/mcp.py", "moneybin.mcp.server", "init_db"),
    ("cli/commands/mcp.py", "moneybin.mcp.server", "mcp"),
    (
        "cli/commands/mcp.py",
        "moneybin.mcp.server",
        "purge_expired_import_previews_at_boot",
    ),
    # why: the first-run middleware is installed on the server the command
    # just built; it configures that server and nothing else.
    ("cli/commands/mcp.py", "moneybin.mcp.first_run", "FirstRunSetupMiddleware"),
    # why: `moneybin mcp install` warns when a host's tool cap is below the
    # tool count. Those are plain ints in `surface.py` precisely so the
    # warning does not have to boot the server to count tools.
    ("cli/commands/mcp.py", "moneybin.mcp.surface", "VISIBLE_TOOL_COUNT"),
    ("cli/commands/mcp.py", "moneybin.mcp.surface", "WINDSURF_ACTIVE_TOOL_CAP"),
    # why: `serve` imports these two for their registration side effects, so
    # they are named as strings to `importlib.import_module` rather than
    # bound. Same justification as the `server` entries above.
    ("cli/commands/mcp.py", "moneybin.mcp.prompts", "<dynamic>"),
    ("cli/commands/mcp.py", "moneybin.mcp.resources", "<dynamic>"),
    # --- Reports → MCP tool registration glue ----------------------------
    # why: report runners are declared in `reports/`, but one of the surfaces
    # they must appear on is MCP. This is the registration call itself, not
    # report logic reaching for a transport — it runs only when a server is
    # being built and is deferred so importing the registry does not pull
    # fastmcp.
    ("reports/_framework/registry.py", "moneybin.mcp._registration", "register"),
    ("reports/_framework/registry.py", "moneybin.mcp.tools.reports", "reports"),
})


def _collect_mcp_references(path: Path) -> list[tuple[str, str, str]]:
    """Return (relpath, mcp_module, imported_name) triples for one module."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    relpath = path.relative_to(SRC_ROOT).as_posix()

    package = package_of(path)

    triples: list[tuple[str, str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = resolved_module(node, package)
            if MCP_MODULE_RE.match(module):
                triples.extend((relpath, module, alias.name) for alias in node.names)
            elif module == "moneybin":
                # `from moneybin import mcp`, and its relative twin
                # `from .. import mcp`, name the package in `names`.
                triples.extend(
                    (relpath, "moneybin.mcp", "<module>")
                    for alias in node.names
                    if alias.name == "mcp"
                )
        elif isinstance(node, ast.Import):
            triples.extend(
                (relpath, alias.name, "<module>")
                for alias in node.names
                if MCP_MODULE_RE.match(alias.name)
            )
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if MCP_MODULE_RE.match(node.value):
                triples.append((relpath, node.value, "<dynamic>"))
    return triples


def _scan_non_mcp_sources() -> set[tuple[str, str, str]]:
    """Every reference to `moneybin.mcp` from outside `src/moneybin/mcp/`."""
    triples: set[tuple[str, str, str]] = set()
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if MCP_ROOT in path.parents:
            continue
        triples.update(_collect_mcp_references(path))
    return triples


def test_nothing_outside_mcp_imports_mcp() -> None:
    """Only the exempted entry points may name `moneybin.mcp`.

    A new violation means one of two things. If the symbol is cross-surface,
    it is in the wrong package: move it to `moneybin.privacy` (classification,
    audit, caps), `moneybin.protocol` (envelope, pagination, write contracts),
    or `moneybin.adapters` (rendering a domain result as a response) and
    import it from there. If it is genuinely MCP-specific, the caller reaching
    for it belongs inside `mcp/`.
    """
    violations = sorted(_scan_non_mcp_sources() - MCP_IMPORT_EXEMPTIONS)
    if violations:
        formatted = "\n".join(
            f"  - {rel}: from {mod} import {name}" for rel, mod, name in violations
        )
        pytest.fail(
            "Modules outside src/moneybin/mcp/ must not import moneybin.mcp. "
            "Move the shared symbol to moneybin.privacy, moneybin.protocol or "
            "moneybin.adapters, "
            "or move the caller into mcp/. Only add an MCP_IMPORT_EXEMPTIONS "
            "entry when the caller's whole job is running or registering the "
            f"MCP server.\n\nViolations:\n{formatted}"
        )


def test_exemptions_have_no_dead_entries() -> None:
    """Every exemption must match a real reference in the tree.

    Asserted alongside the test above so the pair is a set equality: a new
    edge fails there, a removed one fails here. A stale entry silently
    pre-authorizes an import nobody has justified.
    """
    stale = sorted(MCP_IMPORT_EXEMPTIONS - _scan_non_mcp_sources())
    if stale:
        formatted = "\n".join(
            f"  - {rel}: from {mod} import {name}" for rel, mod, name in stale
        )
        pytest.fail(
            "MCP_IMPORT_EXEMPTIONS contains entries with no matching reference "
            f"in the tree — remove them.\n\nStale entries:\n{formatted}"
        )
