"""Import resolution shared by the layering guards in this package.

Both `test_orchestration_layering` and `test_mcp_import_direction` decide
whether one package reaches another by AST-walking `src/moneybin`, and both
have to resolve a relative import before they can compare it to anything. The
arithmetic is fiddly enough that a second copy is a second thing to get wrong,
and a guard that resolves one level off reports nothing at all — a silent
green, which is the failure mode both files exist to prevent.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src" / "moneybin"


def package_of(path: Path) -> str:
    """Dotted package a file under ``src/moneybin`` belongs to.

    ``services/foo.py`` and ``services/__init__.py`` both answer
    ``moneybin.services``: Python measures a relative import in a package's
    ``__init__`` from that package, not from its parent.
    """
    return ".".join(("moneybin", *path.relative_to(SRC).parts[:-1]))


def resolved_module(node: ast.ImportFrom, package: str) -> str:
    """Absolute dotted module an ``ImportFrom`` names, relative or not.

    Without this a relative import is invisible to a scan: ``from
    ..orchestration import refresh`` parks ``"orchestration"`` in
    ``node.module`` and ``from .. import orchestration`` parks ``None``, and
    neither string matches anything the caller compares against. Relative
    imports are already an active pattern here (``cli/commands/**/__init__``,
    ``cli/main``).

    An over-relative import — more leading dots than the package has parts —
    resolves to ``""`` at every depth. It cannot be a live bypass either way:
    Python raises ``ImportError`` on that shape before any of its names bind.
    The depth is tested rather than the slice result, because two dots too
    many drives the slice index negative and ``parts[:-1]`` hands back a
    plausible-looking base that would invent an edge out of nothing.
    """
    if not node.level:
        return node.module or ""
    parts = package.split(".")
    if node.level > len(parts):
        return ""
    base = ".".join(parts[: len(parts) - node.level + 1])
    return f"{base}.{node.module}" if node.module else base
