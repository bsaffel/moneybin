"""Structural guardrail: extractors must not import from services.

`extractors/` sits below `services/` in MoneyBin's layering
(surfaces -> services -> extractors/loaders/matching -> DuckDB; see
`test_adapter_layering.py` for the downward half of this convention). A
`services` import inside `extractors/` is an upward inversion: it means a
lifecycle-owned type leaked into a channel-specific parsing module instead of
living somewhere both layers can reach.

This test enforces the convention by AST-parsing every module under
`src/moneybin/extractors/` and flagging any import from a guarded package
that isn't on the allowlist below.

Every allowlist entry exists because relocating the imported symbol out of
`services/` is a scoped change of its own (see the entry's `# why` comment
and linked issue) — it is not a license to route new orchestration logic
through `services/` from an extractor. Adding a new entry for a new import
means one more inversion is spreading, not that the pattern is fine.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
EXTRACTOR_ROOTS = (REPO_ROOT / "src" / "moneybin" / "extractors",)
GUARDED_PACKAGES = ("moneybin.services",)

# Allowlist entries are (module_relpath, imported_module, imported_name) triples.
# `module_relpath` is relative to src/moneybin/ for stability across moves.
EXTRACTOR_LAYERING_ALLOWLIST: frozenset[tuple[str, str, str]] = frozenset({
    # why: ofx_source_accounts() (moved out of ImportService in MB-52 slice 1,
    # PR #585) builds SourceAccount/AccountNameFacts value objects that
    # currently live under services/. MB-246 relocates them to a layer both
    # extractors/ and services/ can import; remove these four entries and the
    # DEPRECATED markers in extractors/ofx/extractor.py once it lands.
    (
        "extractors/ofx/extractor.py",
        "moneybin.services.account_display_name",
        "AccountNameFacts",
    ),
    (
        "extractors/ofx/extractor.py",
        "moneybin.services.account_display_name",
        "account_category",
    ),
    (
        "extractors/ofx/extractor.py",
        "moneybin.services.account_display_name",
        "derived_last_four",
    ),
    (
        "extractors/ofx/extractor.py",
        "moneybin.services.account_resolution_types",
        "SourceAccount",
    ),
    (
        "extractors/ofx/extractor.py",
        "moneybin.services.account_resolution_types",
        "normalize_account_identifier",
    ),
})


def _collect_imports(path: Path) -> list[tuple[str, str, str]]:
    """Return (module_relpath, imported_module, imported_name) triples for a file."""
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    relpath = path.relative_to(REPO_ROOT / "src" / "moneybin").as_posix()

    triples: list[tuple[str, str, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        if not any(node.module.startswith(pkg) for pkg in GUARDED_PACKAGES):
            continue
        for alias in node.names:
            triples.append((relpath, node.module, alias.name))
    return triples


def _scan_extractors() -> list[tuple[str, str, str]]:
    """Walk every extractor module and collect guarded imports."""
    triples: list[tuple[str, str, str]] = []
    for root in EXTRACTOR_ROOTS:
        for path in sorted(root.rglob("*.py")):
            if path.name == "__init__.py":
                continue
            triples.extend(_collect_imports(path))
    return triples


def test_extractors_dont_import_services() -> None:
    """Extractor modules must not import from services/ without an allowlist entry.

    Any guarded import not on the allowlist is a layering inversion. To fix:
    relocate the shared symbol to a layer both extractors/ and services/ can
    import (never add a new allowlist entry to paper over a new inversion
    without a tracked relocation issue attached).
    """
    found = _scan_extractors()
    violations = [t for t in found if t not in EXTRACTOR_LAYERING_ALLOWLIST]
    if violations:
        formatted = "\n".join(
            f"  - {rel}: from {mod} import {name}" for rel, mod, name in violations
        )
        pytest.fail(
            "Extractor modules must not import from moneybin.services without "
            "an allowlist entry. Relocate the shared symbol to a neutral layer, "
            "or add the import to EXTRACTOR_LAYERING_ALLOWLIST with a `# why` "
            f"comment and a tracked issue.\n\nViolations:\n{formatted}"
        )


def test_allowlist_has_no_dead_entries() -> None:
    """Every allowlist entry must match a real import in the tree.

    Stale allowlist entries silently widen the exception surface — if a
    refactor removes the import, the entry should go too.
    """
    found = set(_scan_extractors())
    stale = [entry for entry in EXTRACTOR_LAYERING_ALLOWLIST if entry not in found]
    if stale:
        formatted = "\n".join(
            f"  - {rel}: from {mod} import {name}" for rel, mod, name in stale
        )
        pytest.fail(
            "EXTRACTOR_LAYERING_ALLOWLIST contains entries with no matching "
            f"import in the tree — remove them.\n\nStale entries:\n{formatted}"
        )
