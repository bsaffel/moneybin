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

from tests.moneybin.test_architecture._import_graph import (
    SRC,
    package_of,
    resolved_module,
)

EXTRACTOR_ROOTS = (SRC / "extractors",)
GUARDED_PACKAGES = ("moneybin.services",)

# Third element for a statement that binds a guarded *module* rather than a name
# out of one (`import moneybin.services.x`, `from moneybin import services`).
MODULE_IMPORT = "<module>"

# Allowlist entries are (module_relpath, imported_module, imported_name) triples.
# `module_relpath` is relative to src/moneybin/ for stability across moves.
EXTRACTOR_LAYERING_ALLOWLIST: frozenset[tuple[str, str, str]] = frozenset({
    # why: ofx_source_accounts() (moved out of ImportService in MB-52 slice 1,
    # PR #585) builds SourceAccount/AccountNameFacts value objects that
    # currently live under services/. MB-246 relocates them to a layer both
    # extractors/ and services/ can import; remove these five entries and the
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


def _is_guarded(module: str) -> bool:
    """Whether a fully-qualified module path sits inside a guarded package."""
    return any(module.startswith(pkg) for pkg in GUARDED_PACKAGES)


def _format_violation(relpath: str, module: str, name: str) -> str:
    """Render one triple back as the statement that produced it."""
    if name == MODULE_IMPORT:
        return f"  - {relpath}: import {module}"
    return f"  - {relpath}: from {module} import {name}"


def _collect_imports(path: Path, src_root: Path = SRC) -> list[tuple[str, str, str]]:
    """Return (module_relpath, imported_module, imported_name) triples for a file.

    The guard has to recognize every statement form that reaches a guarded
    package, not just `from <absolute> import Y`. A bare
    `import moneybin.services.x` is an `ast.Import` node; `from moneybin import
    services` is an `ast.ImportFrom` whose own module is unguarded; and a
    relative `from ..services.x import Y` keeps its package hops in
    `node.level`. Matching on `ast.ImportFrom.module` alone lets all three
    import the same symbols with the guard silent.

    `_import_graph.resolved_module` owns the level arithmetic, shared with the
    other layering guards in this package — a second copy of it resolves one
    level off and reports nothing, which is the silent green these guards exist
    to prevent. This function owns only the guarded-package comparison.
    """
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    relpath = path.relative_to(src_root).as_posix()
    # `package_of` is pure path arithmetic, so rejoining the relpath under the
    # real source root also resolves an adversarial fixture living elsewhere.
    package = package_of(SRC / relpath)

    triples: list[tuple[str, str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = resolved_module(node, package)
            for alias in node.names:
                if _is_guarded(module):
                    triples.append((relpath, module, alias.name))
                elif _is_guarded(f"{module}.{alias.name}"):
                    triples.append((relpath, f"{module}.{alias.name}", MODULE_IMPORT))
        elif isinstance(node, ast.Import):
            triples.extend(
                (relpath, alias.name, MODULE_IMPORT)
                for alias in node.names
                if _is_guarded(alias.name)
            )
    return triples


def _scan_extractors() -> list[tuple[str, str, str]]:
    """Walk every extractor module — including __init__.py — for guarded imports.

    Unlike `test_adapter_layering.py`'s adapters, an extractor's `__init__.py`
    is the public re-export surface (`from moneybin.extractors.ofx import
    OFXExtractor` resolves through it) — exactly where a services import would
    land if someone routed one through the package root instead of the
    submodule. Skipping it here would leave the guard's most likely evasion
    path unchecked.
    """
    triples: list[tuple[str, str, str]] = []
    for root in EXTRACTOR_ROOTS:
        for path in sorted(root.rglob("*.py")):
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
            _format_violation(rel, mod, name) for rel, mod, name in violations
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
            _format_violation(rel, mod, name) for rel, mod, name in stale
        )
        pytest.fail(
            "EXTRACTOR_LAYERING_ALLOWLIST contains entries with no matching "
            f"import in the tree — remove them.\n\nStale entries:\n{formatted}"
        )


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        (
            "from moneybin.services.account_resolver import resolve",
            ("evasion.py", "moneybin.services.account_resolver", "resolve"),
        ),
        (
            "import moneybin.services.account_resolver",
            ("evasion.py", "moneybin.services.account_resolver", MODULE_IMPORT),
        ),
        (
            "import moneybin.services.account_resolver as resolver",
            ("evasion.py", "moneybin.services.account_resolver", MODULE_IMPORT),
        ),
        (
            "from moneybin import services",
            ("evasion.py", "moneybin.services", MODULE_IMPORT),
        ),
    ],
)
def test_collect_imports_catches_every_statement_form(
    tmp_path: Path, statement: str, expected: tuple[str, str, str]
) -> None:
    """Each statement form that reaches services/ must register as a violation.

    Adversarial fixtures, not redundant coverage: the collector originally
    matched `ast.ImportFrom` only, so `import moneybin.services.x` reached the
    same symbols while both layering guards stayed green.
    """
    module = tmp_path / "evasion.py"
    module.write_text(f"{statement}\n", encoding="utf-8")

    assert _collect_imports(module, src_root=tmp_path) == [expected]


def test_collect_imports_ignores_unguarded_statements(tmp_path: Path) -> None:
    """A neighbouring package that merely shares the `moneybin` prefix is not a hit."""
    module = tmp_path / "innocent.py"
    module.write_text(
        "import moneybin.tables\nfrom moneybin import error_codes\n", encoding="utf-8"
    )

    assert _collect_imports(module, src_root=tmp_path) == []


@pytest.mark.parametrize(
    ("relpath", "statement", "expected"),
    [
        # One dot from `moneybin/extractors/evasion.py` stays in
        # `moneybin.extractors`; two walk up to `moneybin` and reach services/.
        (
            "extractors/evasion.py",
            "from ..services.account_display_name import AccountNameFacts",
            (
                "extractors/evasion.py",
                "moneybin.services.account_display_name",
                "AccountNameFacts",
            ),
        ),
        # Three dots from one package deeper reach the same place.
        (
            "extractors/ofx/evasion.py",
            "from ...services import account_resolver",
            ("extractors/ofx/evasion.py", "moneybin.services", "account_resolver"),
        ),
    ],
)
def test_collect_imports_resolves_relative_statements(
    tmp_path: Path, relpath: str, statement: str, expected: tuple[str, str, str]
) -> None:
    """A relative import that crosses packages must resolve before the guard check.

    The dots live in `ast.ImportFrom.level`, not in `node.module`, so an
    unresolved check reads `from ..services.x import Y` as the unguarded string
    `services.x` while Python imports `moneybin.services.x`.
    """
    module = tmp_path / relpath
    module.parent.mkdir(parents=True, exist_ok=True)
    module.write_text(f"{statement}\n", encoding="utf-8")

    assert _collect_imports(module, src_root=tmp_path) == [expected]


@pytest.mark.parametrize(
    "statement",
    [
        "from . import sibling",
        "from .helpers import parse",
        # More dots than the package is deep: Python rejects this itself, so
        # there is nothing for the guard to classify.
        "from .... import anything",
    ],
)
def test_collect_imports_ignores_relative_statements_inside_the_layer(
    tmp_path: Path, statement: str
) -> None:
    """A relative import that stays inside `extractors/` is not an inversion."""
    module = tmp_path / "extractors" / "ofx" / "innocent.py"
    module.parent.mkdir(parents=True, exist_ok=True)
    module.write_text(f"{statement}\n", encoding="utf-8")

    assert _collect_imports(module, src_root=tmp_path) == []
