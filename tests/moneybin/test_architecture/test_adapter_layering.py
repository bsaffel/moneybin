"""Structural guardrail: adapters must route domain mutations through services.

The MoneyBin layering convention (see `.claude/rules/mcp.md`,
`.claude/rules/cli.md`, and `.claude/rules/surface-design.md`):

    MCP tools / CLI commands  →  ServiceClass(db).method(...)  →  DuckDB

"Adapter" here means a surface module — the MCP tools and CLI commands named by
`ADAPTER_ROOTS` below — not the `moneybin.adapters` package, which renders a
domain result as a response and is a different job under the same word.

Adapters in `src/moneybin/mcp/tools/` and `src/moneybin/cli/commands/` must not
reach past the service layer into `moneybin.loaders`, `moneybin.extractors`, or
`moneybin.matching` for domain orchestration. When they do, the audit pattern
that produced this PR recurs: business logic ends up in the wrong layer and
fans out across adapters.

This test enforces the convention by AST-parsing every adapter module and
flagging any import from a guarded package that isn't on the allowlist below.

If this test fails on a new import, the cleanest fix is to add a service
method that wraps the underlying function and route the adapter through it.
Only add an allowlist entry when the import is genuinely one of:

  * a pure constant (uppercase name, no side effects)
  * a pure read helper (no writes, no orchestration)
  * a class constructed for dependency injection into a service
  * a type, dataclass, enum, or format-descriptor module

Every allowlist entry carries a one-line ``# why`` comment so future readers
can judge whether the exception still holds.
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

ADAPTER_ROOTS = (
    SRC / "mcp" / "tools",
    SRC / "cli" / "commands",
)
GUARDED_PACKAGES = (
    "moneybin.loaders",
    "moneybin.extractors",
    "moneybin.matching",
)

# Third element for a statement that binds a guarded *module* rather than a name
# out of one (`import moneybin.extractors.x`, `from moneybin import extractors`).
MODULE_IMPORT = "<module>"

# Allowlist entries are (adapter_relpath, imported_module, imported_name) triples.
# `adapter_relpath` is relative to src/moneybin/ for stability across moves.
ADAPTER_LAYERING_ALLOWLIST: frozenset[tuple[str, str, str]] = frozenset({
    # --- Pure validation constants --------------------------------------
    # VALID_MATCH_TYPES is a frozenset of valid match_type values used for
    # CLI argument validation. Pure data, no DB access.
    (
        "cli/commands/transactions/matches.py",
        "moneybin.matching.persistence",
        "VALID_MATCH_TYPES",
    ),
    # --- Pure read helpers ----------------------------------------------
    # import_log.get_import_history is a read-only repo helper consumed by
    # the import_status MCP tool. No writes, no orchestration.
    (
        "mcp/tools/import_tools.py",
        "moneybin.loaders",
        "import_log",
    ),
    # TabularExtractor.get_import_history() is the CLI's read path for
    # `moneybin import history` — class method but read-only (opens DB
    # read_only=True). Functionally equivalent to a module-level read
    # helper.
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular",
        "TabularExtractor",
    ),
    # --- Dependency injection -------------------------------------------
    # PlaidExtractor is constructed by the sync adapters and passed into
    # SyncService(loader=...) as a constructor argument. The adapter
    # never calls a method on it; SyncService is the only consumer.
    (
        "mcp/tools/sync.py",
        "moneybin.extractors.plaid",
        "PlaidExtractor",
    ),
    (
        "cli/commands/sync.py",
        "moneybin.extractors.plaid",
        "PlaidExtractor",
    ),
    # --- Pure type / format descriptors ---------------------------------
    # MatchRunError is the exception MatchingService.run() raises, and it
    # carries the count of accepted transfers the reconciliation already
    # reversed and committed. An adapter that cannot name it cannot report
    # that a decision the user made was undone before the run died — the
    # count exists nowhere else. Catching what the service throws is not
    # matching work crossing the layer; it is the service's own contract.
    (
        "mcp/tools/transactions.py",
        "moneybin.matching.engine",
        "MatchRunError",
    ),
    (
        "cli/commands/transactions/matches.py",
        "moneybin.matching.engine",
        "MatchRunError",
    ),
    # The clause naming what collapsed, in the module that does the
    # collapsing. Pure text, no side effects. It lives there rather than in
    # each surface's helpers because the MCP twin (adapters/rematch_report.py)
    # prints the same sentence, and a copy per surface is exactly how the two
    # wordings drifted apart before.
    (
        "cli/commands/transactions/matches.py",
        "moneybin.matching.reconciliation",
        "RETIRED_SIDES_COLLAPSED",
    ),
    (
        "cli/commands/transactions/review.py",
        "moneybin.matching.reconciliation",
        "RETIRED_SIDES_COLLAPSED",
    ),
    (
        "cli/commands/refresh.py",
        "moneybin.matching.reconciliation",
        "RETIRED_SIDES_COLLAPSED",
    ),
    # The four embedded refresh callers. Each reaches the same reconciliation
    # the dedicated refresh surfaces do — three by running the full cascade,
    # `gsheet pull` by naming `match` explicitly — and each owes the user the
    # same sentence when it reverses a decision of theirs. An import, a pull,
    # a sheet sync, and an unattended drain are where that reversal is least
    # expected, which is why they name it rather than staying quiet.
    (
        "cli/commands/import_cmd.py",
        "moneybin.matching.reconciliation",
        "RETIRED_SIDES_COLLAPSED",
    ),
    (
        "cli/commands/import_inbox.py",
        "moneybin.matching.reconciliation",
        "RETIRED_SIDES_COLLAPSED",
    ),
    (
        "cli/commands/sync.py",
        "moneybin.matching.reconciliation",
        "RETIRED_SIDES_COLLAPSED",
    ),
    (
        "cli/commands/gsheet.py",
        "moneybin.matching.reconciliation",
        "RETIRED_SIDES_COLLAPSED",
    ),
    (
        "cli/commands/accounts/links.py",
        "moneybin.matching.reconciliation",
        "RETIRED_SIDES_OR_ACCOUNTS_COLLAPSED",
    ),
    # Format descriptors and column-mapping types from the tabular
    # extractor subpackage — pure types and constants, no DB access.
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular.formats",
        "NumberFormatType",
    ),
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular.formats",
        "SignConventionType",
    ),
    # cli/commands/gsheet.py uses SignConventionType to type the --sign
    # flag on `gsheet connect` / `gsheet reconnect` so Typer validates
    # the choice against the canonical Literal at parse time. Pure type
    # import, no DB or extractor logic crosses the layer.
    (
        "cli/commands/gsheet.py",
        "moneybin.extractors.tabular.formats",
        "SignConventionType",
    ),
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular.formats",
        "TabularFormat",
    ),
    # --- Pure utility functions (parsing / detection, no DB) ------------
    # detect_format / map_columns / read_file are stateless transforms
    # over file content. They produce dataframes/structs that services
    # then write — the correct direction.
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular.column_mapper",
        "map_columns",
    ),
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular.format_detector",
        "detect_format",
    ),
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular.readers",
        "read_file",
    ),
    (
        "mcp/tools/import_tools.py",
        "moneybin.extractors.tabular.column_mapper",
        "map_columns",
    ),
    # why: pure read — samples one column of an in-memory frame the adapter
    # already holds. Needed so a mapping override can refresh samples for a
    # destination the detector never proposed.
    (
        "mcp/tools/import_tools.py",
        "moneybin.extractors.tabular.column_mapper",
        "collect_samples",
    ),
    (
        "mcp/tools/import_tools.py",
        "moneybin.extractors.tabular.format_detector",
        "detect_format",
    ),
    (
        "mcp/tools/import_tools.py",
        "moneybin.extractors.tabular.readers",
        "read_file",
    ),
    # FIELD_ALIASES is a pure module-level constant (destination field name ->
    # alias list) — import_preview's mapping= override validates against its
    # keys the same way the service layer's resolve_or_confirm does. No DB or
    # extractor logic crosses the layer.
    (
        "mcp/tools/import_tools.py",
        "moneybin.extractors.tabular.field_aliases",
        "FIELD_ALIASES",
    ),
    # `formats` re-exports — multi-name imports from the same module
    # flatten to one allowlist entry per imported name. These are all
    # pure utilities (load_builtin_formats: reads bundled YAML; merge_formats:
    # pure dict merge) or pure reads (load_formats_from_db: SELECT only).
    # MCP catalog reads now route through ImportService; its only remaining
    # direct formats import is the built-in fallback for an unavailable DB.
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular.formats",
        "load_builtin_formats",
    ),
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular.formats",
        "load_formats_from_db",
    ),
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular.formats",
        "merge_formats",
    ),
    (
        "mcp/tools/import_tools.py",
        "moneybin.extractors.tabular.formats",
        "load_builtin_formats",
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
    """Return (adapter_relpath, imported_module, imported_name) triples for a file.

    The guard has to recognize every statement form that reaches a guarded
    package, not just `from <absolute> import Y`. A bare
    `import moneybin.extractors.x` is an `ast.Import` node; `from moneybin import
    extractors` is an `ast.ImportFrom` whose own module is unguarded; and a
    relative `from ...extractors.x import Y` keeps its package hops in
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


def _scan_adapters() -> list[tuple[str, str, str]]:
    """Walk every adapter file and collect guarded imports."""
    triples: list[tuple[str, str, str]] = []
    for root in ADAPTER_ROOTS:
        for path in sorted(root.rglob("*.py")):
            if path.name == "__init__.py":
                continue
            triples.extend(_collect_imports(path))
    return triples


def test_adapters_dont_bypass_service_layer() -> None:
    """Adapters must not import write-callable symbols from guarded packages.

    Any guarded import not on the allowlist is a layering violation. To fix:
    add a service method that wraps the underlying function and route the
    adapter through the service. Only add an allowlist entry when the
    import is a pure constant, pure read helper, DI target, or pure type.
    """
    found = _scan_adapters()
    violations = [t for t in found if t not in ADAPTER_LAYERING_ALLOWLIST]
    if violations:
        formatted = "\n".join(
            _format_violation(rel, mod, name) for rel, mod, name in violations
        )
        pytest.fail(
            "Adapter modules must not import from loaders/extractors/matching "
            "without an allowlist entry. Either route through the service or "
            "add the import to ADAPTER_LAYERING_ALLOWLIST with a `# why` "
            f"comment.\n\nViolations:\n{formatted}"
        )


def test_allowlist_has_no_dead_entries() -> None:
    """Every allowlist entry must match a real import in the tree.

    Stale allowlist entries silently widen the exception surface — if a
    refactor removes the import, the entry should go too.
    """
    found = set(_scan_adapters())
    stale = [entry for entry in ADAPTER_LAYERING_ALLOWLIST if entry not in found]
    if stale:
        formatted = "\n".join(
            _format_violation(rel, mod, name) for rel, mod, name in stale
        )
        pytest.fail(
            "ADAPTER_LAYERING_ALLOWLIST contains entries with no matching "
            f"import in the tree — remove them.\n\nStale entries:\n{formatted}"
        )


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        (
            "from moneybin.extractors.ofx import OFXExtractor",
            ("evasion.py", "moneybin.extractors.ofx", "OFXExtractor"),
        ),
        (
            "import moneybin.extractors.ofx",
            ("evasion.py", "moneybin.extractors.ofx", MODULE_IMPORT),
        ),
        (
            "import moneybin.extractors.ofx as ofx",
            ("evasion.py", "moneybin.extractors.ofx", MODULE_IMPORT),
        ),
        (
            "from moneybin import matching",
            ("evasion.py", "moneybin.matching", MODULE_IMPORT),
        ),
    ],
)
def test_collect_imports_catches_every_statement_form(
    tmp_path: Path, statement: str, expected: tuple[str, str, str]
) -> None:
    """Each statement form that reaches a guarded package must register as a violation.

    Adversarial fixtures, not redundant coverage: the collector originally
    matched `ast.ImportFrom` only, so `import moneybin.extractors.x` reached the
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
        # Two dots from `moneybin/cli/commands/import_cmd.py` stop at
        # `moneybin.cli`; three walk up to `moneybin` and reach extractors/.
        (
            "cli/commands/import_cmd.py",
            "from ...extractors.tabular.formats import merge_formats",
            (
                "cli/commands/import_cmd.py",
                "moneybin.extractors.tabular.formats",
                "merge_formats",
            ),
        ),
        (
            "mcp/tools/import_tools.py",
            "from ...matching import engine",
            ("mcp/tools/import_tools.py", "moneybin.matching", "engine"),
        ),
    ],
)
def test_collect_imports_resolves_relative_statements(
    tmp_path: Path, relpath: str, statement: str, expected: tuple[str, str, str]
) -> None:
    """A relative import that crosses packages must resolve before the guard check.

    The dots live in `ast.ImportFrom.level`, not in `node.module`, so an
    unresolved check reads `from ...extractors.x import Y` as the unguarded
    string `extractors.x` while Python imports `moneybin.extractors.x`.
    """
    module = tmp_path / relpath
    module.parent.mkdir(parents=True, exist_ok=True)
    module.write_text(f"{statement}\n", encoding="utf-8")

    assert _collect_imports(module, src_root=tmp_path) == [expected]


@pytest.mark.parametrize(
    "statement",
    [
        "from . import sibling",
        "from .helpers import render",
        "from ..commands import other",
        # More dots than the package is deep: Python rejects this itself, so
        # there is nothing for the guard to classify.
        "from ..... import anything",
    ],
)
def test_collect_imports_ignores_relative_statements_inside_the_layer(
    tmp_path: Path, statement: str
) -> None:
    """A relative import that stays inside the surface layer is not a violation."""
    module = tmp_path / "cli" / "commands" / "innocent.py"
    module.parent.mkdir(parents=True, exist_ok=True)
    module.write_text(f"{statement}\n", encoding="utf-8")

    assert _collect_imports(module, src_root=tmp_path) == []
