"""Structural guardrail: adapters must route domain mutations through services.

The MoneyBin layering convention (see `.claude/rules/mcp.md`,
`.claude/rules/cli.md`, and `.claude/rules/surface-design.md`):

    MCP tools / CLI commands  →  ServiceClass(db).method(...)  →  DuckDB

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

REPO_ROOT = Path(__file__).resolve().parents[3]
ADAPTER_ROOTS = (
    REPO_ROOT / "src" / "moneybin" / "mcp" / "tools",
    REPO_ROOT / "src" / "moneybin" / "cli" / "commands",
)
GUARDED_PACKAGES = (
    "moneybin.loaders",
    "moneybin.extractors",
    "moneybin.matching",
)

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
    # `formats` re-exports — multi-name imports from the same module
    # flatten to one allowlist entry per imported name. These are all
    # pure utilities (load_builtin_formats: reads bundled YAML; merge_formats:
    # pure dict merge) or pure reads (load_formats_from_db: SELECT only).
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
    (
        "mcp/tools/import_tools.py",
        "moneybin.extractors.tabular.formats",
        "load_formats_from_db",
    ),
    (
        "mcp/tools/import_tools.py",
        "moneybin.extractors.tabular.formats",
        "merge_formats",
    ),
    # TODO followup: route `import formats delete` through a future
    # FormatsService. delete_format_from_db writes to app.tabular_formats —
    # this is a layering bypass the guardrail surfaced after the audit
    # closed. Tracked as a follow-up; allowlisted temporarily.
    (
        "cli/commands/import_cmd.py",
        "moneybin.extractors.tabular.formats",
        "delete_format_from_db",
    ),
})


def _collect_imports(
    path: Path,
) -> list[tuple[str, str, str]]:
    """Return (adapter_relpath, imported_module, imported_name) triples for a file."""
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
            f"  - {rel}: from {mod} import {name}" for rel, mod, name in violations
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
            f"  - {rel}: from {mod} import {name}" for rel, mod, name in stale
        )
        pytest.fail(
            "ADAPTER_LAYERING_ALLOWLIST contains entries with no matching "
            f"import in the tree — remove them.\n\nStale entries:\n{formatted}"
        )
