"""register_package() orchestration + PackageRegistry singleton.

Wires the validators (manifest, capabilities, prefix, quality_scale)
together: validate_package() returns every error found; register_package()
raises on the first one (refusing the registration entirely) then calls
the package's tools.register() and cli.register() callables and adds the
package to the in-process registry.

The registry is held in a module-level singleton mirroring the pattern
used by FastMCP's tool registry and moneybin's metrics registry. Tests
construct fresh PackageRegistry() instances directly to avoid global
state leakage.

Known deferred (out of Plan 2 scope):
- Gold-tier user-guide check at docs/guides/packages/<name>/ — lives at the
  repo level rather than info.root, wired in Plan 4 when reference packages
  ship.
- SQLMesh model-path registration — Plan 4 adds the integration once
  reference packages have models/ directories.
- Executing package schema DDL in production — schema.init_schemas() accepts
  additional_files, but threading discovered packages' schema files into the
  database bootstrap (and validating path-containment of those files) is wired
  in Plan 4 alongside reference packages. No packages ship in Plan 2, so there
  is nothing to execute yet.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from moneybin.packages._framework.capabilities import (
    validate_identifier_safety,
    validate_schema_layers,
    validate_statement_types,
    validate_writes,
)
from moneybin.packages._framework.discovery import PackageInfo
from moneybin.packages._framework.errors import (
    CapabilityViolation,
    QualityScaleViolation,
    ValidationError,
)
from moneybin.packages._framework.prefix import (
    validate_schema_filenames,
    validate_sql_write_prefixes,
)
from moneybin.packages._framework.quality_scale import (
    validate_quality_scale as _validate_quality_scale,
)

if TYPE_CHECKING:
    from fastmcp import FastMCP
    from typer import Typer

logger = logging.getLogger(__name__)


class PackageRegistry:
    """In-process registry of successfully-registered analysis packages.

    Held as a module-level singleton (see `_global_registry`); tests create
    fresh PackageRegistry() instances to avoid cross-test pollution.
    """

    def __init__(self) -> None:
        self._packages: dict[str, PackageInfo] = {}

    def add(self, info: PackageInfo) -> None:
        if info.manifest.name in self._packages:
            raise ValueError(f"Package '{info.manifest.name}' already registered")
        self._packages[info.manifest.name] = info

    def get(self, name: str) -> PackageInfo:
        if name not in self._packages:
            raise KeyError(f"Package '{name}' not registered")
        return self._packages[name]

    def remove(self, name: str) -> None:
        """Remove a package from the registry (used to roll back a failed registration)."""
        self._packages.pop(name, None)

    def all(self) -> list[PackageInfo]:
        return list(self._packages.values())


_global_registry = PackageRegistry()


def get_registry() -> PackageRegistry:
    """Module-level singleton accessor."""
    return _global_registry


def validate_package(info: PackageInfo) -> list[ValidationError]:
    """Run every validator and return the combined violation list.

    Does not raise — callers (including the validator CLI / MCP tool added in
    Plan 5) consume the list directly. register_package() turns the first
    violation into a raise.

    Note: MCP-tool and CLI-command prefix checks are not run here — they
    require live introspection of the registered surfaces (Plan 5).
    """
    errors: list[ValidationError] = []
    schema_dir = info.root / "schema"
    if not schema_dir.is_dir():
        if info.manifest.capabilities.writes:
            errors.append(
                CapabilityViolation(
                    package_name=info.manifest.name,
                    message=(
                        "manifest declares capabilities.writes but the package "
                        "has no schema/ directory to satisfy them"
                    ),
                    sql_file="(missing schema/ directory)",
                    target="(none)",
                )
            )
        # No schema dir + no declared writes → nothing to validate; still run
        # the quality-scale checks below.
        sql_files: list[Path] = []
    elif not schema_dir.resolve().is_relative_to(info.root.resolve()):
        # A schema/ symlink escaping the package root would let validation bless
        # out-of-tree SQL that init_schemas' own containment guard rejects at
        # execution (.claude/rules/security.md path-traversal rule) — refuse it
        # here too so validation ⊇ execution. Hard stop: nothing else is
        # trustworthy once the schema dir points outside the package.
        return [
            CapabilityViolation(
                package_name=info.manifest.name,
                message="schema/ resolves outside the package root (symlink escape)",
                sql_file=str(schema_dir),
                target="(out-of-tree)",
            )
        ]
    else:
        # rglob, not glob: a *.sql nested in a subdirectory must not escape the
        # capability/prefix validators (otherwise a package could hide a
        # cross-prefix CREATE in schema/sub/). Plan 4's execution wiring must
        # use the same recursive discovery so validation ⊇ execution. rglob
        # follows symlinks on Python < 3.13, so drop any file whose real path
        # escapes the package root — a nested symlink can't smuggle in
        # out-of-tree SQL the validators would otherwise bless.
        root = info.root.resolve()
        sql_files = sorted(
            p for p in schema_dir.rglob("*.sql") if p.resolve().is_relative_to(root)
        )

    errors.extend(
        validate_statement_types(
            package_name=info.manifest.name,
            sql_files=sql_files,
        )
    )

    errors.extend(
        validate_identifier_safety(
            package_name=info.manifest.name,
            sql_files=sql_files,
        )
    )

    errors.extend(
        validate_schema_layers(
            package_name=info.manifest.name,
            sql_files=sql_files,
        )
    )

    errors.extend(
        validate_writes(
            package_name=info.manifest.name,
            sql_files=sql_files,
            capability=info.manifest.capabilities,
        )
    )

    errors.extend(
        validate_sql_write_prefixes(
            package_name=info.manifest.name,
            owns_prefix=info.manifest.owns_prefix,
            sql_files=sql_files,
        )
    )
    errors.extend(
        validate_schema_filenames(
            package_name=info.manifest.name,
            owns_prefix=info.manifest.owns_prefix,
            schema_files=sql_files,
        )
    )

    errors.extend(
        _validate_quality_scale(info, claimed_tier=info.manifest.quality_scale)
    )

    return errors


def validate_quality_scale(
    info: PackageInfo, claimed_tier: str
) -> list[QualityScaleViolation]:
    """Public re-export for callers that want only the QS check.

    Raises:
        ValueError: if claimed_tier is unknown, or exceeds the manifest's
            declared quality_scale. Callers passing user-supplied tier strings
            (e.g. a validator CLI) should handle this.
    """
    return _validate_quality_scale(info, claimed_tier=claimed_tier)


def register_package(
    *,
    info: PackageInfo,
    mcp: FastMCP,
    cli: Typer,
    tools_callable: Callable[[Any], None] | None = None,
    cli_callable: Callable[[Any], None] | None = None,
) -> None:
    """Validate and register a package.

    On any validation failure, raises the first error and registers nothing.
    On success, adds the package to the registry, then invokes the package's
    tools.register(mcp) and cli.register(cli) callables.

    The tools_callable / cli_callable arguments allow tests and the framework
    bootstrap to inject the resolved callable directly; production code paths
    resolve them from manifest.entry_points via importlib.

    Raises:
        ValidationError subclass: first validation violation encountered.
        ValueError: a malformed entry-point spec or an uninstalled entry-point
            module (from _resolve_entry_point_callable), or a duplicate package
            name (from the registry add) — both raised after validation passes
            and are NOT ValidationError subclasses.
        TypeError: an entry-point target that resolves to a non-callable.
    """
    errors = validate_package(info)
    if errors:
        # Raise the first error so callers see a precise exception type;
        # validate_package() provides the full list when callers want all.
        raise errors[0]

    if tools_callable is None:
        tools_callable = _resolve_entry_point_callable(info.manifest.entry_points.tools)
    if cli_callable is None:
        cli_callable = _resolve_entry_point_callable(info.manifest.entry_points.cli)

    # Add to the registry first so a duplicate-name failure aborts before
    # any external surface (MCP/CLI) is mutated.
    _global_registry.add(info)
    try:
        tools_callable(mcp)
        cli_callable(cli)
    except Exception:
        # Remove the registry entry so a failed callable leaves no
        # half-registered package recorded.
        #
        # Limitation (partial rollback): the registry entry is removed, but
        # FastMCP's tool surface is not. If tools_callable(mcp) succeeded before
        # cli_callable(cli) raised, those MCP tools are already registered and
        # FastMCP exposes no un-register API to undo them. Re-running registration
        # would then double-register the package's MCP tools, so recovery from a
        # partial failure requires a process restart, not a retry. Atomic
        # registration (full MCP-surface rollback) is deferred to Plan 4 (ties to
        # the registry-injection followup).
        _global_registry.remove(info.manifest.name)
        raise
    logger.info(
        f"Registered package '{info.manifest.name}' "
        f"(tier={info.manifest.quality_scale})"
    )


def _resolve_entry_point_callable(spec: str) -> Callable[[Any], None]:
    """Resolve a 'module.path:callable' string into the callable.

    Mirrors the importlib.metadata.EntryPoint.load convention; we re-implement
    the lookup here because manifest entry-point strings are not registered
    via setuptools — they're paths inside the already-installed package.
    """
    module_path, _, attr = spec.partition(":")
    if not module_path or not attr:
        raise ValueError(f"Entry point '{spec}' must be 'module.path:callable'")
    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        # Only rewrite when the entry-point module itself (or a parent package)
        # is missing. If the module exists but one of ITS imports is missing,
        # exc.name differs — re-raise so the real dependency failure isn't
        # masked as "not installed". exc.name is None for some C-extension /
        # shared-library load failures (and bare `ModuleNotFoundError()`); that
        # is NOT evidence the entry module is missing, so re-raise it unchanged
        # rather than mislabeling a real load error as "not installed".
        missing = exc.name or ""
        entry_module_missing = missing == module_path or module_path.startswith(
            f"{missing}."
        )
        if not entry_module_missing:
            raise
        raise ValueError(
            f"Entry point '{spec}' module '{module_path}' is not installed"
        ) from exc
    try:
        fn = getattr(module, attr)
    except AttributeError as exc:
        raise ValueError(
            f"Entry point '{spec}' has no attribute '{attr}' "
            f"on module '{module.__name__}'"
        ) from exc
    if not callable(fn):
        raise TypeError(f"Entry point '{spec}' resolves to non-callable {fn!r}")
    return cast(Callable[[Any], None], fn)
