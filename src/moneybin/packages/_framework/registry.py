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
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from moneybin.packages._framework.capabilities import validate_writes
from moneybin.packages._framework.discovery import PackageInfo
from moneybin.packages._framework.errors import (
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
    sql_files = sorted((info.root / "schema").glob("*.sql"))

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
    """Public re-export for callers that want only the QS check."""
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
    On success, invokes the package's tools.register(mcp) and cli.register(app)
    callables, then adds the package to the registry.

    The tools_callable / cli_callable arguments allow tests and the framework
    bootstrap to inject the resolved callable directly; production code paths
    resolve them from manifest.entry_points via importlib.

    Raises:
        ValidationError subclass: first violation encountered.
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

    tools_callable(mcp)
    cli_callable(cli)
    _global_registry.add(info)
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
    if not attr:
        raise ValueError(f"Entry point '{spec}' must be 'module.path:callable'")
    module = importlib.import_module(module_path)
    fn = getattr(module, attr)
    if not callable(fn):
        raise TypeError(f"Entry point '{spec}' resolves to non-callable {fn!r}")
    return cast(Callable[[Any], None], fn)
