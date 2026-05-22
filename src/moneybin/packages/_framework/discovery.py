"""Entry-points-based package discovery.

discover_packages() enumerates the 'moneybin.packages' entry-points group via
importlib.metadata, locates each package's moneybin_package.yaml manifest from
its distribution's file records *without importing the package*, parses it, and
returns a list of PackageInfo dataclasses for the registration step to consume.

Why no import at discovery: discovery runs before the manifest / capability /
prefix validation gate. EntryPoint.load() would import — and therefore execute
— the package's Python at module load, letting an installed-but-malformed
package run side effects before it has been vetted. We resolve the manifest
from distribution metadata (importlib.metadata file records) instead, and defer
importing the package's callables to registration (register_package), which
runs only after validation passes.

Packages with missing or invalid manifests are skipped with an ERROR log —
a single bad package must NOT take down the whole framework. Registration
proceeds with whatever discovery succeeded.

Known gap: some editable installs (PEP 660) omit data files from the dist
RECORD, so dist.files won't list the manifest and the package is skipped.
In-tree reference packages (Plan 4) ship inside the moneybin wheel, so their
manifests are present; third-party editable dev installs are addressed by the
Plan 5 contributor tooling.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path

from moneybin.packages._framework.manifest import PackageManifest

logger = logging.getLogger(__name__)

_ENTRY_POINT_GROUP = "moneybin.packages"
_MANIFEST_NAME = "moneybin_package.yaml"


@dataclass(frozen=True)
class PackageInfo:
    """A discovered package — parsed manifest plus its filesystem root.

    The root is the directory containing moneybin_package.yaml; validators
    locate SQL files via root/schema/*.sql, models via root/models/, etc.
    """

    manifest: PackageManifest
    root: Path


def _locate_manifest(ep: EntryPoint) -> Path | None:
    """Resolve a package's moneybin_package.yaml from metadata, without importing.

    Reads the entry point's distribution file records (importlib.metadata)
    rather than importing the module, so package code never runs before the
    validation gate. When a distribution ships several manifests (e.g. multiple
    in-tree packages under one wheel), the one whose package directory is the
    *longest* prefix of the entry point's module path is selected — a root-level
    manifest matches any module but loses to a more specific package-dir
    manifest.

    Returns None when no unique manifest resolves: no distribution, an empty
    file list (some editable installs), no manifest whose directory prefixes the
    entry point's module path, or a tie between equally-specific manifests. The
    caller logs and skips. There is deliberately NO single-candidate fallback —
    assigning a non-matching manifest would mis-scope every validator (wrong
    owns_prefix / root), silently validating the wrong package in a multi-package
    or multi-entry-point distribution.
    """
    dist = ep.dist
    if dist is None or not dist.files:
        return None

    candidates = [pp for pp in dist.files if pp.name == _MANIFEST_NAME]
    if not candidates:
        return None

    module_parts = tuple(ep.module.split("."))
    matched = [
        pp for pp in candidates if module_parts[: len(pp.parts) - 1] == pp.parts[:-1]
    ]
    if not matched:
        return None
    # Longest directory prefix wins (parts includes the filename, so a deeper
    # package dir has more parts). A tie at the deepest level is genuinely
    # ambiguous → skip rather than guess.
    max_depth = max(len(pp.parts) for pp in matched)
    most_specific = [pp for pp in matched if len(pp.parts) == max_depth]
    if len(most_specific) != 1:
        return None

    return Path(str(dist.locate_file(most_specific[0]))).resolve()


def discover_packages() -> list[PackageInfo]:
    """Enumerate all installed analysis packages.

    Iterates entry_points(group='moneybin.packages'), resolves each entry's
    moneybin_package.yaml from distribution metadata (no import), and parses it.

    Returns:
        list[PackageInfo]: one per successfully-discovered package.
            Packages whose manifest can't be located, parsed, or validated are
            logged and skipped.
    """
    discovered: list[PackageInfo] = []
    seen_manifests: set[Path] = set()
    for ep in entry_points(group=_ENTRY_POINT_GROUP):
        # Broad catch: discovery faces arbitrary third-party-controlled
        # entry-point strings. A malformed value makes EntryPoint.module raise
        # AttributeError; an unreadable manifest raises OSError; a bad manifest
        # raises PydanticValidationError/ValueError/yaml.YAMLError. One bad
        # package must never abort discovery for every other installed package.
        try:
            manifest_path = _locate_manifest(ep)
            if manifest_path is None:
                logger.error(
                    f"Entry point '{ep.name}': could not resolve a unique "
                    f"{_MANIFEST_NAME} from distribution metadata — none recorded, "
                    f"none matching the entry point's module path, or multiple "
                    f"ambiguous matches; skipping (package not imported)"
                )
                continue
            if not manifest_path.exists():
                logger.error(
                    f"Entry point '{ep.name}': {_MANIFEST_NAME} recorded at "
                    f"{manifest_path} but not present on disk; skipping"
                )
                continue
            manifest = PackageManifest.from_yaml(manifest_path)
        except Exception as exc:  # noqa: BLE001 — defensive at discovery boundary
            logger.error(
                f"Entry point '{ep.name}' could not be discovered: "
                f"{type(exc).__name__}: {exc}; skipping"
            )
            continue

        if manifest_path in seen_manifests:
            # Two entry points resolved to the same manifest — e.g. a malformed
            # dist that ships one shared manifest for several packages. Discover
            # it once; skipping the rest with a clear log beats the opaque
            # "already registered" error register_package would otherwise raise.
            logger.warning(
                f"Entry point '{ep.name}' resolves to manifest {manifest_path}, "
                f"already discovered via another entry point; skipping duplicate"
            )
            continue
        seen_manifests.add(manifest_path)

        discovered.append(PackageInfo(manifest=manifest, root=manifest_path.parent))
        logger.info(
            f"Discovered package '{manifest.name}' "
            f"(quality_scale={manifest.quality_scale}) at {manifest_path.parent}"
        )

    return discovered
