"""Entry-points-based package discovery.

discover_packages() enumerates the 'moneybin.packages' entry-points group via
importlib.metadata, loads each entry's module, locates its moneybin_package.yaml
manifest relative to the module file, parses it, and returns a list of
PackageInfo dataclasses for the registration step to consume.

Packages with missing or invalid manifests are skipped with an ERROR log —
a single bad package must NOT take down the whole framework. Registration
proceeds with whatever discovery succeeded.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from importlib.metadata import entry_points
from pathlib import Path

import yaml
from pydantic import ValidationError as PydanticValidationError

from moneybin.packages._framework.manifest import PackageManifest

logger = logging.getLogger(__name__)

_ENTRY_POINT_GROUP = "moneybin.packages"


@dataclass(frozen=True)
class PackageInfo:
    """A discovered package — parsed manifest plus its filesystem root.

    The root is the directory containing moneybin_package.yaml; validators
    locate SQL files via root/schema/*.sql, models via root/models/, etc.
    """

    manifest: PackageManifest
    root: Path


def discover_packages() -> list[PackageInfo]:
    """Enumerate all installed analysis packages.

    Iterates entry_points(group='moneybin.packages'), loads each entry's
    module, reads the adjacent moneybin_package.yaml, and parses it.

    Returns:
        list[PackageInfo]: one per successfully-discovered package.
            Packages with missing or invalid manifests are logged and skipped.
    """
    discovered: list[PackageInfo] = []
    for ep in entry_points(group=_ENTRY_POINT_GROUP):
        try:
            module = ep.load()
        except Exception as exc:  # noqa: BLE001 — defensive at discovery boundary
            logger.error(
                f"Failed to load entry point '{ep.name}': {type(exc).__name__}: {exc}"
            )
            continue

        module_file = getattr(module, "__file__", None)
        if module_file is None:
            logger.error(
                f"Entry point '{ep.name}' resolved to a module without __file__; skipping"
            )
            continue

        root = Path(module_file).resolve().parent
        manifest_path = root / "moneybin_package.yaml"
        if not manifest_path.exists():
            logger.error(
                f"Entry point '{ep.name}' has no moneybin_package.yaml at {root}; skipping"
            )
            continue

        try:
            manifest = PackageManifest.from_yaml(manifest_path)
        except (PydanticValidationError, ValueError, yaml.YAMLError) as exc:
            logger.error(
                f"Entry point '{ep.name}' has invalid manifest at "
                f"{manifest_path}: {type(exc).__name__}: {exc}"
            )
            continue

        discovered.append(PackageInfo(manifest=manifest, root=root))
        logger.info(
            f"Discovered package '{manifest.name}' "
            f"(quality_scale={manifest.quality_scale}) at {root}"
        )

    return discovered
