"""Capability declarations + the write-glob validator.

The 'writes' axis is the load-bearing capability per spec — pre-launch and
post-launch, a package physically cannot register if its SQL creates tables
outside the declared globs. Reads/network/secrets are documentation-only at
launch (runtime-enforced post-launch); their declarations are parsed for
forward-compat schema stability.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Iterable
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from moneybin.packages._framework._sql_walk import extract_create_targets
from moneybin.packages._framework.errors import CapabilityViolation


class Capability(BaseModel):
    """A package's declared capability surface.

    Mirrors CapabilityDeclarations in manifest.py — kept as a separate model
    so validators consume a runtime-friendly Capability (with helper methods)
    rather than the raw parsed manifest sub-block. PackageManifest.capabilities
    converts via Capability(**manifest.capabilities.model_dump()).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    writes: list[str]
    """Glob patterns of tables this package writes (e.g. ['app.assets_*'])."""

    reads: list[str]
    """Glob patterns of tables this package reads (documentation at launch)."""

    network: list[str]
    """Hostnames this package may HTTP to (documentation at launch)."""

    secrets: list[str]
    """SecretStore keys this package needs (enforced at first access)."""

    def is_write_allowed(self, target: str) -> bool:
        """True if 'target' (schema.name) matches any declared write glob."""
        return any(fnmatch.fnmatchcase(target, pattern) for pattern in self.writes)


def validate_writes(
    *,
    package_name: str,
    sql_files: Iterable[Path],
    capability: Capability,
) -> list[CapabilityViolation]:
    """Confirm every CREATE TABLE / CREATE VIEW matches a declared write glob.

    Returns a list of violations rather than raising on the first one so
    contributors see every problem at once. The orchestration layer
    (registry.py) decides whether to raise based on the list's length.
    """
    violations: list[CapabilityViolation] = []
    for sql_file in sql_files:
        for schema, name in extract_create_targets(sql_file):
            target = f"{schema}.{name}"
            if not capability.is_write_allowed(target):
                violations.append(
                    CapabilityViolation(
                        package_name=package_name,
                        message=(
                            f"SQL creates {target} but no matching write capability "
                            f"is declared (declared: {capability.writes})"
                        ),
                        sql_file=str(sql_file),
                        target=target,
                    )
                )
    return violations
