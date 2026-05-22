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

from moneybin.packages._framework._sql_walk import extract_create_targets
from moneybin.packages._framework.errors import CapabilityViolation
from moneybin.packages._framework.manifest import CapabilityDeclarations


def is_write_allowed(capability: CapabilityDeclarations, target: str) -> bool:
    """True if 'target' (schema.name) matches any declared write glob.

    Matching is case-insensitive: DuckDB treats unquoted identifiers
    case-insensitively, and _sql_walk normalizes extracted targets to
    lowercase, so patterns are lowered here to match either-case globs.
    """
    return any(
        fnmatch.fnmatchcase(target, pattern.lower()) for pattern in capability.writes
    )


def validate_writes(
    *,
    package_name: str,
    sql_files: Iterable[Path],
    capability: CapabilityDeclarations,
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
            if not is_write_allowed(capability, target):
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
