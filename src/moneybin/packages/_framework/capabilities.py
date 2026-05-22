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

from moneybin.packages._framework._sql_walk import (
    extract_create_targets,
    find_disallowed_statements,
)
from moneybin.packages._framework.errors import CapabilityViolation
from moneybin.packages._framework.manifest import CapabilityDeclarations

# Schemas a package's schema/ bootstrap DDL may create objects in. Narrower
# than manifest._WRITABLE_SCHEMAS ({raw, app, reports}): a package may declare a
# reports.<prefix>_* write capability, but that capability is satisfied by its
# models/ directory (SQLMesh), never by schema/ bootstrap DDL. reports/core/prep
# are framework-managed layers — confining schema/ CREATE targets to raw/app
# stops a raw_/app_-named schema file from mutating them at bootstrap.
_SCHEMA_DDL_SCHEMAS = frozenset({"raw", "app"})


def is_write_allowed(capability: CapabilityDeclarations, target: str) -> bool:
    """True if 'target' (schema.name) matches any declared write glob.

    Matching is case-insensitive: DuckDB treats unquoted identifiers
    case-insensitively. Internal callers pass targets already lowercased by
    _sql_walk, but this is part of the public API — lower both sides so an
    external caller passing a mixed-case target still matches.
    """
    return any(
        fnmatch.fnmatchcase(target.lower(), pattern.lower())
        for pattern in capability.writes
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
        try:
            targets = extract_create_targets(sql_file)
        except ValueError as exc:
            # extract_create_targets raises on unparseable SQL; surface it as a
            # violation rather than crashing the framework bootstrap (this
            # function's contract is to return violations, never raise).
            violations.append(
                CapabilityViolation(
                    package_name=package_name,
                    message=f"could not parse {sql_file.name}: {exc}",
                    sql_file=str(sql_file),
                    target="(unparseable)",
                )
            )
            continue
        for schema, name in targets:
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


def validate_schema_layers(
    *,
    package_name: str,
    sql_files: Iterable[Path],
) -> list[CapabilityViolation]:
    """Confirm every schema/ CREATE target lands in raw or app.

    Sibling to validate_statement_types: both confine what package schema/
    bootstrap DDL may do. validate_statement_types restricts the statement
    kind (CREATE TABLE/VIEW only); this restricts the target layer (raw/app
    only). The write-glob check (validate_writes) permits reports.<prefix>_*
    because that capability is satisfied by the package's models/ directory —
    but schema/ bootstrap DDL must never write the framework-managed reports
    (or core/prep) layers. Without this, a package could declare a
    reports.<prefix>_* write capability and slip a CREATE VIEW into reports via
    a raw_/app_-named schema file, passing both the write-glob and filename
    checks. Returns violations rather than raising.
    """
    violations: list[CapabilityViolation] = []
    for sql_file in sql_files:
        try:
            targets = extract_create_targets(sql_file)
        except ValueError as exc:
            violations.append(
                CapabilityViolation(
                    package_name=package_name,
                    message=f"could not parse {sql_file.name}: {exc}",
                    sql_file=str(sql_file),
                    target="(unparseable)",
                )
            )
            continue
        for schema, name in targets:
            if schema not in _SCHEMA_DDL_SCHEMAS:
                violations.append(
                    CapabilityViolation(
                        package_name=package_name,
                        message=(
                            f"schema file {sql_file.name} creates {schema}.{name} "
                            f"but package schema/ DDL may only target "
                            f"{sorted(_SCHEMA_DDL_SCHEMAS)} — reports/core views "
                            f"come from models/, not schema/"
                        ),
                        sql_file=str(sql_file),
                        target=f"{schema}.{name}",
                    )
                )
    return violations


def validate_statement_types(
    *,
    package_name: str,
    sql_files: Iterable[Path],
) -> list[CapabilityViolation]:
    """Flag any schema statement that isn't CREATE TABLE / CREATE VIEW.

    The write-glob check (validate_writes) inspects only CREATE targets, so DML
    (INSERT/UPDATE/DELETE) and destructive DDL (DROP/ALTER/TRUNCATE) would slip
    through and run unchecked when Plan 4 executes the SQL. Restricting schema
    files to table/view declarations closes that bypass: anything else is a
    capability violation. Returns violations rather than raising.
    """
    violations: list[CapabilityViolation] = []
    for sql_file in sql_files:
        try:
            disallowed = find_disallowed_statements(sql_file)
        except ValueError as exc:
            violations.append(
                CapabilityViolation(
                    package_name=package_name,
                    message=f"could not parse {sql_file.name}: {exc}",
                    sql_file=str(sql_file),
                    target="(unparseable)",
                )
            )
            continue
        for descriptor in disallowed:
            violations.append(
                CapabilityViolation(
                    package_name=package_name,
                    message=(
                        f"schema file {sql_file.name} contains a {descriptor} "
                        f"statement; package schema/ may only declare CREATE "
                        f"TABLE / CREATE VIEW"
                    ),
                    sql_file=str(sql_file),
                    target=f"({descriptor})",
                )
            )
    return violations
