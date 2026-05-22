"""Prefix-discipline validators per spec §"Naming and prefix discipline".

Every surface a package contributes (SQL writes, schema files, MCP tools,
CLI commands) must start with the package's declared owns_prefix. The
coherence rule per design-principles.md treats violations as refusing
registration; the validators surface every offender so contributors see
the full diff between declared and actual.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from moneybin.packages._framework._sql_walk import extract_create_targets
from moneybin.packages._framework.errors import PrefixViolation


def validate_sql_write_prefixes(
    *,
    package_name: str,
    owns_prefix: str,
    sql_files: Iterable[Path],
) -> list[PrefixViolation]:
    """Confirm every CREATE TABLE/VIEW target uses the package's prefix.

    Returns a violation per (table, schema) outside the prefix; empty list
    when all writes are inside. This checks only the target *name* prefix, so
    it stays reusable for the package's models/ surface (where reports.* is a
    valid target). The orthogonal constraint that schema/ bootstrap DDL may
    only target the raw/app layers is enforced separately by
    capabilities.validate_schema_layers.
    """
    violations: list[PrefixViolation] = []
    for sql_file in sql_files:
        try:
            targets = extract_create_targets(sql_file)
        except ValueError as exc:
            # Unparseable SQL surfaces as a violation, not a crash — same
            # return-violations-never-raise contract as validate_writes.
            violations.append(
                PrefixViolation(
                    package_name=package_name,
                    message=f"could not parse {sql_file.name}: {exc}",
                    surface="sql_write",
                    offender=sql_file.name,
                )
            )
            continue
        for schema, name in targets:
            if not name.startswith(f"{owns_prefix}_"):
                violations.append(
                    PrefixViolation(
                        package_name=package_name,
                        message=(
                            f"{sql_file.name} creates {schema}.{name} but "
                            f"every write must start with '{owns_prefix}_'"
                        ),
                        surface="sql_write",
                        offender=f"{schema}.{name}",
                    )
                )
    return violations


def validate_schema_filenames(
    *,
    package_name: str,
    owns_prefix: str,
    schema_files: Iterable[Path],
) -> list[PrefixViolation]:
    """Confirm every SQL file in schema/ matches (raw|app)_<prefix>_*.sql.

    Only raw and app are allowed: packages own their raw landing tables and
    app-layer state, but core/prep/reports are framework-managed layers
    (SQLMesh models, staging views) a package must not write DDL into — those
    surfaces come from models/, not schema/.
    """
    violations: list[PrefixViolation] = []
    for sql_file in schema_files:
        name = sql_file.name
        if not (
            name.startswith(f"raw_{owns_prefix}_")
            or name.startswith(f"app_{owns_prefix}_")
        ):
            violations.append(
                PrefixViolation(
                    package_name=package_name,
                    message=(
                        f"schema file {name} must match (raw|app)_{owns_prefix}_*.sql"
                    ),
                    surface="schema_file",
                    offender=name,
                )
            )
    return violations


def validate_mcp_tool_prefixes(
    *,
    package_name: str,
    owns_prefix: str,
    tool_names: Iterable[str],
) -> list[PrefixViolation]:
    """Confirm every registered MCP tool starts with '<prefix>_'."""
    violations: list[PrefixViolation] = []
    for tool_name in tool_names:
        if not tool_name.startswith(f"{owns_prefix}_"):
            violations.append(
                PrefixViolation(
                    package_name=package_name,
                    message=(
                        f"MCP tool '{tool_name}' must start with '{owns_prefix}_'"
                    ),
                    surface="mcp_tool",
                    offender=tool_name,
                )
            )
    return violations


def validate_cli_prefixes(
    *,
    package_name: str,
    owns_prefix: str,
    cli_commands: Iterable[str],
) -> list[PrefixViolation]:
    """Confirm every CLI command lives under the package's subgroup.

    CLI subgroups use kebab-case per spec — owns_prefix 'us_tax' maps to
    'us-tax' in CLI surface. The validator accepts the kebab form directly.
    """
    cli_prefix = owns_prefix.replace("_", "-")
    violations: list[PrefixViolation] = []
    for command in cli_commands:
        # command == cli_prefix accepts the bare subgroup name (e.g. "us-tax");
        # Typer auto-generates it as a registered target even before subcommands run.
        if command == cli_prefix or command.startswith(f"{cli_prefix} "):
            continue
        violations.append(
            PrefixViolation(
                package_name=package_name,
                message=(
                    f"CLI command '{command}' must live under '{cli_prefix}' subgroup"
                ),
                surface="cli_command",
                offender=command,
            )
        )
    return violations
