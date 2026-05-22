"""Shared exception types for the package framework.

Validation errors carry structured fields so callers (validator CLI / MCP
tool / framework startup) can surface precise diagnostics rather than raw
strings.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from moneybin.packages._framework.manifest import QualityTier

SurfaceKind = Literal["sql_write", "schema_file", "mcp_tool", "cli_command"]


@dataclass(frozen=True)
class ValidationError(Exception):
    """Base class for all package-validation failures."""

    package_name: str
    message: str

    def __str__(self) -> str:
        return f"[{self.package_name}] {self.message}"


@dataclass(frozen=True)
class CapabilityViolation(ValidationError):  # noqa: N818  # "Violation" distinguishes validation failure from unexpected error
    """A package's SQL writes to a table not covered by its declared capabilities.

    Raised when a CREATE TABLE / CREATE VIEW target falls outside every
    glob pattern in the manifest's capabilities.writes list.
    """

    sql_file: str
    target: str  # "schema.name" of the offending CREATE statement

    def __str__(self) -> str:
        return f"[{self.package_name}] {self.message} (file: {self.sql_file}, target: {self.target})"


@dataclass(frozen=True)
class PrefixViolation(ValidationError):  # noqa: N818  # "Violation" distinguishes validation failure from unexpected error
    """A package surface (SQL, MCP tool, CLI subcommand, schema file) violates owns_prefix.

    The owns_prefix is load-bearing per design-principles.md coherence rule —
    a package writing to or registering surfaces outside its declared prefix
    is a cross-prefix leak that must fail registration.
    """

    surface: SurfaceKind
    offender: str  # the offending name (table, tool, command, filename)

    def __str__(self) -> str:
        return f"[{self.package_name}] {self.message} (surface: {self.surface}, offender: {self.offender})"


@dataclass(frozen=True)
class QualityScaleViolation(ValidationError):  # noqa: N818  # "Violation" distinguishes validation failure from unexpected error
    """A package claims a Quality Scale tier it doesn't satisfy.

    Each tier's evidence is checked mechanically per spec §"Type-specific
    requirements". This violation captures which check failed so authors
    can demote the claim or supply the missing evidence.
    """

    claimed_tier: QualityTier
    missing_evidence: str  # human-readable description of the failed check

    def __str__(self) -> str:
        return f"[{self.package_name}] {self.message} (claimed: {self.claimed_tier}, missing: {self.missing_evidence})"
