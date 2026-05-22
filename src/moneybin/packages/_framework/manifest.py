"""PackageManifest — Pydantic model for moneybin_package.yaml.

The manifest is a package's declared contract. Loading it via
PackageManifest.from_yaml() validates structure (required fields, types,
semver) and coherence (name matches owns_prefix). Capability declarations
nested in the manifest are validated separately (see capabilities.py)
because they require SQL inspection.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

QualityTier = Literal["bronze", "silver", "gold", "platinum"]

# owns_prefix must be lowercase snake_case — ^[a-z][a-z0-9_]*$.
# DuckDB normalizes identifiers to lowercase; every package surface (tables,
# tool names, schema files) derives from this prefix. A non-lowercase prefix
# would never match the lowercased SQL targets the prefix validators check,
# producing spurious violations. Reject at parse time.
_PREFIX_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Strict semver matcher — the canonical https://semver.org grammar.
# Accepts 1.0.0, 1.2.3-beta, 1.0.0-rc.1, 1.0.0+build.1; rejects single-digit
# ("v1") and numeric prerelease identifiers with leading zeros ("1.0.0-01",
# forbidden by SemVer rule 9). Build-metadata identifiers may have leading
# zeros (rule 10), so only the prerelease alternatives constrain them.
_SEMVER_RE = re.compile(
    r"^(?P<major>0|[1-9]\d*)\."
    r"(?P<minor>0|[1-9]\d*)\."
    r"(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<prerelease>"
    r"(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*"
    r"))?"
    r"(?:\+(?P<build>[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)


class Publisher(BaseModel):
    """Manifest-declared publisher metadata."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    url: str | None = None
    verified: bool = False


class CapabilityDeclarations(BaseModel):
    """The capabilities block — see capabilities.py for the validator."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    writes: list[str] = Field(default_factory=list)
    reads: list[str] = Field(default_factory=list)
    network: list[str] = Field(default_factory=list)
    secrets: list[str] = Field(default_factory=list)


class Requires(BaseModel):
    """Required dependency declarations."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    moneybin: str  # PEP-440 version spec, e.g. ">=1.0.0,<2.0.0"


class EntryPoints(BaseModel):
    """Module paths for the framework to import at registration time.

    All four are required. `tools` and `cli` are consumed today by
    register_package; `models` and `schema` are required for forward-compat —
    Plan 4 wires model-path and schema registration and will read them. A
    SQL-less Bronze package still declares them (pointing at its package module)
    so the manifest contract stays uniform across tiers.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    tools: str  # "module.path:callable" — invoked as tools.register(mcp)
    cli: str  # "module.path:callable" — invoked as cli.register(typer_app)
    models: str  # dotted module path to the models/ directory (Plan 4)
    schema_module: str = Field(
        alias="schema"
    )  # dotted module path to the schema/ directory (Plan 4)


class PackageManifest(BaseModel):
    """Parsed moneybin_package.yaml manifest.

    Loaded once at framework startup for each discovered package; held in
    PackageRegistry for the process lifetime. The manifest is the contract
    every validator and the registration orchestrator consume.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    display_name: str
    version: str
    quality_scale: QualityTier
    owns_prefix: str
    publisher: Publisher
    # Named maintainer responsible for the package — distinct from publisher
    # (who distributes). Optional at Bronze; the Silver tier check requires it
    # (spec §Quality Scale: "code-owner declared in manifest").
    code_owner: str | None = None
    description: str
    capabilities: CapabilityDeclarations
    requires: Requires
    entry_points: EntryPoints

    @field_validator("owns_prefix")
    @classmethod
    def _prefix_is_lowercase_snake(cls, value: str) -> str:
        """owns_prefix must be lowercase snake_case.

        DuckDB normalizes identifiers to lowercase, and every package surface
        (tables, tool names, schema files) derives from this prefix. A
        non-lowercase prefix would never match the lowercased SQL targets the
        validators check, producing spurious violations. Reject it at parse time.
        """
        if not _PREFIX_RE.match(value):
            raise ValueError(
                f"owns_prefix '{value}' must be lowercase snake_case "
                f"(matching ^[a-z][a-z0-9_]*$)"
            )
        return value

    @model_validator(mode="after")
    def _name_matches_prefix(self) -> PackageManifest:
        """The package name must equal owns_prefix.

        The two could in principle differ — but every spec example treats
        them as identical, and the prefix is what every surface (tables,
        tools, CLI) actually uses. Forcing equality eliminates an entire
        class of contributor confusion.
        """
        if self.name != self.owns_prefix:
            raise ValueError(
                f"name '{self.name}' must match owns_prefix '{self.owns_prefix}'"
            )
        return self

    @model_validator(mode="after")
    def _version_is_semver(self) -> PackageManifest:
        if not _SEMVER_RE.match(self.version):
            raise ValueError(
                f"version '{self.version}' is not valid semver (e.g. '1.0.0')"
            )
        return self

    @classmethod
    def from_yaml(cls, path: Path) -> PackageManifest:
        """Load and validate a manifest from a YAML file on disk.

        Raises:
            pydantic.ValidationError: on schema / coherence violations.
            yaml.YAMLError: on malformed YAML syntax.
        """
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return cls.model_validate(data)
