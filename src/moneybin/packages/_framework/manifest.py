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

# owns_prefix must be lowercase snake_case — letters/digits, single internal
# underscores, no leading/trailing/doubled underscore. DuckDB normalizes
# identifiers to lowercase; every package surface (tables, tool names, schema
# files) derives from this prefix. A non-conforming prefix would never match
# the lowercased SQL targets the prefix validators check. Reject at parse time.
_PREFIX_RE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")

# Schema names a package may never claim as its owns_prefix — using one would
# let a package's tables collide with framework-managed schemas. Per spec
# §"Naming and prefix discipline": "declared prefix does not overlap with core
# or another extension." 'main' is DuckDB's default schema.
_RESERVED_PREFIXES = frozenset({
    "raw",
    "prep",
    "core",
    "app",
    "reports",
    "meta",
    "seeds",
    "synthetic",
    "main",
})

# Schemas a package may name in its capabilities.writes globs. raw/app are
# written by schema/ bootstrap DDL; reports is written ONLY by the package's
# models/ directory (SQLMesh views), never by schema/ — that split is enforced
# by capabilities.validate_schema_layers, which confines schema/ CREATE targets
# to {raw, app}. core/prep are framework-managed (SQLMesh), and
# meta/seeds/synthetic are infrastructure, so none of those are writable.
# (models/ write-glob validation lands in Plan 4 alongside reference packages;
# until then a reports.* glob is a forward-compat declaration, satisfied by
# models/ — declaring it must not let schema/ DDL reach the reports layer.)
_WRITABLE_SCHEMAS = frozenset({"raw", "app", "reports"})

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
                f"(matching {_PREFIX_RE.pattern})"
            )
        if value in _RESERVED_PREFIXES:
            raise ValueError(
                f"owns_prefix '{value}' is a reserved schema name; pick a prefix "
                f"that does not collide with {sorted(_RESERVED_PREFIXES)}"
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

    @model_validator(mode="after")
    def _writes_are_prefix_scoped(self) -> PackageManifest:
        """Every write glob must be '<writable-schema>.<owns_prefix>_*'.

        capabilities.writes is the load-bearing security primitive. An
        unscoped glob defeats it: a wildcard schema ('*.x') or a bare
        schema claim ('app.*') would let a CREATE land anywhere. Require an
        explicit, package-writable schema and a table portion that starts with
        the package's own prefix, so the declared contract matches what the
        prefix validator actually enforces on the SQL.
        """
        for glob in self.capabilities.writes:
            schema, sep, table = glob.partition(".")
            if not sep:
                raise ValueError(
                    f"write glob '{glob}' must be "
                    f"'<schema>.{self.owns_prefix}_*' (explicit schema required)"
                )
            if set(schema) & set("*?[]"):
                raise ValueError(
                    f"write glob '{glob}' must name an explicit schema, not a wildcard"
                )
            if schema not in _WRITABLE_SCHEMAS:
                raise ValueError(
                    f"write glob '{glob}' targets schema '{schema}', which is "
                    f"not package-writable (allowed: {sorted(_WRITABLE_SCHEMAS)})"
                )
            if not table.startswith(f"{self.owns_prefix}_"):
                raise ValueError(
                    f"write glob '{glob}' table must start with "
                    f"'{self.owns_prefix}_' to stay inside the package's prefix"
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
