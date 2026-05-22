"""Tests for PackageManifest parsing and validation.

The manifest model is the framework's first line of defence — malformed
manifests fail to parse, eliminating downstream cascading failures.
"""

from pathlib import Path
from textwrap import dedent

import pytest
from pydantic import ValidationError as PydanticValidationError

from moneybin.packages._framework.manifest import PackageManifest


def _write_manifest(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "moneybin_package.yaml"
    path.write_text(dedent(body).strip())
    return path


def test_valid_manifest_parses(tmp_path: Path) -> None:
    """A complete, well-formed manifest parses into a PackageManifest."""
    manifest_path = _write_manifest(
        tmp_path,
        """
        name: test_synthetic
        display_name: Test Synthetic
        version: 1.0.0
        quality_scale: bronze
        owns_prefix: test_synthetic
        publisher:
          name: MoneyBin Core
          url: https://moneybin.app
          verified: true
        description: |
          Minimal synthetic package used to exercise the framework.
        capabilities:
          writes:
            - app.test_synthetic_*
            - reports.test_synthetic_*
          reads:
            - core.fct_transactions
          network: []
          secrets: []
        requires:
          moneybin: ">=1.0.0,<2.0.0"
        entry_points:
          tools: moneybin.packages.test_synthetic.tools:register
          cli: moneybin.packages.test_synthetic.cli:register
          models: moneybin.packages.test_synthetic.models
          schema: moneybin.packages.test_synthetic.schema
        """,
    )

    manifest = PackageManifest.from_yaml(manifest_path)

    assert manifest.name == "test_synthetic"
    assert manifest.quality_scale == "bronze"
    assert manifest.owns_prefix == "test_synthetic"
    assert manifest.publisher.verified is True
    assert "app.test_synthetic_*" in manifest.capabilities.writes
    assert manifest.entry_points.tools == (
        "moneybin.packages.test_synthetic.tools:register"
    )


def test_name_must_match_owns_prefix(tmp_path: Path) -> None:
    """Coherence: a package's name and prefix must match.

    Mismatched name/prefix is the single most common authoring bug a
    contributor could make; surfacing it at manifest load saves the
    cascading failures it would otherwise cause downstream.
    """
    manifest_path = _write_manifest(
        tmp_path,
        """
        name: assets
        display_name: Assets
        version: 1.0.0
        quality_scale: bronze
        owns_prefix: real_estate
        publisher: {name: x, verified: false}
        description: ok
        capabilities: {writes: [], reads: [], network: [], secrets: []}
        requires: {moneybin: ">=1.0.0"}
        entry_points: {tools: x:y, cli: x:y, models: x, schema: x}
        """,
    )

    with pytest.raises(PydanticValidationError, match="must match owns_prefix"):
        PackageManifest.from_yaml(manifest_path)


def test_quality_scale_must_be_known(tmp_path: Path) -> None:
    """Only bronze/silver/gold/platinum are valid claims."""
    manifest_path = _write_manifest(
        tmp_path,
        """
        name: test_synthetic
        display_name: Test
        version: 1.0.0
        quality_scale: titanium
        owns_prefix: test_synthetic
        publisher: {name: x, verified: false}
        description: ok
        capabilities: {writes: [], reads: [], network: [], secrets: []}
        requires: {moneybin: ">=1.0.0"}
        entry_points: {tools: x:y, cli: x:y, models: x, schema: x}
        """,
    )

    with pytest.raises(PydanticValidationError):
        PackageManifest.from_yaml(manifest_path)


def test_version_must_be_semver(tmp_path: Path) -> None:
    """Version field rejects non-semver strings."""
    manifest_path = _write_manifest(
        tmp_path,
        """
        name: test_synthetic
        display_name: Test
        version: v1
        quality_scale: bronze
        owns_prefix: test_synthetic
        publisher: {name: x, verified: false}
        description: ok
        capabilities: {writes: [], reads: [], network: [], secrets: []}
        requires: {moneybin: ">=1.0.0"}
        entry_points: {tools: x:y, cli: x:y, models: x, schema: x}
        """,
    )

    with pytest.raises(PydanticValidationError, match="semver"):
        PackageManifest.from_yaml(manifest_path)


def test_version_rejects_leading_zero_prerelease(tmp_path: Path) -> None:
    """SemVer forbids leading zeros in numeric prerelease identifiers (rule 9)."""
    manifest_path = _write_manifest(
        tmp_path,
        """
        name: test_synthetic
        display_name: Test
        version: 1.0.0-01
        quality_scale: bronze
        owns_prefix: test_synthetic
        publisher: {name: x, verified: false}
        description: ok
        capabilities: {writes: [], reads: [], network: [], secrets: []}
        requires: {moneybin: ">=1.0.0"}
        entry_points: {tools: x:y, cli: x:y, models: x, schema: x}
        """,
    )

    with pytest.raises(PydanticValidationError, match="semver"):
        PackageManifest.from_yaml(manifest_path)


def test_version_accepts_valid_prerelease(tmp_path: Path) -> None:
    """Valid prerelease/build metadata (1.2.3-rc.1+build.5) parses."""
    manifest_path = _write_manifest(
        tmp_path,
        """
        name: test_synthetic
        display_name: Test
        version: 1.2.3-rc.1+build.5
        quality_scale: bronze
        owns_prefix: test_synthetic
        publisher: {name: x, verified: false}
        description: ok
        capabilities: {writes: [], reads: [], network: [], secrets: []}
        requires: {moneybin: ">=1.0.0"}
        entry_points: {tools: x:y, cli: x:y, models: x, schema: x}
        """,
    )

    assert PackageManifest.from_yaml(manifest_path).version == "1.2.3-rc.1+build.5"


def test_owns_prefix_must_be_lowercase(tmp_path: Path) -> None:
    """A non-lowercase owns_prefix is rejected at parse time."""
    manifest_path = _write_manifest(
        tmp_path,
        """
        name: Assets
        display_name: Assets
        version: 1.0.0
        quality_scale: bronze
        owns_prefix: Assets
        publisher: {name: x, verified: false}
        description: ok
        capabilities: {writes: [], reads: [], network: [], secrets: []}
        requires: {moneybin: ">=1.0.0"}
        entry_points: {tools: x:y, cli: x:y, models: x, schema: x}
        """,
    )
    with pytest.raises(PydanticValidationError, match="lowercase snake_case"):
        PackageManifest.from_yaml(manifest_path)


def test_owns_prefix_snake_case_with_underscores_ok(tmp_path: Path) -> None:
    """A multi-word snake_case prefix like us_tax is accepted."""
    manifest_path = _write_manifest(
        tmp_path,
        """
        name: us_tax
        display_name: US Tax
        version: 1.0.0
        quality_scale: bronze
        owns_prefix: us_tax
        publisher: {name: x, verified: false}
        description: ok
        capabilities: {writes: [], reads: [], network: [], secrets: []}
        requires: {moneybin: ">=1.0.0"}
        entry_points: {tools: x:y, cli: x:y, models: x, schema: x}
        """,
    )
    manifest = PackageManifest.from_yaml(manifest_path)
    assert manifest.owns_prefix == "us_tax"


def test_missing_required_field_fails(tmp_path: Path) -> None:
    """Required fields are required."""
    manifest_path = _write_manifest(
        tmp_path,
        """
        name: test_synthetic
        version: 1.0.0
        quality_scale: bronze
        owns_prefix: test_synthetic
        publisher: {name: x, verified: false}
        description: ok
        capabilities: {writes: [], reads: [], network: [], secrets: []}
        requires: {moneybin: ">=1.0.0"}
        entry_points: {tools: x:y, cli: x:y, models: x, schema: x}
        """,
    )

    # display_name omitted
    with pytest.raises(PydanticValidationError):
        PackageManifest.from_yaml(manifest_path)
