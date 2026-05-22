"""End-to-end framework tests against the synthetic test package.

These tests don't mock discovery — they construct PackageInfo from the
synthetic package on disk and run the full validation + registration
pipeline. Tests covering discover_packages() against pip-installed entry
points live in test_discovery.py (mocked) — exercising live entry points
requires an editable install which would slow the test suite.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moneybin.packages._framework import (
    PackageInfo,
    PackageManifest,
    PackageRegistry,
    register_package,
    validate_package,
)


@pytest.fixture
def synthetic_info(synthetic_package_root: Path) -> PackageInfo:
    manifest = PackageManifest.from_yaml(
        synthetic_package_root / "moneybin_package.yaml"
    )
    return PackageInfo(manifest=manifest, root=synthetic_package_root)


def test_synthetic_package_validates_clean(synthetic_info: PackageInfo) -> None:
    """The bundled synthetic package passes every validator."""
    errors = validate_package(synthetic_info)
    assert errors == [], f"Synthetic package failed validation: {errors}"


def test_synthetic_package_registers_end_to_end(
    synthetic_info: PackageInfo,
) -> None:
    """Discover → validate → register → tools.register called → cli.register called.

    Patches _global_registry with a fresh instance to avoid collision with
    test_register_package_invokes_tools_and_cli (test_registry.py), which
    also registers a package named 'test_synthetic' to the module-level
    singleton in the same session.
    """
    # Reset module-level tracking from any prior test runs.
    importlib.import_module("test_synthetic.tools").reset()
    importlib.import_module("test_synthetic.cli").reset()

    mcp = MagicMock()
    cli = MagicMock()

    fresh_registry = PackageRegistry()
    with patch(
        "moneybin.packages._framework.registry._global_registry", fresh_registry
    ):
        register_package(info=synthetic_info, mcp=mcp, cli=cli)

    assert importlib.import_module("test_synthetic.tools").calls() == ["tools.register"]
    assert importlib.import_module("test_synthetic.cli").calls() == ["cli.register"]


def test_synthetic_package_capabilities_match_sql(
    synthetic_info: PackageInfo,
) -> None:
    """The manifest's declared writes cover every CREATE in schema/."""
    errors = validate_package(synthetic_info)
    capability_violations = [
        e for e in errors if type(e).__name__ == "CapabilityViolation"
    ]
    assert capability_violations == []


def test_breaking_synthetic_capability_surfaces_violation(
    synthetic_info: PackageInfo, tmp_path: Path
) -> None:
    """Adding a SQL leak to the synthetic package fires a CapabilityViolation."""
    # Copy the synthetic package to a temp dir so we can mutate it.
    import shutil

    target = tmp_path / "test_synthetic"
    shutil.copytree(synthetic_info.root, target)
    (target / "schema" / "leak.sql").write_text(
        "CREATE TABLE core.test_synthetic_leak (x INT);"
    )

    mutated = PackageInfo(manifest=synthetic_info.manifest, root=target)
    errors = validate_package(mutated)

    assert any(type(e).__name__ == "CapabilityViolation" for e in errors), (
        f"Expected CapabilityViolation; got: {[type(e).__name__ for e in errors]}"
    )


def test_breaking_synthetic_prefix_surfaces_violation(
    synthetic_info: PackageInfo, tmp_path: Path
) -> None:
    """A CREATE statement outside the prefix surfaces a PrefixViolation."""
    import shutil

    target = tmp_path / "test_synthetic"
    shutil.copytree(synthetic_info.root, target)
    (target / "schema" / "leak.sql").write_text("CREATE TABLE app.other_thing (x INT);")

    mutated = PackageInfo(manifest=synthetic_info.manifest, root=target)
    errors = validate_package(mutated)

    assert any(type(e).__name__ == "PrefixViolation" for e in errors), (
        f"Expected PrefixViolation; got: {[type(e).__name__ for e in errors]}"
    )


def test_registry_can_hold_multiple_packages(synthetic_info: PackageInfo) -> None:
    """PackageRegistry handles multiple add()/get() calls without conflict."""
    registry = PackageRegistry()
    registry.add(synthetic_info)
    assert registry.get("test_synthetic") is synthetic_info
    assert len(registry.all()) == 1
