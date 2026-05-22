"""Tests for entry-points-based package discovery.

Real entry points require a real distribution install. We test discovery
against an in-memory mock via importlib.metadata.entry_points() patching.
Discovery resolves each manifest from distribution *metadata* (dist.files)
rather than importing the package, so the mocks supply a fake dist.files list
and locate_file() — never a loaded module.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moneybin.packages._framework.discovery import (
    PackageInfo,
    discover_packages,
)

_MANIFEST_BODY = """
name: fake_pkg
display_name: Fake
version: 1.0.0
quality_scale: bronze
owns_prefix: fake_pkg
publisher: {name: Test, verified: false}
description: ok
capabilities: {writes: [], reads: [], network: [], secrets: []}
requires: {moneybin: ">=1.0.0"}
entry_points:
  tools: fake_pkg.tools:register
  cli: fake_pkg.cli:register
  models: fake_pkg.models
  schema: fake_pkg.schema
""".strip()


def _fake_ep(
    pkg_dir: Path,
    *,
    name: str = "fake_pkg",
    module: str = "fake_pkg.tools",
    manifest_in_metadata: bool = True,
) -> MagicMock:
    """Build a fake EntryPoint whose dist exposes the manifest via file records.

    No `.load()` is configured — discovery must resolve the manifest from
    metadata without importing. A test that calls `.load()` would raise.
    """
    files: list[MagicMock] = []
    if manifest_in_metadata:
        manifest_pp = MagicMock()
        manifest_pp.name = "moneybin_package.yaml"
        manifest_pp.parts = (pkg_dir.name, "moneybin_package.yaml")
        files.append(manifest_pp)

    fake_dist = MagicMock()
    fake_dist.files = files
    fake_dist.locate_file.return_value = pkg_dir / "moneybin_package.yaml"

    fake_ep = MagicMock()
    fake_ep.name = name
    fake_ep.module = module
    fake_ep.dist = fake_dist
    # Importing during discovery is a security regression — make it loud.
    fake_ep.load.side_effect = AssertionError("discovery must not import the package")
    return fake_ep


def test_empty_entry_points_returns_empty_list() -> None:
    """When no packages are installed, discovery returns []."""
    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = []
        result = discover_packages()
    assert result == []


def test_single_entry_point_yields_one_package_info(tmp_path: Path) -> None:
    """An entry point whose dist records moneybin_package.yaml is loaded."""
    pkg_dir = tmp_path / "fake_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "moneybin_package.yaml").write_text(_MANIFEST_BODY)

    fake_ep = _fake_ep(pkg_dir, module="fake_pkg.tools")

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        result = discover_packages()

    assert len(result) == 1
    info = result[0]
    assert isinstance(info, PackageInfo)
    assert info.manifest.name == "fake_pkg"
    assert info.root == pkg_dir
    fake_ep.load.assert_not_called()


def test_manifest_not_in_metadata_skips_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A dist whose file records omit the manifest is skipped (no import attempt)."""
    pkg_dir = tmp_path / "broken_pkg"
    pkg_dir.mkdir()

    fake_ep = _fake_ep(pkg_dir, name="broken_pkg", manifest_in_metadata=False)

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        with caplog.at_level("ERROR"):
            result = discover_packages()

    assert result == []
    assert any("moneybin_package.yaml" in rec.message for rec in caplog.records)
    fake_ep.load.assert_not_called()


def test_no_dist_skips_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """An entry point with no resolvable distribution is skipped."""
    fake_ep = _fake_ep(tmp_path, name="no_dist_pkg")
    fake_ep.dist = None

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        with caplog.at_level("ERROR"):
            result = discover_packages()

    assert result == []
    assert any("could not locate" in rec.message.lower() for rec in caplog.records)


def test_manifest_recorded_but_absent_on_disk_skips(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A manifest in the file records but missing on disk is skipped."""
    pkg_dir = tmp_path / "ghost_pkg"
    pkg_dir.mkdir()
    # Manifest recorded in metadata but never written to disk.

    fake_ep = _fake_ep(pkg_dir, name="ghost_pkg")

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        with caplog.at_level("ERROR"):
            result = discover_packages()

    assert result == []
    assert any("not present on disk" in rec.message.lower() for rec in caplog.records)


def test_invalid_manifest_skips_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """An entry point with an invalid manifest logs the validation error."""
    pkg_dir = tmp_path / "bad_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "moneybin_package.yaml").write_text("not a manifest at all")

    fake_ep = _fake_ep(pkg_dir, name="bad_pkg")

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        with caplog.at_level("ERROR"):
            result = discover_packages()

    assert result == []
    assert any("invalid manifest" in rec.message.lower() for rec in caplog.records)


def test_yaml_syntax_error_skips_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A manifest with malformed YAML syntax is skipped, not crashed."""
    pkg_dir = tmp_path / "bad_yaml_pkg"
    pkg_dir.mkdir()
    # Malformed YAML: unclosed bracket causes yaml.safe_load to raise YAMLError.
    (pkg_dir / "moneybin_package.yaml").write_text("name: [unclosed\n  bad: : :")

    fake_ep = _fake_ep(pkg_dir, name="bad_yaml_pkg")

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        with caplog.at_level("ERROR"):
            result = discover_packages()

    assert result == []
    assert any("invalid manifest" in rec.message.lower() for rec in caplog.records)
