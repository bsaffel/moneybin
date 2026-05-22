"""Tests for entry-points-based package discovery.

Real entry-points require a real distribution install. We test discovery
against an in-memory mock via importlib.metadata.entry_points() patching.
The end-to-end smoke (against the synthetic test package installed via
pip install -e) lives in test_framework.py.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moneybin.packages._framework.discovery import (
    PackageInfo,
    discover_packages,
)


def test_empty_entry_points_returns_empty_list() -> None:
    """When no packages are installed, discovery returns []."""
    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = []
        result = discover_packages()
    assert result == []


def test_single_entry_point_yields_one_package_info(tmp_path: Path) -> None:
    """An entry point pointing to a module with moneybin_package.yaml is loaded."""
    pkg_dir = tmp_path / "fake_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "moneybin_package.yaml").write_text(
        """
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
    )

    fake_module = MagicMock()
    fake_module.__file__ = str(pkg_dir / "__init__.py")
    fake_module.__name__ = "fake_pkg"

    fake_ep = MagicMock()
    fake_ep.name = "fake_pkg"
    fake_ep.load.return_value = fake_module

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        result = discover_packages()

    assert len(result) == 1
    info = result[0]
    assert isinstance(info, PackageInfo)
    assert info.manifest.name == "fake_pkg"
    assert info.root == pkg_dir


def test_missing_manifest_skips_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """An entry point without moneybin_package.yaml logs an error and is skipped."""
    pkg_dir = tmp_path / "broken_pkg"
    pkg_dir.mkdir()
    # No moneybin_package.yaml deliberately.

    fake_module = MagicMock()
    fake_module.__file__ = str(pkg_dir / "__init__.py")
    fake_module.__name__ = "broken_pkg"

    fake_ep = MagicMock()
    fake_ep.name = "broken_pkg"
    fake_ep.load.return_value = fake_module

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        with caplog.at_level("ERROR"):
            result = discover_packages()

    assert result == []
    assert any("moneybin_package.yaml" in rec.message for rec in caplog.records)


def test_invalid_manifest_skips_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """An entry point with an invalid manifest logs the validation error."""
    pkg_dir = tmp_path / "bad_pkg"
    pkg_dir.mkdir()
    (pkg_dir / "moneybin_package.yaml").write_text("not a manifest at all")

    fake_module = MagicMock()
    fake_module.__file__ = str(pkg_dir / "__init__.py")
    fake_module.__name__ = "bad_pkg"

    fake_ep = MagicMock()
    fake_ep.name = "bad_pkg"
    fake_ep.load.return_value = fake_module

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

    fake_module = MagicMock()
    fake_module.__file__ = str(pkg_dir / "__init__.py")
    fake_module.__name__ = "bad_yaml_pkg"

    fake_ep = MagicMock()
    fake_ep.name = "bad_yaml_pkg"
    fake_ep.load.return_value = fake_module

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        with caplog.at_level("ERROR"):
            result = discover_packages()

    assert result == []
    assert any("invalid manifest" in rec.message.lower() for rec in caplog.records)


def test_entry_point_without_file_skips_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An entry point whose module has no __file__ is skipped with an error."""
    fake_module = MagicMock(spec=[])  # spec=[] → no __file__ attribute
    fake_module.__name__ = "no_file_pkg"

    fake_ep = MagicMock()
    fake_ep.name = "no_file_pkg"
    fake_ep.load.return_value = fake_module

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        with caplog.at_level("ERROR"):
            result = discover_packages()

    assert result == []
    assert any("__file__" in rec.message for rec in caplog.records)
