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
    module: str | None = None,
    manifest_in_metadata: bool = True,
) -> MagicMock:
    """Build a fake EntryPoint whose dist exposes the manifest via file records.

    No `.load()` is configured — discovery must resolve the manifest from
    metadata without importing. A test that calls `.load()` would raise.

    module defaults to ``<pkg_dir.name>.tools`` so the entry point's module path
    sits under the manifest's directory (the realistic layout _locate_manifest
    matches on). Pass module explicitly to exercise a mismatch.
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
    fake_ep.module = module if module is not None else f"{pkg_dir.name}.tools"
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
    assert any("could not resolve" in rec.message.lower() for rec in caplog.records)


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


def test_unreadable_manifest_skips_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """An OSError reading the manifest is caught — one bad package can't abort all.

    A manifest path that exists but is a directory raises IsADirectoryError
    (an OSError) from open(); discovery must skip it, not propagate.
    """
    pkg_dir = tmp_path / "oserror_pkg"
    pkg_dir.mkdir()
    # The "manifest" is a directory, so open("r") raises IsADirectoryError.
    (pkg_dir / "moneybin_package.yaml").mkdir()

    fake_ep = _fake_ep(pkg_dir, name="oserror_pkg")

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        with caplog.at_level("ERROR"):
            result = discover_packages()

    assert result == []
    assert any(
        "could not be discovered" in rec.message.lower() for rec in caplog.records
    )


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
    assert any(
        "could not be discovered" in rec.message.lower() for rec in caplog.records
    )


def _manifest_pp(parts: tuple[str, ...]) -> MagicMock:
    """A fake dist file record for a manifest at the given path parts."""
    pp = MagicMock()
    pp.name = "moneybin_package.yaml"
    pp.parts = parts
    return pp


def test_multi_manifest_picks_most_specific(tmp_path: Path) -> None:
    """A package-dir manifest beats a root-level manifest for the same module.

    A root-level manifest matches every module (empty dir prefix); the package
    whose directory is the longest prefix of the entry point's module must win,
    not the root-level one.
    """
    pkg_dir = tmp_path / "pkg_a"
    pkg_dir.mkdir()
    (pkg_dir / "moneybin_package.yaml").write_text(
        _MANIFEST_BODY.replace("fake_pkg", "pkg_a")
    )
    root_manifest = tmp_path / "moneybin_package.yaml"
    root_manifest.write_text(_MANIFEST_BODY)  # name=fake_pkg if wrongly chosen

    root_pp = _manifest_pp(("moneybin_package.yaml",))
    sub_pp = _manifest_pp(("pkg_a", "moneybin_package.yaml"))
    fake_dist = MagicMock()
    fake_dist.files = [root_pp, sub_pp]

    def _locate(pp: object) -> Path:
        return root_manifest if pp is root_pp else pkg_dir / "moneybin_package.yaml"

    fake_dist.locate_file.side_effect = _locate

    fake_ep = MagicMock()
    fake_ep.name = "pkg_a"
    fake_ep.module = "pkg_a.tools"
    fake_ep.dist = fake_dist
    fake_ep.load.side_effect = AssertionError("discovery must not import")

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        result = discover_packages()

    assert len(result) == 1
    assert result[0].manifest.name == "pkg_a"
    assert result[0].root == pkg_dir


def test_single_manifest_not_matching_module_is_skipped(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A lone manifest that doesn't prefix the entry point's module is not guessed.

    There is no single-candidate fallback: assigning the wrong manifest would
    mis-scope every validator in a multi-entry-point distribution.
    """
    pkg_dir = tmp_path / "pkg_a"
    pkg_dir.mkdir()
    (pkg_dir / "moneybin_package.yaml").write_text(_MANIFEST_BODY)

    sub_pp = _manifest_pp(("pkg_a", "moneybin_package.yaml"))
    fake_dist = MagicMock()
    fake_dist.files = [sub_pp]
    fake_dist.locate_file.return_value = pkg_dir / "moneybin_package.yaml"

    fake_ep = MagicMock()
    fake_ep.name = "unrelated"
    fake_ep.module = "unrelated.tools"  # not under pkg_a/
    fake_ep.dist = fake_dist
    fake_ep.load.side_effect = AssertionError("discovery must not import")

    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = [fake_ep]
        with caplog.at_level("ERROR"):
            result = discover_packages()

    assert result == []
    assert any("could not resolve" in rec.message.lower() for rec in caplog.records)


def test_duplicate_manifest_across_entry_points_discovered_once(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Two entry points resolving to the same manifest yield one PackageInfo.

    A malformed dist sharing one (e.g. root-level) manifest across several entry
    points must not produce duplicate PackageInfos — that would crash
    register_package with an opaque 'already registered'. Discover once, warn.
    """
    root_manifest = tmp_path / "moneybin_package.yaml"
    root_manifest.write_text(_MANIFEST_BODY)
    root_pp = _manifest_pp(("moneybin_package.yaml",))

    def _mk_ep(name: str, module: str) -> MagicMock:
        dist = MagicMock()
        dist.files = [root_pp]
        dist.locate_file.return_value = root_manifest
        ep = MagicMock()
        ep.name = name
        ep.module = module
        ep.dist = dist
        ep.load.side_effect = AssertionError("discovery must not import")
        return ep

    eps = [_mk_ep("pkg_a", "pkg_a.tools"), _mk_ep("pkg_b", "pkg_b.tools")]
    with patch("moneybin.packages._framework.discovery.entry_points") as mock_eps:
        mock_eps.return_value = eps
        with caplog.at_level("WARNING"):
            result = discover_packages()

    assert len(result) == 1
    assert any("already discovered" in rec.message.lower() for rec in caplog.records)


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
    assert any(
        "could not be discovered" in rec.message.lower() for rec in caplog.records
    )
