"""Tests for scripts.check_release_metadata: the pre-publish consistency guard."""

from pathlib import Path

import pytest

from scripts.check_release_metadata import check_release_metadata

PYPROJECT = '[project]\nname = "moneybin"\nversion = "0.2.0"\n'
CHANGELOG = "# Changelog\n\n## [Unreleased]\n\n## [0.2.0] - 2026-07-11\n\n- thing\n"


@pytest.fixture
def files(tmp_path: Path) -> Path:
    (tmp_path / "pyproject.toml").write_text(PYPROJECT)
    (tmp_path / "CHANGELOG.md").write_text(CHANGELOG)
    return tmp_path


def test_accepts_a_matching_tag(files: Path) -> None:
    problems = check_release_metadata(
        "v0.2.0", files / "pyproject.toml", files / "CHANGELOG.md"
    )
    assert problems == []


def test_rejects_a_tag_that_does_not_match_pyproject(files: Path) -> None:
    problems = check_release_metadata(
        "v0.3.0", files / "pyproject.toml", files / "CHANGELOG.md"
    )
    assert any("0.3.0" in p and "0.2.0" in p for p in problems)


def test_rejects_a_version_with_no_changelog_section(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(PYPROJECT)
    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n\n## [Unreleased]\n")
    problems = check_release_metadata(
        "v0.2.0", tmp_path / "pyproject.toml", tmp_path / "CHANGELOG.md"
    )
    assert any("CHANGELOG" in p for p in problems)


def test_rejects_a_non_pep440_tag(files: Path) -> None:
    problems = check_release_metadata(
        "release-2", files / "pyproject.toml", files / "CHANGELOG.md"
    )
    assert problems
