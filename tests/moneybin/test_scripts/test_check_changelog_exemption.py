"""Tests for scripts.check_changelog_exemption: the Dependabot changelog guard."""

import subprocess  # noqa: S404  # builds a throwaway git repo for the CLI test
import sys
from pathlib import Path

import pytest

from scripts.check_changelog_exemption import (
    DEPENDABOT,
    dependency_update_problem,
    main,
)

BASE = """\
[project]
name = "moneybin"
version = "0.2.0"
dependencies = ["fastmcp>=4.0.8,<5"]

[project.optional-dependencies]
docs = ["mkdocs>=1.6.0"]

[dependency-groups]
dev = ["ruff>=0.17.0"]

[tool.ruff]
line-length = 88
"""

FLOOR_RAISE = BASE.replace("fastmcp>=4.0.8,<5", "fastmcp>=4.1.0,<5").replace(
    "ruff>=0.17.0", "ruff>=0.18.0"
)


@pytest.mark.parametrize(
    ("changed", "head"),
    [
        pytest.param(["pyproject.toml", "uv.lock"], FLOOR_RAISE, id="floor-raise"),
        pytest.param(["uv.lock"], BASE, id="lockfile-only-security-fix"),
        pytest.param(
            [".github/workflows/ci.yml", ".github/workflows/release.yml"],
            BASE,
            id="actions-pin",
        ),
        pytest.param(
            ["pyproject.toml", "uv.lock"],
            BASE.replace('docs = ["mkdocs>=1.6.0"]', 'docs = ["mkdocs>=1.7.0"]'),
            id="optional-dependency",
        ),
    ],
)
def test_exempts_dependabot_dependency_updates(changed: list[str], head: str) -> None:
    assert dependency_update_problem(DEPENDABOT, changed, BASE, head) is None


def test_requires_dependabot_as_author() -> None:
    problem = dependency_update_problem("someone", ["uv.lock"], BASE, BASE)
    assert problem is not None and "someone" in problem


def test_refuses_a_file_outside_the_dependency_manifests() -> None:
    changed = ["uv.lock", "src/moneybin/config.py"]
    problem = dependency_update_problem(DEPENDABOT, changed, BASE, BASE)
    assert problem is not None and "src/moneybin/config.py" in problem


@pytest.mark.parametrize(
    "head",
    [
        pytest.param(BASE.replace('"0.2.0"', '"0.3.0"'), id="version"),
        pytest.param(BASE.replace("88", "100"), id="tool-config"),
    ],
)
def test_refuses_a_pyproject_edit_outside_the_dependency_tables(head: str) -> None:
    problem = dependency_update_problem(DEPENDABOT, ["pyproject.toml"], BASE, head)
    assert problem is not None and "pyproject.toml" in problem


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(  # noqa: S603  # fixed git argv in a temp repo
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],  # noqa: S607  # git from PATH, as in CI
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.mark.parametrize(
    ("files", "expected"),
    [
        pytest.param(
            {"pyproject.toml": FLOOR_RAISE, "uv.lock": "b"}, "true", id="bump"
        ),
        pytest.param({"uv.lock": "b", "README.md": "x"}, "false", id="other-file"),
    ],
)
def test_cli_reads_the_diff_from_git(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    files: dict[str, str],
    expected: str,
) -> None:
    _git(tmp_path, "init", "-q")
    (tmp_path / "pyproject.toml").write_text(BASE)
    (tmp_path / "uv.lock").write_text("a")
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "base")
    base = _git(tmp_path, "rev-parse", "HEAD")
    for name, text in files.items():
        (tmp_path / name).write_text(text)
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-qm", "bump")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        sys, "argv", ["prog", "--author", DEPENDABOT, "--base", base, "--head", "HEAD"]
    )

    assert main() == 0
    assert capsys.readouterr().out.strip() == expected
