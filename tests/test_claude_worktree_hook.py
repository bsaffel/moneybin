"""Regression coverage for Moneybin's Claude worktree creation adapter."""

from __future__ import annotations

import json
import re
import subprocess  # noqa: S404  # The tests exercise real Git worktree behavior.
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
HOOK_PATH = REPOSITORY_ROOT / ".claude" / "hooks" / "create_worktree.py"
BRANCHING_RULE = REPOSITORY_ROOT / ".claude" / "rules" / "branching.md"


def git(cwd: Path, *args: str) -> str:
    """Run Git in a disposable test repository and return stdout."""
    result = subprocess.run(  # noqa: S603  # Test fixtures supply every Git argument.
        ["git", *args],  # noqa: S607  # Git is required to construct the fixture.
        check=True,
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def initialise_repository(path: Path) -> Path:
    """Create a committed repository that can host a linked worktree."""
    path.mkdir()
    git(path, "init", "--initial-branch=main")
    git(path, "config", "user.name", "Moneybin test")
    git(path, "config", "user.email", "test@example.com")
    (path / "README.md").write_text("fixture\n")
    git(path, "add", "README.md")
    git(path, "commit", "-m", "Create fixture")
    return path


def invoke_hook(root: Path, name: str) -> subprocess.CompletedProcess[str]:
    """Run the hook as Claude Code does, passing its JSON event through stdin."""
    return subprocess.run(  # noqa: S603  # Test-controlled Python executable and event payload.
        [sys.executable, str(HOOK_PATH)],
        input=json.dumps({"cwd": str(root), "name": name}),
        cwd=root,
        capture_output=True,
        text=True,
    )


def documented_branch_types() -> set[str]:
    """Extract the human-maintained type contract from Moneybin's rule."""
    return set(re.findall(r"\| `([a-z]+)/` \|", BRANCHING_RULE.read_text()))


def test_hook_creates_a_canonical_branch_for_a_native_worktree_name(
    tmp_path: Path,
) -> None:
    root = initialise_repository(tmp_path / "repository")

    result = invoke_hook(root, "fix+sqlmesh-console-noise")

    worktree = root / ".claude" / "worktrees" / "fix+sqlmesh-console-noise"
    assert result.returncode == 0, result.stderr
    assert result.stdout == f"{worktree}\n"
    assert git(worktree, "branch", "--show-current") == "fix/sqlmesh-console-noise"


@pytest.mark.parametrize("branch_type", sorted(documented_branch_types()))
def test_hook_accepts_every_documented_branch_type(
    tmp_path: Path,
    branch_type: str,
) -> None:
    root = initialise_repository(tmp_path / "repository")

    result = invoke_hook(root, f"{branch_type}+branch-policy")

    worktree = root / ".claude" / "worktrees" / f"{branch_type}+branch-policy"
    assert result.returncode == 0, result.stderr
    assert git(worktree, "branch", "--show-current") == f"{branch_type}/branch-policy"


@pytest.mark.parametrize(
    "name",
    [
        "worktree-fix+noise",
        "fix/noise",
        "fix+UPPER",
        "unknown+noise",
        "fix+has+extra-separator",
        "fix+../escape",
    ],
)
def test_hook_rejects_an_invalid_native_worktree_name(
    tmp_path: Path,
    name: str,
) -> None:
    root = initialise_repository(tmp_path / "repository")

    result = invoke_hook(root, name)

    assert result.returncode == 1
    assert result.stdout == ""
    assert "type+kebab-case-summary" in result.stderr
    assert git(root, "branch", "--list", "*") == "* main"


def test_hook_rejects_a_worktreeinclude_before_creating_a_branch(
    tmp_path: Path,
) -> None:
    root = initialise_repository(tmp_path / "repository")
    (root / ".worktreeinclude").write_text(".env\n")

    result = invoke_hook(root, "fix+needs-env")

    assert result.returncode == 1
    assert result.stdout == ""
    assert "worktreeinclude" in result.stderr
    assert not (root / ".claude" / "worktrees" / "fix+needs-env").exists()
    assert git(root, "branch", "--list", "fix/needs-env") == ""


def test_hook_rejects_an_existing_canonical_branch(
    tmp_path: Path,
) -> None:
    root = initialise_repository(tmp_path / "repository")
    git(root, "branch", "fix/already-exists")

    result = invoke_hook(root, "fix+already-exists")

    assert result.returncode == 1
    assert result.stdout == ""
    assert "already exists" in result.stderr


def test_hook_rejects_an_existing_target_directory(
    tmp_path: Path,
) -> None:
    root = initialise_repository(tmp_path / "repository")
    target = root / ".claude" / "worktrees" / "fix+target-exists"
    target.mkdir(parents=True)

    result = invoke_hook(root, "fix+target-exists")

    assert result.returncode == 1
    assert result.stdout == ""
    assert "already exists" in result.stderr


def test_hook_uses_origin_head_when_a_remote_default_ref_exists(
    tmp_path: Path,
) -> None:
    remote = initialise_repository(tmp_path / "remote")
    remote_bare = tmp_path / "remote.git"
    git(tmp_path, "init", "--bare", str(remote_bare))
    git(remote, "remote", "add", "origin", str(remote_bare))
    git(remote, "push", "-u", "origin", "main")
    git(remote_bare, "symbolic-ref", "HEAD", "refs/heads/main")
    root = tmp_path / "repository"
    git(tmp_path, "clone", str(remote_bare), str(root))
    git(root, "config", "user.name", "Moneybin test")
    git(root, "config", "user.email", "test@example.com")
    (root / "LOCAL.md").write_text("local only\n")
    git(root, "add", "LOCAL.md")
    git(root, "commit", "-m", "Add local-only commit")

    result = invoke_hook(root, "fix+remote-base")

    worktree = root / ".claude" / "worktrees" / "fix+remote-base"
    assert result.returncode == 0, result.stderr
    assert git(worktree, "rev-parse", "HEAD") == git(
        root,
        "rev-parse",
        "refs/remotes/origin/HEAD",
    )
    assert not (worktree / "LOCAL.md").exists()
