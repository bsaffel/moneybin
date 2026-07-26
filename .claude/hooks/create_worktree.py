"""Create a Moneybin-compatible Git worktree for Claude Code."""

from __future__ import annotations

import json
import re
import subprocess  # noqa: S404  # The hook must invoke Git without a shell.
import sys
from pathlib import Path
from typing import TextIO

BRANCH_TYPES = frozenset({
    "feat",
    "fix",
    "docs",
    "refactor",
    "chore",
    "deps",
    "ci",
    "security",
    "test",
    "perf",
})
NAME_RE = re.compile(r"^(?P<kind>[a-z]+)\+(?P<summary>[a-z0-9]+(?:-[a-z0-9]+)*)$")


class HookError(Exception):
    """Raised when a worktree event cannot be completed safely."""


def parse_worktree_name(name: str) -> str:
    """Translate a safe native worktree name into a canonical Git branch."""
    match = NAME_RE.fullmatch(name)
    if match is None or match["kind"] not in BRANCH_TYPES:
        raise HookError("worktree name must use type+kebab-case-summary")
    return f"{match['kind']}/{match['summary']}"


def git_output(root: Path, *args: str) -> str:
    """Run Git from the repository root and return standard output."""
    result = subprocess.run(  # noqa: S603  # All argv values are validated local Git inputs.
        ["git", "-C", str(root), *args],  # noqa: S607  # Git is a required developer tool.
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or "Git command failed"
        raise HookError(detail)
    return result.stdout.strip()


def git_succeeds(root: Path, *args: str) -> bool:
    """Return whether Git accepts the requested read-only check."""
    return (
        subprocess.run(  # noqa: S603  # All argv values are validated local Git inputs.
            ["git", "-C", str(root), *args],  # noqa: S607  # Git is a required developer tool.
            capture_output=True,
            text=True,
        ).returncode
        == 0
    )


def repository_root(cwd: str) -> Path:
    """Resolve the current hook directory to its Git repository root."""
    if not isinstance(cwd, str) or not cwd:
        raise HookError("worktree hook input is missing cwd")
    return Path(git_output(Path(cwd), "rev-parse", "--show-toplevel"))


def base_ref(root: Path) -> str:
    """Use Claude's fresh-worktree base when the remote default is available."""
    remote_head = "refs/remotes/origin/HEAD"
    if git_succeeds(root, "rev-parse", "--verify", "--quiet", remote_head):
        return remote_head
    return "HEAD"


def create_worktree(root: Path, name: str) -> Path:
    """Validate and create the requested linked worktree."""
    branch = parse_worktree_name(name)
    if (root / ".worktreeinclude").exists():
        raise HookError("cannot create a custom worktree while .worktreeinclude exists")

    target = root / ".claude" / "worktrees" / name
    if target.exists():
        raise HookError(f"worktree path already exists: {target}")
    if git_succeeds(root, "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"):
        raise HookError(f"branch already exists: {branch}")

    target.parent.mkdir(parents=True, exist_ok=True)
    git_output(
        root, "worktree", "add", "--quiet", "-b", branch, str(target), base_ref(root)
    )
    return target


def main(
    stream: TextIO = sys.stdin, output: TextIO = sys.stdout, error: TextIO = sys.stderr
) -> int:
    """Create a worktree for a Claude Code WorktreeCreate JSON event."""
    try:
        event = json.load(stream)
        if not isinstance(event, dict):
            raise HookError("worktree hook input must be a JSON object")
        name = event.get("name")
        if not isinstance(name, str):
            raise HookError("worktree hook input is missing name")
        target = create_worktree(repository_root(event.get("cwd")), name)
    except (HookError, json.JSONDecodeError, OSError) as exc:
        print(f"Moneybin worktree hook: {exc}", file=error)
        return 1

    print(target, file=output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
