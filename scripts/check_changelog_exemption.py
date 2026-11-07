"""Changelog guard: decide whether a PR is a Dependabot dependency update.

Such a PR needs no changelog.d fragment; release preparation records dependency
changes once per release instead. The decision reads the PR's author and diff,
never its labels: Dependabot applies labels after it opens a PR, so the
`opened` run would race them.
"""

import argparse
import fnmatch
import subprocess  # noqa: S404  # fixed git invocations, no shell
import sys
import tomllib
from typing import Any

DEPENDABOT = "dependabot[bot]"
PYPROJECT = "pyproject.toml"
_DEPENDENCY_FILES = ("uv.lock", PYPROJECT, ".github/workflows/*.yml")


def _without_dependency_tables(pyproject: dict[str, Any]) -> dict[str, Any]:
    """Return pyproject minus the tables a dependency update may edit."""
    stripped = {
        **pyproject,
        "project": dict(pyproject.get("project", {})),
        "build-system": dict(pyproject.get("build-system", {})),
    }
    stripped["project"].pop("dependencies", None)
    stripped["project"].pop("optional-dependencies", None)
    # Dependabot's uv ecosystem raises build requirements too (e.g. setuptools).
    stripped["build-system"].pop("requires", None)
    stripped.pop("dependency-groups", None)
    return stripped


def dependency_update_problem(
    author: str,
    changed_files: list[str],
    base_pyproject: str,
    head_pyproject: str,
) -> str | None:
    """Return why the PR is not a dependency update, or None when it is."""
    if author != DEPENDABOT:
        return f"PR author is {author}, not {DEPENDABOT}."
    other = [
        path
        for path in changed_files
        if not any(fnmatch.fnmatch(path, pattern) for pattern in _DEPENDENCY_FILES)
    ]
    if other:
        return f"PR changes files outside dependency manifests: {', '.join(other)}."
    if PYPROJECT in changed_files and _without_dependency_tables(
        tomllib.loads(base_pyproject)
    ) != _without_dependency_tables(tomllib.loads(head_pyproject)):
        return f"PR changes {PYPROJECT} outside its dependency tables."
    return None


def _git(*args: str) -> str:
    return subprocess.run(  # noqa: S603  # fixed git argv, no shell
        ["git", *args],  # noqa: S607  # git from PATH, as in CI
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def main() -> int:
    """Print true for a Dependabot dependency update, false otherwise."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--author", required=True)
    parser.add_argument("--base", required=True, help="base branch commit")
    parser.add_argument("--head", required=True, help="commit to compare with base")
    args = parser.parse_args()

    changed = _git("diff", "--name-only", f"{args.base}...{args.head}").splitlines()
    problem = dependency_update_problem(
        args.author,
        changed,
        _git("show", f"{args.base}:{PYPROJECT}"),
        _git("show", f"{args.head}:{PYPROJECT}"),
    )
    if problem:
        print(f"Not a Dependabot dependency update: {problem}", file=sys.stderr)  # noqa: T201  # script output
        print("false")  # noqa: T201  # script output
    else:
        print("Dependabot dependency update: no fragment required.", file=sys.stderr)  # noqa: T201  # script output
        print("true")  # noqa: T201  # script output
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
