"""Pre-publish guard: the tag, pyproject version, and CHANGELOG must agree.

PyPI never permits reuse of a version number, so a wrong tag burns it
permanently. These checks run before anything is built or published.
"""

import argparse
import re
import sys
import tomllib
from pathlib import Path

_PEP440 = re.compile(r"^\d+\.\d+\.\d+([ab]\d+|rc\d+)?$")


def check_release_metadata(tag: str, pyproject: Path, changelog: Path) -> list[str]:
    """Return a list of problems; an empty list means the release may proceed."""
    problems: list[str] = []

    version = tag.removeprefix("v")
    if not _PEP440.match(version):
        problems.append(f"Tag '{tag}' is not a PEP 440 version (expected vX.Y.Z).")
        return problems  # everything downstream keys off a valid version

    declared = tomllib.loads(pyproject.read_text())["project"]["version"]
    if declared != version:
        problems.append(
            f"Tag '{tag}' says version {version}, but pyproject.toml declares "
            f"{declared}. Bump pyproject or fix the tag."
        )

    text = changelog.read_text()
    if f"## [{version}]" not in text:
        problems.append(
            f"CHANGELOG.md has no '## [{version}]' section. Cut the Unreleased "
            f"block into a dated section for this release."
        )

    return problems


def main() -> int:
    """Run the guard from the command line; exit non-zero if anything disagrees."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--pyproject", type=Path, default=Path("pyproject.toml"))
    parser.add_argument("--changelog", type=Path, default=Path("CHANGELOG.md"))
    args = parser.parse_args()

    problems = check_release_metadata(args.tag, args.pyproject, args.changelog)
    for problem in problems:
        print(f"❌ {problem}", file=sys.stderr)  # noqa: T201  # script output
    if problems:
        return 1
    print(f"✅ Release metadata for {args.tag} is consistent.")  # noqa: T201  # script output
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
