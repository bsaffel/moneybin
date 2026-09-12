"""The single source for the version and revision this process is running.

``system_status``'s build block and every export manifest's provenance derive
their build stamp from here. Never recompute either value at a second call
site — see AGENTS.md's coherence rule.
"""

from __future__ import annotations

import subprocess  # noqa: S404  # subprocess used for git rev-parse; static args only
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

#: Bound on the `git rev-parse` probe. Generous for a local repository read;
#: it exists so a wedged git can never delay import indefinitely.
_GIT_REVISION_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class BuildInfo:
    """The version and source revision this process loaded at boot."""

    version: str | None
    revision: str | None


def read_git_revision(root: Path) -> str | None:
    """Return the commit ``root`` is checked out at, or None when it can't be read.

    None covers every unreadable case, not just a non-repository: no ``git`` on
    PATH, a launch failure, a timeout, an unparseable answer, or a ``root`` that
    is inside a checkout rather than its top level.

    Delegates to ``git`` rather than parsing ``.git`` by hand: a linked worktree
    stores a *file* there pointing elsewhere, and a branch tip may live in
    ``packed-refs`` rather than as a loose ref. Both are cases this project
    actually runs in, and both are ones a hand-rolled reader gets wrong.

    ``root`` must be the repository's own top level, and that is checked rather
    than assumed. ``git`` answers about the nearest enclosing checkout, so an
    installed wheel sitting under someone else's repository — a virtualenv
    inside the user's own project, which is where ``uv`` puts one by default —
    would otherwise report *that* project's HEAD as MoneyBin's build: a
    confidently wrong stamp, worse than the documented ``null``.
    """
    try:
        completed = subprocess.run(  # noqa: S603  # git with static args
            [  # noqa: S607
                "git",
                "-C",
                str(root),
                "rev-parse",
                "--show-toplevel",
                "HEAD",
            ],
            capture_output=True,
            text=True,
            timeout=_GIT_REVISION_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        # No git on PATH, or it failed to launch. An unknown revision is a
        # perfectly good answer here; a broken caller is not.
        return None
    if completed.returncode != 0:
        return None
    # Split on line terminators, never on arbitrary whitespace: git prints the
    # top level unquoted, so a checkout path containing a space would otherwise
    # parse as three tokens and silently blank the stamp — and a blank reads as
    # a legitimate wheel install, hiding the failure.
    lines = completed.stdout.splitlines()
    if len(lines) != 2:
        return None
    toplevel, revision = lines
    if Path(toplevel).resolve() != root.resolve():
        return None
    return revision or None


#: Resolved once at import rather than per call. The whole point of reporting a
#: revision is to name the code this process actually loaded, and a checkout
#: can move underneath a long-lived server. A per-call read would then report
#: the *new* commit while still running the old code, which is worse than
#: reporting nothing: it would corroborate exactly the wrong conclusion.
#:
#: ``parents[2]`` is the checkout root of a source tree laid out as
#: ``<root>/src/moneybin/build_info.py``; from an installed wheel it is some
#: directory above ``site-packages``, which is not a repository top level and
#: so answers None.
_REVISION: str | None = read_git_revision(Path(__file__).resolve().parents[2])


def read_package_version() -> str | None:
    """Return the installed distribution version, or None when it has none.

    Guarded rather than allowed to propagate: this value is resolved at import,
    so an unreadable distribution would otherwise take down the whole process
    at boot. A missing version is a fine answer here; a process that will not
    start is not.
    """
    try:
        return version("moneybin")
    except PackageNotFoundError:
        return None


#: Captured at import for the same reason as ``_REVISION``, and it is the more
#: dangerous of the two to read late. ``version()`` re-reads the installed
#: distribution's metadata on every call, so upgrading the environment beneath a
#: long-lived process reports the *new* version while it still runs the old
#: modules — and a wheel install answers ``revision: None``, leaving this field
#: as the only signal a caller has. Reading it per call therefore produces a
#: confident wrong answer in precisely the stale-process case this module
#: exists to diagnose.
_VERSION: str | None = read_package_version()


def get_build_info() -> BuildInfo:
    """Return the version and revision this process loaded at boot."""
    return BuildInfo(version=_VERSION, revision=_REVISION)
