"""``moneybin.build_info`` is the single source for the running build stamp.

Both ``system_status``'s build block and export manifest provenance derive
from ``get_build_info()`` — these tests exercise the derivation itself.
"""

from __future__ import annotations

import subprocess  # noqa: S404  # reads/writes git state to derive the expected value
from importlib.metadata import PackageNotFoundError
from pathlib import Path

import pytest

from moneybin.build_info import get_build_info, read_git_revision, read_package_version


@pytest.mark.unit
def test_version_is_resolved_at_import_not_per_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Upgrading the environment must not make a live process claim the new version.

    ``importlib.metadata.version`` re-reads the installed distribution's
    metadata on every call, so upgrading MoneyBin underneath a running process
    would report the *new* version while the process still holds the old
    modules. That is the stale-process scenario this module exists to
    diagnose, and it is the worst place to be confidently wrong: an installed
    wheel reports ``revision: null``, leaving ``version`` as the only signal.
    """
    captured = get_build_info().version

    def _upgraded(_name: str) -> str:
        return "99.99.99"

    monkeypatch.setattr("moneybin.build_info.version", _upgraded)

    assert get_build_info().version == captured


@pytest.mark.unit
def test_absent_distribution_metadata_reports_no_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unreadable metadata degrades the field; it must never fail the import.

    The version is resolved at import, so an unguarded lookup would turn a
    missing distribution into a process that cannot boot — trading a rare
    degraded answer for a total outage on the one surface whose job is to keep
    answering when everything else is broken.
    """

    def _missing(_name: str) -> str:
        raise PackageNotFoundError("moneybin")

    monkeypatch.setattr("moneybin.build_info.version", _missing)

    assert read_package_version() is None


@pytest.mark.unit
def test_revision_is_absent_outside_a_source_checkout(tmp_path: Path) -> None:
    """An installed wheel has no repository to interrogate; say so, don't guess."""
    assert read_git_revision(tmp_path) is None


@pytest.mark.unit
def test_revision_is_absent_when_the_repository_is_merely_an_ancestor() -> None:
    """A directory *inside* someone's checkout is not that checkout's build.

    ``git`` answers about the nearest enclosing repository, so an installed
    wheel under a virtualenv in the user's own project would otherwise stamp
    MoneyBin's build with that project's HEAD — a confident wrong answer where
    the contract promises ``null``.
    """
    assert read_git_revision(Path(__file__).resolve().parent) is None


@pytest.mark.unit
def test_revision_survives_a_checkout_path_containing_a_space(tmp_path: Path) -> None:
    """A space in the checkout path must not silently blank the stamp.

    ``git rev-parse --show-toplevel`` prints the path unquoted, so splitting its
    two-line answer on arbitrary whitespace turns one such path into three
    tokens and discards the revision. macOS paths routinely contain spaces, and
    the failure is invisible: the field reads ``null``, which is also exactly
    what a legitimate wheel install reports.
    """
    repo = tmp_path / "space test dir"
    repo.mkdir()
    for command in (
        ["init", "-q"],
        ["-c", "user.email=t@example.com", "-c", "user.name=t"]
        + ["commit", "-q", "--allow-empty", "-m", "x"],
    ):
        subprocess.run(  # noqa: S603  # git with static args
            ["git", "-C", str(repo), *command],  # noqa: S607
            capture_output=True,
            check=True,
        )
    head = subprocess.run(  # noqa: S603  # git with static args
        ["git", "-C", str(repo), "rev-parse", "HEAD"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    assert read_git_revision(repo) == head
