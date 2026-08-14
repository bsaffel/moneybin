"""``system_status`` must name the build the server is actually running.

A stdio MCP server is a long-lived process pinned to whatever the checkout held
when it started. Without a build stamp in the response, a caller comparing live
behaviour against ``main`` cannot tell a stale process from a real defect — and
on 2026-08-14 that ambiguity produced a filed finding claiming a confirmation
gate did not exist, when the gate was present and only its evidence sentence
was missing from a three-day-old server.
"""

from __future__ import annotations

import subprocess  # noqa: S404 — reads HEAD to derive the expected value
from importlib.metadata import version
from pathlib import Path

import pytest

from moneybin.database import DatabaseLockError
from moneybin.mcp.tools.system import (
    _read_git_revision,  # pyright: ignore[reportPrivateUsage]
    system_status,
    system_status_coarse,
)


@pytest.mark.unit
async def test_system_status_names_the_running_package_version(
    mcp_db: object,
) -> None:
    """The envelope carries the installed distribution version, not a placeholder."""
    data = system_status().to_dict()["data"]

    assert data["build"]["version"] == version("moneybin")


@pytest.mark.unit
async def test_locked_database_still_names_the_build(
    mcp_db: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The degraded path reports the build too — it needs no DB connection.

    This is the case that motivates the field. When a caller is already seeing
    something they cannot explain, "which build am I talking to" must not be
    the one question the server refuses to answer.
    """

    def _locked(*_args: object, **_kwargs: object) -> object:
        raise DatabaseLockError("a writer holds the lock")

    monkeypatch.setattr("moneybin.database.get_database", _locked)

    envelope = system_status().to_dict()

    assert envelope["summary"]["degraded"] is True
    assert envelope["data"]["build"]["version"] == version("moneybin")


@pytest.mark.unit
async def test_source_checkout_reports_the_commit_it_is_running(
    mcp_db: object,
) -> None:
    """``revision`` names the checkout's HEAD.

    The version alone cannot do this job: it reads ``0.1.0`` on every commit,
    so it would not have separated #386 from #387 — the exact distinction whose
    absence caused the misdiagnosis. The commit is the load-bearing half.
    """
    head = subprocess.run(  # noqa: S603 — git with static args
        ["git", "-C", str(Path(__file__).resolve().parent), "rev-parse", "HEAD"],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    data = system_status().to_dict()["data"]

    assert data["build"]["revision"] == head


@pytest.mark.unit
def test_revision_is_absent_outside_a_source_checkout(tmp_path: Path) -> None:
    """An installed wheel has no repository to interrogate; say so, don't guess."""
    assert _read_git_revision(tmp_path) is None


@pytest.mark.unit
async def test_the_registered_tool_surfaces_the_build(mcp_db: object) -> None:
    """The build reaches the agent through the tool it actually calls.

    ``system_status`` is registered as ``system_status_coarse``; the block above
    tests the inner overview builder, which is not the surface an agent drives.
    A projection that dropped the field on the way out would leave every test
    above green while the stamp never reached a caller.
    """
    response = await system_status_coarse()

    overview = next(
        section
        for section in response.to_dict()["data"]["sections"]
        if section.get("kind") == "overview"
    )
    assert overview["overview"]["build"]["version"] == version("moneybin")
