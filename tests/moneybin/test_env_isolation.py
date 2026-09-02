"""The suite must not read the developer's own ``MONEYBIN_*`` configuration.

``MoneyBinSettings`` resolves every field from ``MONEYBIN_``-prefixed
environment variables, so a var exported in the developer's shell (or their
launch wrapper) reaches any test that constructs settings without an explicit
override. Those tests then assert against the developer's machine rather than
against the shipped defaults, and they pass in CI — where no such var
exists — while failing locally, or the reverse.

``tests/conftest.py`` clears the whole prefix at import time and re-adds only
the vars the harness itself owns. Two tests guard that, and they are not
redundant: the first pins *which* names survive, the second proves the
clearing code actually runs.

The split exists because the first test cannot fail on a clean CI runner.
There, no ambient ``MONEYBIN_*`` var exists to begin with, so the snapshot
equals the harness-owned set whether or not conftest deletes anything — the
predicate never executes, and a passing run says nothing. Only the second test
seeds a variable of its own, so only it holds the deletion path in place
everywhere the suite runs.
"""

from __future__ import annotations

import os
import subprocess  # noqa: S404 — a child pytest run is how this proves isolation
import sys
from pathlib import Path

# Set by tests/conftest.py: a per-xdist-worker MoneyBin home, and the import
# inbox root beneath it.
HARNESS_OWNED = frozenset({"MONEYBIN_HOME", "MONEYBIN_IMPORT___INBOX_ROOT"})

_REPO_ROOT = Path(__file__).parents[2]
_SNAPSHOT_TEST = "test_only_harness_owned_moneybin_env_vars_are_visible"

# Deliberately lowercase. MoneyBinSettings sets `case_sensitive=False`, so
# pydantic reads this spelling into the same field an uppercase export would;
# seeding it this way keeps conftest's `.upper()` match load-bearing.
_SEEDED_VAR = "moneybin_seeded_ambient_probe"


def test_only_harness_owned_moneybin_env_vars_are_visible(
    moneybin_env_at_startup: frozenset[str],
) -> None:
    """No ambient ``MONEYBIN_*`` var survived conftest's cleanup.

    Asserts against the snapshot conftest took at import rather than against
    live ``os.environ``, because isolation is an import-time property. Reading
    the environment at call time would fold in mutations made by other tests
    sharing this xdist worker and report them as the developer's shell.
    """
    assert moneybin_env_at_startup == HARNESS_OWNED


def test_seeded_ambient_var_does_not_survive_into_a_child_run() -> None:
    """Conftest strips a ``MONEYBIN_*`` var this test plants itself.

    The guard above is only load-bearing on a machine that already exports
    ambient config. This one supplies its own, so the deletion path runs on
    every machine including a clean CI runner, and deleting conftest's
    clearing loop fails it there too.
    """
    env = dict(os.environ) | {_SEEDED_VAR: "1"}
    nodeid = f"{Path(__file__).relative_to(_REPO_ROOT)}::{_SNAPSHOT_TEST}"

    result = subprocess.run(  # noqa: S603 — fixed interpreter, literal arguments
        [sys.executable, "-m", "pytest", nodeid, "-n0", "-p", "no:cacheprovider"],
        cwd=_REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    # Two independent halves: the child reported clearing the seeded name, and
    # the child's own snapshot assertion then passed. The first proves conftest
    # matched and removed it; the second proves nothing put it back.
    assert _SEEDED_VAR in result.stderr, result.stderr
    assert result.returncode == 0, result.stdout + result.stderr
