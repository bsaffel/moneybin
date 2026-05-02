"""Unit tests for the scenario runner's merge / callback paths.

These tests stub out the heavy bootstrap (encrypted DB + SQLMesh catalog)
so we exercise only the orchestration logic — assertion merging, the
`extra_assertions` callback contract, and crash handling — without paying
the cost of a real scenario run.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import runner as runner_mod
from tests.scenarios._runner.loader import Scenario, load_scenario_from_string

_MINIMAL_YAML = """
scenario: unit-test
description: "minimal scenario for runner unit tests"
setup:
  persona: basic
  seed: 42
  years: 1
  fixtures: []
pipeline: []
assertions: []
"""


def _make_scenario() -> Scenario:
    return load_scenario_from_string(_MINIMAL_YAML)


@pytest.fixture()
def stubbed_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the bootstrap + preflight catalog check with no-ops."""

    @contextmanager
    def _fake_env(scenario: Scenario, *, keep_tmpdir: bool = False) -> Any:  # noqa: ARG001
        yield (object(), "fake-scenario", {})

    def _passing_catalog(_db: object) -> AssertionResult:
        return AssertionResult(name="catalog", passed=True)

    monkeypatch.setattr(runner_mod, "scenario_env", _fake_env)
    monkeypatch.setattr(runner_mod, "assert_sqlmesh_catalog_matches", _passing_catalog)
    # Steps and verify_expectations aren't reached for an empty pipeline /
    # empty expectations, but stubbing get_database avoids any accidental
    # singleton fetch in the loop body.
    monkeypatch.setattr(runner_mod, "get_database", lambda: object())


def test_run_scenario_invokes_extra_assertions(stubbed_runner: None) -> None:  # noqa: ARG001 — fixture activation
    """extra_assertions results are appended after standard assertions."""
    scenario = _make_scenario()
    sentinel = AssertionResult(name="extra_check", passed=True, details={"k": "v"})

    result = runner_mod.run_scenario(
        scenario,
        extra_assertions=lambda _db: [sentinel],
    )

    names = [a.name for a in result.assertions]
    assert "extra_check" in names
    assert result.passed


def test_extra_assertion_failure_propagates_to_result(stubbed_runner: None) -> None:  # noqa: ARG001 — fixture activation
    """A failing extra assertion flips result.passed to False."""
    scenario = _make_scenario()
    failing = AssertionResult(name="extra_fail", passed=False, error="boom")

    result = runner_mod.run_scenario(
        scenario,
        extra_assertions=lambda _db: [failing],
    )

    assert not result.passed
    assert "extra_fail" in result.failure_summary()


def test_extra_assertions_crash_halts_scenario(stubbed_runner: None) -> None:  # noqa: ARG001 — fixture activation
    """An exception inside the callback halts the scenario with a clean reason."""
    scenario = _make_scenario()

    def _crash(_db: object) -> list[AssertionResult]:
        raise RuntimeError("explode")

    result = runner_mod.run_scenario(scenario, extra_assertions=_crash)

    assert not result.passed
    assert result.halted is not None
    assert "extra_assertions crashed" in result.halted
    # Halt reason carries only the exception type — full str(exc) might
    # echo PII from local variables (logger module rule).
    assert "RuntimeError" in result.halted
    assert "explode" not in result.halted
