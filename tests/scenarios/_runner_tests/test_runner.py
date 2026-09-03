"""Unit tests for the scenario runner's merge / callback paths.

These tests stub out the heavy bootstrap (encrypted DB + SQLMesh catalog)
so we exercise only the orchestration logic — assertion merging, the
`extra_assertions` callback contract, and crash handling — without paying
the cost of a real scenario run.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

import pytest

from tests.scenarios._runner import runner as runner_mod
from tests.scenarios._runner.loader import Scenario, load_scenario_from_string
from tests.validation.result import AssertionResult, EvaluationResult

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


_CRASHING_YAML = """
scenario: unit-test-crash
description: "assertion and evaluation whose functions do not resolve"
setup:
  persona: basic
  seed: 42
  years: 1
  fixtures: []
pipeline: []
assertions:
  - name: broken_assertion
    fn: no_such_module.no_such_fn
evaluations:
  - name: broken_evaluation
    fn: no_such_module.no_such_fn
    threshold:
      metric: precision
      min: 0.9
"""


def test_runner_marks_crashed_assertions_and_evaluations(stubbed_runner: None) -> None:  # noqa: ARG001 — fixture activation
    """A caught crash is recorded as a crash, not as a verdict or a low score.

    Without the flag both render identically to the one summary CI ever sees,
    which is what sends triage at the scenario's data instead of its code.
    """
    result = runner_mod.run_scenario(load_scenario_from_string(_CRASHING_YAML))

    assert not result.passed
    crashed = {a.name for a in result.assertions if a.crashed}
    assert crashed == {"broken_assertion"}
    assert all(v.crashed for v in result.evaluations)

    summary = result.failure_summary()
    assert "assertion broken_assertion: crashed," in summary
    assert "evaluation broken_evaluation: crashed," in summary
    # The pre-flight assertion passed and is not a crash — the flag must not
    # smear across every result in a failing run.
    assert not any(a.crashed for a in result.assertions if a.name == "catalog")


# Synthetic value; a real assertion's message would quote the rows it compared.
_LEAKY_MESSAGE = "txn 4111111111111111 differed by 42.00"


def test_assertion_crash_records_the_type_not_the_message(
    stubbed_runner: None,  # noqa: ARG001 — fixture activation
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An assertion queries scenario rows, so its crash text cannot be echoed.

    ``failure_summary()`` is the one output CI shows, so the message has to be
    dropped where it is recorded, not where it is rendered.
    """

    def _explode(_db: object, **_kwargs: object) -> AssertionResult:
        raise RuntimeError(_LEAKY_MESSAGE)

    def _resolve(_fn: str) -> Callable[..., AssertionResult]:
        return _explode

    monkeypatch.setattr(runner_mod, "_resolve_assertion", _resolve)

    result = runner_mod.run_scenario(load_scenario_from_string(_CRASHING_YAML))

    crashed = next(a for a in result.assertions if a.name == "broken_assertion")
    assert crashed.error == "RuntimeError"
    summary = result.failure_summary()
    assert "assertion broken_assertion: crashed, RuntimeError" in summary
    assert "4111111111111111" not in summary


def test_evaluation_crash_records_the_type_not_the_message(
    stubbed_runner: None,  # noqa: ARG001 — fixture activation
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same contract on the evaluation branch, which records into breakdown."""

    def _explode(_db: object, **_kwargs: object) -> EvaluationResult:
        raise RuntimeError(_LEAKY_MESSAGE)

    def _resolve(_fn: str) -> Callable[..., EvaluationResult]:
        return _explode

    monkeypatch.setattr(runner_mod, "resolve_evaluation", _resolve)

    result = runner_mod.run_scenario(load_scenario_from_string(_CRASHING_YAML))

    crashed = next(v for v in result.evaluations if v.name == "broken_evaluation")
    assert crashed.breakdown == {"error": "RuntimeError"}
    summary = result.failure_summary()
    assert "evaluation broken_evaluation: crashed, RuntimeError" in summary
    assert "4111111111111111" not in summary
