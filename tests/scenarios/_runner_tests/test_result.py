"""Unit tests for ScenarioResult — no DB needed."""

from __future__ import annotations

from tests.scenarios._runner.result import ScenarioResult
from tests.validation.result import (
    AssertionResult,
    EvaluationResult,
    ExpectationResult,
)


def _result(**overrides: object) -> ScenarioResult:
    base: dict[str, object] = {"scenario": "demo", "duration_seconds": 0.0}
    base.update(overrides)
    return ScenarioResult(**base)  # type: ignore[arg-type]


def test_passed_true_when_no_failures_and_not_halted() -> None:
    r = _result(
        assertions=[AssertionResult(name="a", passed=True)],
        expectations=[ExpectationResult(name="e", kind="match_decision", passed=True)],
        evaluations=[
            EvaluationResult(
                name="v", metric="m", value=1.0, threshold=0.5, passed=True
            )
        ],
    )
    assert r.passed


def test_passed_false_when_halted() -> None:
    r = _result(halted="setup error")
    assert not r.passed


def test_passed_false_when_any_check_fails() -> None:
    r = _result(assertions=[AssertionResult(name="a", passed=False, error="boom")])
    assert not r.passed


def test_failure_summary_includes_halt_reason() -> None:
    r = _result(halted="setup error")
    summary = r.failure_summary()
    assert "halted: setup error" in summary


def test_failure_summary_uses_assertion_error_text() -> None:
    r = _result(assertions=[AssertionResult(name="a", passed=False, error="boom")])
    assert "assertion a: boom" in r.failure_summary()


def test_failure_summary_falls_back_to_details_then_failed() -> None:
    r = _result(
        assertions=[
            AssertionResult(name="a1", passed=False, details={"violations": 3}),
            AssertionResult(name="a2", passed=False),
        ]
    )
    summary = r.failure_summary()
    assert "assertion a1: {'violations': 3}" in summary
    assert "assertion a2: failed" in summary


def test_failure_summary_lists_failed_expectations_and_evaluations() -> None:
    r = _result(
        expectations=[
            ExpectationResult(name="e1", kind="match_decision", passed=False)
        ],
        evaluations=[
            EvaluationResult(
                name="v1", metric="precision", value=0.5, threshold=0.9, passed=False
            )
        ],
    )
    summary = r.failure_summary()
    assert "expectation e1" in summary
    assert "evaluation v1: precision=0.5 < threshold=0.9" in summary


def test_failure_summary_marks_a_runner_caught_assertion_crash() -> None:
    """A crash and a verdict both carry error text; only the crash says so."""
    r = _result(
        assertions=[
            AssertionResult(name="crash", passed=False, error="boom", crashed=True),
            AssertionResult(name="verdict", passed=False, error="boom"),
        ]
    )
    summary = r.failure_summary()
    assert "assertion crash: crashed, boom" in summary
    assert "assertion verdict: boom" in summary


def test_failure_summary_shows_evaluation_crash_instead_of_a_score() -> None:
    """A crashed evaluator's 0.0 is a placeholder, not a measurement."""
    r = _result(
        evaluations=[
            EvaluationResult(
                name="v",
                metric="precision",
                value=0.0,
                threshold=0.9,
                passed=False,
                breakdown={"error": "boom"},
                crashed=True,
            )
        ]
    )
    summary = r.failure_summary()
    assert "evaluation v: crashed, boom" in summary
    assert "threshold=0.9" not in summary
