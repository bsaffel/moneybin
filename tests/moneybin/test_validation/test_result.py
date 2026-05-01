"""Tests for validation result dataclasses."""

from moneybin.validation.result import AssertionResult, EvaluationResult


def test_assertion_result_frozen() -> None:
    """AssertionResult stores fields correctly and defaults error to None."""
    r = AssertionResult(name="x", passed=True, details={"rows": 3})
    assert r.passed is True
    assert r.details == {"rows": 3}
    assert r.error is None


def test_evaluation_result_passed_inferred_externally() -> None:
    """EvaluationResult stores passed and numeric fields correctly."""
    r = EvaluationResult(
        name="cat",
        metric="accuracy",
        value=0.82,
        threshold=0.80,
        passed=True,
        breakdown={"per_category": {}},
    )
    assert r.passed is True
    assert r.value > r.threshold


def test_expectation_result_has_expected_shape():
    """ExpectationResult stores all fields correctly."""
    from moneybin.validation.result import ExpectationResult

    r = ExpectationResult(
        name="x", kind="match_decision", passed=True, details={"a": 1}
    )
    assert r.name == "x"
    assert r.kind == "match_decision"
    assert r.passed is True
    assert r.details == {"a": 1}


def test_expectation_result_default_details_empty():
    """ExpectationResult defaults details to an empty dict."""
    from moneybin.validation.result import ExpectationResult

    r = ExpectationResult(name="x", kind="k", passed=False)
    assert r.details == {}
