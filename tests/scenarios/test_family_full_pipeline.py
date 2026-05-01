"""Scenario: end-to-end pipeline correctness for the family persona (3 years)."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_family_full_pipeline() -> None:
    """End-to-end pipeline correctness for the family persona (3 years)."""
    scenario = load_shipped_scenario("family-full-pipeline")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
