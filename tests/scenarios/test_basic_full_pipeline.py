"""Scenario: end-to-end pipeline correctness for the basic persona."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_basic_full_pipeline() -> None:
    scenario = load_shipped_scenario("basic-full-pipeline")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
