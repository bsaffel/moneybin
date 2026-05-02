"""Scenario: adversarial near-miss rows must not collapse.

tiers: T1, T2-negative-expectations.
"""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_dedup_negative_fixture() -> None:
    scenario = load_shipped_scenario("dedup-negative-fixture")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
