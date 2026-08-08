"""Scenario: a FITID orphaned by disambiguation must not reach core twice."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_ofx_fitid_supersession() -> None:
    scenario = load_shipped_scenario("ofx-fitid-supersession")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
