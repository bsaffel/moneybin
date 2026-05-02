"""Scenario: institution-resolution failure + --institution override recovery."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_ofx_missing_institution_metadata() -> None:
    scenario = load_shipped_scenario("ofx-missing-institution-metadata")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
