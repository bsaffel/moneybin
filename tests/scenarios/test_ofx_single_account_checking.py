"""Scenario: single-account OFX import — golden path through the new pipeline."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_ofx_single_account_checking() -> None:
    scenario = load_shipped_scenario("ofx-single-account-checking")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
