"""Scenario: multi-account OFX statement (checking + savings in one file)."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_ofx_multi_account_statement() -> None:
    scenario = load_shipped_scenario("ofx-multi-account-statement")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
