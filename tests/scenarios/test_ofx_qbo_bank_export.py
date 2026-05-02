"""Scenario: QBO file from a bank's Quicken Web Connect download."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_ofx_qbo_bank_export() -> None:
    scenario = load_shipped_scenario("ofx-qbo-bank-export")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
