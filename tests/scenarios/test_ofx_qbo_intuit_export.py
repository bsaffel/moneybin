"""Scenario: QBO file from a QuickBooks/Intuit export."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_ofx_qbo_intuit_export() -> None:
    scenario = load_shipped_scenario("ofx-qbo-intuit-export")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
