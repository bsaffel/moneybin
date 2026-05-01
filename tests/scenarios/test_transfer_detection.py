"""Scenario: cross-account transfer pairs detected; F1 vs ground truth."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_transfer_detection() -> None:
    """Cross-account transfer pairs detected; F1 vs ground truth."""
    scenario = load_shipped_scenario("transfer-detection-cross-account")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
