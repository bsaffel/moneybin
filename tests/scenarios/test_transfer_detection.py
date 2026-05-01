"""Scenario: cross-account transfer pairs detected; F1 vs ground truth."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario
from tests.scenarios._tier1_backfill import tier1_backfill


@pytest.mark.scenarios
@pytest.mark.slow
def test_transfer_detection() -> None:
    """tiers: T1, T2-balanced-transfers, T2-transfer-f1."""
    scenario = load_shipped_scenario("transfer-detection-cross-account")
    assert scenario is not None
    result = run_scenario(
        scenario,
        extra_assertions=tier1_backfill(scenario.setup),
    )
    assert result.passed, result.failure_summary()
