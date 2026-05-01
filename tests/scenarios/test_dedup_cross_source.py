"""Scenario: cross-source dedup collapses 6 fixture rows into 3 gold records."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_dedup_cross_source() -> None:
    """Cross-source dedup collapses 6 fixture rows into 3 gold records."""
    scenario = load_shipped_scenario("dedup-cross-source")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
