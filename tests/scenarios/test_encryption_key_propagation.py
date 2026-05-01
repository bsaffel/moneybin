"""Scenario: subprocess transform opens the encrypted DB with the propagated key."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_encryption_key_propagation() -> None:
    """Subprocess transform opens the encrypted DB with the propagated key."""
    scenario = load_shipped_scenario("encryption-key-propagation")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
