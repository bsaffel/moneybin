"""Scenario: re-importing the same OFX file without --force is rejected."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_ofx_reimport_idempotent() -> None:
    scenario = load_shipped_scenario("ofx-reimport-idempotent")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
