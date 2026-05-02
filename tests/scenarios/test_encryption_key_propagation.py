"""Scenario: subprocess transform opens the encrypted DB with the propagated key."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario
from tests.scenarios._tier1_backfill import tier1_backfill


@pytest.mark.scenarios
@pytest.mark.slow
def test_encryption_key_propagation() -> None:
    """tiers: T1, T3-encryption-key-propagation.

    The Tier 1 row count is the deterministic GeneratorEngine output for
    (basic, seed=42, years=1) — replacing the previous ``min_rows >= 100``
    band that was an observe-and-paste expectation. If the subprocess
    transform produced the right row count, it must have opened the
    encrypted DB with the propagated key.
    """
    scenario = load_shipped_scenario("encryption-key-propagation")
    assert scenario is not None
    result = run_scenario(
        scenario,
        extra_assertions=tier1_backfill(scenario.setup),
    )
    assert result.passed, result.failure_summary()
