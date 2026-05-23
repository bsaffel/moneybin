"""Scenario: N-way dedup collapses 3+ copies of a transaction into one record.

Proves the end-to-end union-find collapse across the full spectrum of dedup
tiers in a single component:

- X has three copies — two within-source CSVs (csv_a, csv_b) and one
  cross-source OFX (ofx_x). The union-find spanning forest must merge all
  three into ONE gold record (mixed within-source + cross-source component).
- Y has three copies, all within-source (csv_a, csv_b, csv_c). Three pure
  within-source copies must merge into ONE gold record.
- Z (csv_a, 2024-04-10) and Z' (ofx_x, 2024-04-30) share an amount but their
  dates are 20 days apart — well outside the 3-day date window — so blocking
  never pairs them. They must remain TWO separate gold records (the negative
  guard against over-collapse).

Hand-derived gold-record counts (independent of pipeline output):
  X: CSVA_X + CSVB_X + OFX_X  -> 1 gold record
  Y: CSVA_Y + CSVB_Y + CSVC_Y -> 1 gold record
  Z / Z': CSVA_Z, OFX_Z       -> 2 gold records (no merge)

Auto-merge derivation: confidence = 0.40*date_score + 0.60*desc_sim.
Each copy of X/Y shares an identical date (date_score 1.0) and a
byte-identical description/payee (desc_sim 1.0) -> confidence 1.0, above
the 0.95 high-confidence threshold, so every pair auto-merges with no
review step.
"""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, run_scenario


@pytest.mark.scenarios
@pytest.mark.slow
def test_dedup_nway() -> None:
    """tiers: T1, T2-dedup-nway-collapse, T2-negative-expectations."""
    scenario = load_shipped_scenario("dedup-nway")
    assert scenario is not None
    result = run_scenario(scenario)
    assert result.passed, result.failure_summary()
