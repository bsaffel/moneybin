"""Smoke test for meta.model_freshness.

Applies the SQLMesh transform pipeline using the idempotency-rerun scenario and
asserts the view's public column contract holds against real SQLMesh state.
This is the only layer that exercises the view's own derivation — the unit
tests stub ``meta.model_freshness`` as a table, so a column dropped from the
CTE chain would leave them green.
"""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_meta_model_freshness_returns_row_per_model() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        core_rows = db.execute(
            """
            SELECT model_name, last_changed_at, last_applied_at,
                   last_executed_at, model_kind
            FROM meta.model_freshness
            WHERE model_name LIKE 'core.%'
            ORDER BY model_name
            """
        ).fetchall()
        external_rows = db.execute(
            """
            SELECT model_name, last_executed_at, model_kind
            FROM meta.model_freshness
            WHERE model_name LIKE 'raw.%'
            ORDER BY model_name
            """
        ).fetchall()

    assert core_rows, "meta.model_freshness returned no core.* rows"
    for name, _changed, applied, executed, kind in core_rows:
        assert applied is not None, f"{name} has NULL last_applied_at"
        # The step above ran a full apply, so every core model was backfilled.
        assert executed is not None, f"{name} has NULL last_executed_at"
        assert kind is not None, f"{name} has NULL model_kind"

    # `external_models.yaml` declares the `raw.*` sources, and SQLMesh never
    # executes an EXTERNAL model — so it records no interval for one and
    # `last_executed_at` stays NULL. `TransformService` filters exactly this set
    # back out; were the kind to arrive wrong, that filter would silently start
    # counting sources that can never carry a stamp.
    assert external_rows, "meta.model_freshness returned no raw.* rows"
    for name, executed, kind in external_rows:
        assert kind == "EXTERNAL", f"{name} has model_kind {kind!r}, want EXTERNAL"
        assert executed is None, f"{name} is EXTERNAL but has last_executed_at"
