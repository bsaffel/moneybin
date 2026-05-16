"""Smoke test for meta.model_freshness.

Applies the SQLMesh transform pipeline using the idempotency-rerun scenario
and asserts the view returns at least one row for a core model with non-NULL
last_applied_at.
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

        rows = db.execute(
            """
            SELECT model_name, last_changed_at, last_applied_at
            FROM meta.model_freshness
            WHERE model_name LIKE 'core.%'
            ORDER BY model_name
            """
        ).fetchall()

    assert rows, "meta.model_freshness returned no core.* rows"
    for name, _changed, applied in rows:
        assert applied is not None, f"{name} has NULL last_applied_at"
