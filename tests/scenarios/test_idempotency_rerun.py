"""Scenario: re-running transform must not duplicate rows.

tiers: T1, T3-idempotency, T3-incremental.
"""

from __future__ import annotations

import pytest

from tests.scenarios._harnesses import assert_idempotent
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_idempotency_rerun() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)

        result = assert_idempotent(
            db,
            tables=["core.fct_transactions", "core.dim_accounts"],
            rerun=lambda: run_step("transform", scenario.setup, db, env=env),
        )
    result.raise_if_failed()
