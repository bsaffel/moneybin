"""Tests for SystemService.model_freshness — typed wrapper over meta.model_freshness.

Uses the scenario runner to apply the full SQLMesh pipeline (which is what
materializes meta.model_freshness). Cheaper unit-style fixtures cannot stand
in here because the view is defined by SQLMesh's own internal metadata.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from moneybin.services.system_service import ModelFreshness, SystemService
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_returns_freshness_for_known_model() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        result = SystemService(db).model_freshness("core.dim_accounts")

    assert result is not None
    assert isinstance(result, ModelFreshness)
    assert result.model_name == "core.dim_accounts"
    assert isinstance(result.last_changed_at, datetime)
    assert isinstance(result.last_applied_at, datetime)


@pytest.mark.scenarios
@pytest.mark.slow
def test_returns_none_for_unknown_model() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        result = SystemService(db).model_freshness("core.does_not_exist")

    assert result is None
