"""Scenario: empty input fixture must not crash; downstream tables stay empty.

tiers: T1, T3-empty-input.
"""

from __future__ import annotations

import pytest

from tests.scenarios._harnesses import assert_empty_input_safe
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
from tests.scenarios._runner.loader import FixtureSpec, Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


def _empty_scenario() -> Scenario:
    return Scenario(
        scenario="empty-input-handling",
        setup=SetupSpec(persona="basic", seed=42, years=1),
        pipeline=[],
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_empty_input_handling() -> None:
    scenario = _empty_scenario()
    csv_spec = FixtureSpec(
        path="empty-input/empty.csv", account="empty-card", source_type="csv"
    )
    ofx_spec = FixtureSpec(
        path="empty-input/empty.ofx.csv", account="empty-card", source_type="ofx"
    )

    with scenario_env(scenario) as (db, _tmp, env):
        load_fixture_into_db(db, csv_spec)
        load_fixture_into_db(db, ofx_spec)

        def run_pipeline() -> None:
            run_step("transform", scenario.setup, db, env=env)
            run_step("match", scenario.setup, db, env=env)

        # Only assert fact tables empty: the fixture loader intentionally
        # seeds a minimal account row so FK targets exist for downstream
        # views. That seed is setup, not pipeline output, so dim_accounts
        # is allowed to be non-empty here.
        result = assert_empty_input_safe(
            db,
            run=run_pipeline,
            tables=["core.fct_transactions"],
        )
    result.raise_if_failed()
