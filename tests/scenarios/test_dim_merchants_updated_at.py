"""Verify core.dim_merchants.updated_at convention.

All merchants come from app.user_merchants and must carry their own
updated_at. See docs/specs/core-updated-at-convention.md.
"""

from __future__ import annotations

import pytest

from moneybin.database import sqlmesh_context
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_user_merchant_has_non_null_updated_at() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        db.execute(
            """
            INSERT INTO app.user_merchants
            (merchant_id, raw_pattern, match_type, canonical_name, category,
             subcategory, created_by, exemplars, created_at, updated_at)
            VALUES ('testmerchant', 'TEST', 'contains', 'TestMerchant',
                    'Shopping', NULL, 'user', [], CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
        )

        # Force-restate dim_merchants so it picks up the new user_merchants row.
        # app.user_merchants is outside the SQLMesh model graph, so a plain
        # plan() would be a no-op.
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=["core.dim_merchants"],
                auto_apply=True,
                no_prompts=True,
            )

        dim_row = db.execute(
            "SELECT updated_at FROM core.dim_merchants WHERE merchant_id = ?",
            ["testmerchant"],
        ).fetchone()

    assert dim_row is not None, "test user_merchant did not surface in dim_merchants"
    assert dim_row[0] is not None, "user merchant should have non-NULL updated_at"
