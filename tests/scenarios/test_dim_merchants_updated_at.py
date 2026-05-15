"""Verify core.dim_merchants.updated_at convention.

Pure-seed rows (no override, no user_merchants overlay) must have NULL
updated_at — clients query meta.model_freshness for seed model freshness.
User-created rows must carry their own updated_at. See
docs/specs/core-updated-at-convention.md.
"""

from __future__ import annotations

import pytest

from moneybin.database import sqlmesh_context
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_pure_seed_merchant_has_null_updated_at() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        rows = db.execute(
            """
            SELECT merchant_id, updated_at
            FROM core.dim_merchants
            WHERE is_user = FALSE
              AND merchant_id NOT IN (SELECT merchant_id FROM app.merchant_overrides)
            """
        ).fetchall()

    assert rows, "scenario must include seed merchants with no override"
    for merchant_id, updated_at in rows:
        assert updated_at is None, f"{merchant_id} should have NULL updated_at"


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


@pytest.mark.scenarios
@pytest.mark.slow
def test_overridden_seed_merchant_carries_override_updated_at() -> None:
    """Verify seed+override branch carries the override timestamp.

    A seed merchant with a row in app.merchant_overrides exposes the
    override's updated_at on dim_merchants — covering the seed+override
    branch of the per-row freshness formula.
    """
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        # Pick a seed merchant that has no existing override.
        merch_row = db.execute(
            """
            SELECT merchant_id FROM core.dim_merchants
            WHERE is_user = FALSE
              AND merchant_id NOT IN (SELECT merchant_id FROM app.merchant_overrides)
            LIMIT 1
            """
        ).fetchone()
        assert merch_row is not None, "scenario must include a seed merchant with no override"
        merch_id = merch_row[0]

        # Insert an override row with an explicit fresh timestamp.
        db.execute(
            "INSERT INTO app.merchant_overrides (merchant_id, is_active, updated_at) "
            "VALUES (?, TRUE, CURRENT_TIMESTAMP)",
            [merch_id],
        )

        # Force-restate dim_merchants so it picks up the new override row.
        # app.merchant_overrides is outside the SQLMesh model graph, so a plain
        # plan() would be a no-op.
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=["core.dim_merchants"],
                auto_apply=True,
                no_prompts=True,
            )

        row = db.execute(
            """
            SELECT d.updated_at, o.updated_at
            FROM core.dim_merchants AS d
            JOIN app.merchant_overrides AS o USING (merchant_id)
            WHERE d.merchant_id = ?
            """,
            [merch_id],
        ).fetchone()

    assert row is not None
    dim_updated_at, override_updated_at = row
    assert dim_updated_at is not None
    assert dim_updated_at == override_updated_at, (
        f"dim_merchants.updated_at={dim_updated_at} should equal "
        f"app.merchant_overrides.updated_at={override_updated_at}"
    )
