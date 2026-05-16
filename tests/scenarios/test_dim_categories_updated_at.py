"""Verify core.dim_categories.updated_at convention.

Pure-seed rows (no override, no user_categories overlay) must have NULL
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
def test_pure_seed_category_has_null_updated_at() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        rows = db.execute(
            """
            SELECT category_id, updated_at
            FROM core.dim_categories
            WHERE is_default = TRUE
              AND category_id NOT IN (SELECT category_id FROM app.category_overrides)
            """
        ).fetchall()

    assert rows, "scenario must include seed categories with no override"
    for category_id, updated_at in rows:
        assert updated_at is None, f"{category_id} should have NULL updated_at"


@pytest.mark.scenarios
@pytest.mark.slow
def test_user_category_has_non_null_updated_at() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        db.execute(
            """
            INSERT INTO app.user_categories
            (category_id, category, subcategory, description, is_active, created_at, updated_at)
            VALUES ('test12345678', 'TestCategory', NULL, NULL, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
        )

        # Force-restate dim_categories so it picks up the new user_categories row.
        # app.user_categories is outside the SQLMesh model graph, so a plain
        # plan() would be a no-op.
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=["core.dim_categories"],
                auto_apply=True,
                no_prompts=True,
            )

        dim_row = db.execute(
            "SELECT updated_at FROM core.dim_categories WHERE category_id = ?",
            ["test12345678"],
        ).fetchone()

    assert dim_row is not None, "test user_category did not surface in dim_categories"
    assert dim_row[0] is not None, "user category should have non-NULL updated_at"


@pytest.mark.scenarios
@pytest.mark.slow
def test_overridden_seed_category_carries_override_updated_at() -> None:
    """Verify seed+override branch carries the override timestamp.

    A default category with a row in app.category_overrides exposes the
    override's updated_at on dim_categories — covering the seed+override
    branch of the per-row freshness formula.
    """
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        # Pick a default category that has no existing override.
        cat_row = db.execute(
            """
            SELECT category_id FROM core.dim_categories
            WHERE is_default = TRUE
              AND category_id NOT IN (SELECT category_id FROM app.category_overrides)
            LIMIT 1
            """
        ).fetchone()
        assert cat_row is not None, (
            "scenario must include a seed category with no override"
        )
        cat_id = cat_row[0]

        # Insert an override row with an explicit fresh timestamp.
        db.execute(
            "INSERT INTO app.category_overrides (category_id, is_active, updated_at) "
            "VALUES (?, FALSE, CURRENT_TIMESTAMP)",
            [cat_id],
        )

        # Force-restate dim_categories so it picks up the new override row.
        # app.category_overrides is outside the SQLMesh model graph, so a plain
        # plan() would be a no-op.
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=["core.dim_categories"],
                auto_apply=True,
                no_prompts=True,
            )

        row = db.execute(
            """
            SELECT d.updated_at, o.updated_at
            FROM core.dim_categories AS d
            JOIN app.category_overrides AS o USING (category_id)
            WHERE d.category_id = ?
            """,
            [cat_id],
        ).fetchone()

    assert row is not None
    dim_updated_at, override_updated_at = row
    assert dim_updated_at is not None
    assert dim_updated_at == override_updated_at, (
        f"dim_categories.updated_at={dim_updated_at} should equal "
        f"app.category_overrides.updated_at={override_updated_at}"
    )
