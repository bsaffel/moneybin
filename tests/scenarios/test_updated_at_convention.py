"""End-to-end: editing a user category bumps dim_categories.updated_at for that
row only; unrelated rows are unaffected.

Validates the core convention from docs/specs/core-updated-at-convention.md
across the full SQLMesh pipeline."""

from __future__ import annotations

import time

import pytest

from moneybin.database import sqlmesh_context
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_user_category_edit_advances_only_that_row() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        # Seed two user categories. Both should have non-NULL updated_at.
        db.execute(
            """
            INSERT INTO app.user_categories
            (category_id, category, subcategory, description, is_active, created_at, updated_at)
            VALUES
                ('edit12345678', 'EditTarget', NULL, NULL, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
                ('keep12345678', 'KeepUnchanged', NULL, NULL, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
        )

        # Force-restate dim_categories so it picks up the new user_categories rows.
        # app.user_categories is outside the SQLMesh model graph, so a plain
        # plan() would be a no-op.
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=["core.dim_categories"],
                auto_apply=True,
                no_prompts=True,
            )

        before_edit = db.execute(
            "SELECT updated_at FROM core.dim_categories WHERE category_id = 'edit12345678'"
        ).fetchone()[0]
        before_keep = db.execute(
            "SELECT updated_at FROM core.dim_categories WHERE category_id = 'keep12345678'"
        ).fetchone()[0]

        time.sleep(0.01)  # ensure timestamp resolution

        # Edit one category.
        db.execute(
            "UPDATE app.user_categories "
            "SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP "
            "WHERE category_id = 'edit12345678'"
        )

        # Re-apply dim_categories to pick up the edit.
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=["core.dim_categories"],
                auto_apply=True,
                no_prompts=True,
            )

        after_edit = db.execute(
            "SELECT updated_at FROM core.dim_categories WHERE category_id = 'edit12345678'"
        ).fetchone()[0]
        after_keep = db.execute(
            "SELECT updated_at FROM core.dim_categories WHERE category_id = 'keep12345678'"
        ).fetchone()[0]

    assert before_edit is not None
    assert before_keep is not None
    assert after_edit > before_edit, "edited category's updated_at must advance"
    assert after_keep == before_keep, "unrelated category's updated_at must NOT advance"
