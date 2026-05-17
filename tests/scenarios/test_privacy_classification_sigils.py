"""Verify privacy classification sigils land on core.* columns after transform.

Regression for the sqlmesh_context post-yield hook: the sync runs after
SQLMesh's register_comments has populated core.* descriptions, so the
catalog should show `... [class: <name>]` on every classified column.
"""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_core_columns_carry_class_sigil_after_transform() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        row = db.execute(
            """
            SELECT comment FROM duckdb_columns()
            WHERE schema_name = 'core'
              AND table_name = 'fct_transactions'
              AND column_name = 'account_id'
            """
        ).fetchone()

    assert row is not None and row[0] is not None
    assert "[class: account_identifier]" in row[0], (
        f"core.fct_transactions.account_id missing privacy sigil; got: {row[0]!r}"
    )
