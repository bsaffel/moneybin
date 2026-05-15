"""Verify core.fct_balances.updated_at convention.

updated_at carries the contributing observation's loaded_at (OFX/tabular) or
created_at (user assertion). Every row must have a non-NULL updated_at, and
user assertions must surface their created_at exactly. See
docs/specs/core-updated-at-convention.md.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_updated_at_non_null_for_all_observations() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        null_row = db.execute(
            "SELECT COUNT(*) FROM core.fct_balances WHERE updated_at IS NULL"
        ).fetchone()
        total_row = db.execute(
            "SELECT COUNT(*) FROM core.fct_balances"
        ).fetchone()
        assert null_row is not None
        assert total_row is not None
        null_count = null_row[0]
        total_count = total_row[0]

    assert total_count > 0, "scenario must produce balance observations"
    assert null_count == 0, (
        f"{null_count}/{total_count} fct_balances rows have NULL updated_at"
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_user_assertion_carries_created_at() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        account_row = db.execute(
            "SELECT account_id FROM core.dim_accounts LIMIT 1"
        ).fetchone()
        assert account_row is not None, "scenario must include at least one account"
        account_id = account_row[0]

        db.execute(
            "INSERT INTO app.balance_assertions "
            "(account_id, assertion_date, balance, created_at) "
            "VALUES (?, DATE '2020-01-01', 1000.00, TIMESTAMP '2020-01-02 12:00:00')",
            [account_id],
        )

        # fct_balances is a VIEW — no restate needed; view re-evaluates on read.
        row = db.execute(
            "SELECT updated_at FROM core.fct_balances "
            "WHERE account_id = ? AND source_type = 'assertion' "
            "AND balance_date = DATE '2020-01-01'",
            [account_id],
        ).fetchone()

    assert row is not None, "user assertion did not surface in fct_balances"
    assert row[0] == datetime(2020, 1, 2, 12, 0, 0), (
        f"updated_at={row[0]} did not match inserted created_at"
    )
