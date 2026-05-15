"""Verify core.fct_transactions.updated_at convention.

updated_at = GREATEST over loaded_at and the latest input timestamp from
each app.* curation source (notes, tags, splits, categorization). User
edits to any input must advance updated_at. See
docs/specs/core-updated-at-convention.md.
"""

from __future__ import annotations

import time

import pytest

from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_updated_at_at_least_loaded_at() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        rows = db.execute(
            "SELECT transaction_id, updated_at, loaded_at "
            "FROM core.fct_transactions LIMIT 100"
        ).fetchall()

    assert rows
    for txn_id, updated_at, loaded_at in rows:
        assert updated_at >= loaded_at, (
            f"{txn_id}: updated_at={updated_at} < loaded_at={loaded_at}"
        )


@pytest.mark.scenarios
@pytest.mark.slow
def test_adding_note_advances_updated_at() -> None:
    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        txn_row = db.execute(
            "SELECT transaction_id FROM core.fct_transactions LIMIT 1"
        ).fetchone()
        assert txn_row is not None
        txn_id = txn_row[0]

        before_row = db.execute(
            "SELECT updated_at FROM core.fct_transactions WHERE transaction_id = ?",
            [txn_id],
        ).fetchone()
        assert before_row is not None
        before = before_row[0]

        # Ensure CURRENT_TIMESTAMP advances past `before` (timestamp resolution).
        time.sleep(0.01)

        db.execute(
            "INSERT INTO app.transaction_notes "
            "(note_id, transaction_id, text, author, created_at) "
            "VALUES ('testnote0001', ?, 'test', 'cli', CURRENT_TIMESTAMP)",
            [txn_id],
        )

        after_row = db.execute(
            "SELECT updated_at FROM core.fct_transactions WHERE transaction_id = ?",
            [txn_id],
        ).fetchone()
        assert after_row is not None
        after = after_row[0]

    assert after > before, (
        f"updated_at did not advance after note insert: before={before}, after={after}"
    )
