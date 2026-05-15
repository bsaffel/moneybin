"""Verify core.fct_balances.updated_at convention.

updated_at carries the contributing observation's loaded_at (OFX/tabular) or
the assertion row's updated_at (user assertion — mutable on re-assertion).
Every row must have a non-NULL updated_at, user assertions must surface
their updated_at exactly, and edits to an existing assertion must advance
freshness. See docs/specs/core-updated-at-convention.md.
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
        total_row = db.execute("SELECT COUNT(*) FROM core.fct_balances").fetchone()
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
def test_user_assertion_carries_updated_at() -> None:
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
            "(account_id, assertion_date, balance, updated_at) "
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
        f"updated_at={row[0]} did not match inserted updated_at"
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_assertion_edit_advances_fct_balances_updated_at() -> None:
    """Editing an existing assertion via BalanceService advances fct_balances.updated_at.

    Pins the Codex P1 regression: prior to wiring app.balance_assertions.updated_at
    as the source column (and refreshing it on ON CONFLICT DO UPDATE in
    BalanceService.assert_balance), an edited assertion was invisible to
    "changed since T" consumers of fct_balances.
    """
    from datetime import date
    from decimal import Decimal

    from moneybin.services.balance_service import BalanceService

    scenario = load_shipped_scenario("idempotency-rerun")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("generate", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        account_row = db.execute(
            "SELECT account_id FROM core.dim_accounts LIMIT 1"
        ).fetchone()
        assert account_row is not None
        account_id = account_row[0]

        svc = BalanceService(db)
        svc.assert_balance(account_id, date(2020, 1, 1), Decimal("100.00"))
        before_row = db.execute(
            "SELECT updated_at FROM core.fct_balances "
            "WHERE account_id = ? AND source_type = 'assertion' "
            "AND balance_date = DATE '2020-01-01'",
            [account_id],
        ).fetchone()
        assert before_row is not None
        before = before_row[0]

        import time

        time.sleep(0.01)

        svc.assert_balance(account_id, date(2020, 1, 1), Decimal("200.00"))
        after_row = db.execute(
            "SELECT updated_at FROM core.fct_balances "
            "WHERE account_id = ? AND source_type = 'assertion' "
            "AND balance_date = DATE '2020-01-01'",
            [account_id],
        ).fetchone()
        assert after_row is not None
        after = after_row[0]

    assert after > before, (
        f"editing an assertion must advance fct_balances.updated_at "
        f"(before={before}, after={after})"
    )
