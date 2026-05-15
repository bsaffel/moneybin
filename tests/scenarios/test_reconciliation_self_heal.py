"""Scenario: reconciliation_delta goes non-zero then resolves to zero after gap-fill."""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import sqlmesh_context
from tests.scenarios._runner import load_shipped_scenario, scenario_env
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
from tests.scenarios._runner.loader import FixtureSpec
from tests.scenarios._runner.steps import run_step

# Fixture amounts (independently derived from fixture CSVs before running pipeline):
#
# fixture_a.csv (non-gap transactions):
#   2024-01-05: -10.00
#   2024-01-07: -15.00
#   2024-01-22: -20.00
#   2024-01-28: -55.00
#   Total: -$100.00
#
# fixture_b.csv (gap transactions, 2024-01-10 to 2024-01-19):
#   2024-01-10: -25.00
#   2024-01-13: -50.00
#   2024-01-16: -100.00
#   2024-01-19: -25.00
#   Total: -$200.00
#
# Balance assertions:
#   2024-01-01: $5,000.00 (starting)
#   2024-01-31: $4,700.00 (= $5,000 - $100 - $200 = all January transactions)
#
# Phase 1 (no gap): carry at Jan 31 = $5,000 - $100 = $4,900
#   delta = $4,700 (assertion) - $4,900 (carry) = -$200.00
#
# Phase 2 (gap filled): carry at Jan 31 = $5,000 - $100 - $200 = $4,700
#   delta = $4,700 (assertion) - $4,700 (carry) = $0.00

_START_BALANCE = Decimal("5000.00")
_END_BALANCE = Decimal("4700.00")  # starting - fixture_a_total - fixture_b_total
_EXPECTED_DELTA_PHASE1 = Decimal("-200.00")  # gap total missing → delta = gap total
_ACCOUNT = "recon-checking"
_OBS_DATE = "2024-01-31"


@pytest.mark.scenarios
@pytest.mark.slow
def test_reconciliation_self_heal() -> None:
    """reconciliation_delta resolves from non-zero to zero after gap transactions load.

    Two-phase test that does not fit the single-pass run_scenario() contract.
    Phase 1: load fixture_a + balance assertions → assert delta is -$200.
    Phase 2: load fixture_b (gap) → re-transform → assert delta is $0.
    """
    scenario = load_shipped_scenario("reconciliation-self-heal")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        # --- Phase 1: load partial fixture + balance assertions ---
        run_step("load_fixtures", scenario.setup, db, env=env)

        db.execute(  # noqa: S608 — table name literal; values parameterized
            """
            INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
            VALUES (?, ?, ?), (?, ?, ?)
            """,
            [
                _ACCOUNT,
                "2024-01-01",
                float(_START_BALANCE),
                _ACCOUNT,
                _OBS_DATE,
                float(_END_BALANCE),
            ],
        )

        run_step("transform", scenario.setup, db, env=env)

        row = db.execute(  # noqa: S608 — table/column literals; values parameterized
            """
            SELECT reconciliation_delta
            FROM core.fct_balances_daily
            WHERE account_id = ? AND balance_date = ?
            """,
            [_ACCOUNT, _OBS_DATE],
        ).fetchone()
        assert row is not None, f"no fct_balances_daily row for {_OBS_DATE}"
        delta_phase1 = Decimal(str(row[0]))
        assert delta_phase1 != Decimal("0"), (
            f"Phase 1: expected non-zero delta, got {delta_phase1}"
        )
        assert abs(delta_phase1 - _EXPECTED_DELTA_PHASE1) < Decimal("0.01"), (
            f"Phase 1: expected delta {_EXPECTED_DELTA_PHASE1}, got {delta_phase1}"
        )

        # --- Phase 2: load gap transactions and re-transform ---
        gap_spec = FixtureSpec(
            path="reconciliation-self-heal/fixture_b.csv",
            account=_ACCOUNT,
            source_type="csv",
        )
        load_fixture_into_db(db, gap_spec)

        # Force-restate fct_balances_daily so SQLMesh re-executes it even though
        # the model code is unchanged from the first plan. Without restate, SQLMesh
        # sees the model as already applied and skips it (FULL kind only skips when
        # previously applied in the same or a prior plan with identical fingerprints).
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                auto_apply=True,
                no_prompts=True,
                restate_models=["core.fct_balances_daily"],
            )

        row = db.execute(  # noqa: S608
            """
            SELECT reconciliation_delta
            FROM core.fct_balances_daily
            WHERE account_id = ? AND balance_date = ?
            """,
            [_ACCOUNT, _OBS_DATE],
        ).fetchone()
        assert row is not None, f"Phase 2: no fct_balances_daily row for {_OBS_DATE}"
        delta_phase2 = Decimal(str(row[0]))
        assert abs(delta_phase2) < Decimal("0.01"), (
            f"Phase 2: expected delta ≈ 0, got {delta_phase2}"
        )
