"""Scenario: reports.net_worth daily totals match hand-computed ground truth."""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import sqlmesh_context
from moneybin.repositories.profile_settings_repo import ProfileSettingsRepo
from moneybin.services.account_service import AccountService
from tests.scenarios._runner import load_shipped_scenario, scenario_env
from tests.scenarios._runner.steps import run_step

# core.fct_balances_daily and core.fct_exchange_rates_daily are both kind
# FULL; they capture account_settings.currency_code at build time, so the
# post-transform currency fix below needs an explicit restatement to reach
# them — the same pattern test_international_realized_fx_ground_truth uses
# for its own post-transform app.profile_settings change.
_CURRENCY_RESTATE_MODELS = [
    "core.fct_balances_daily",
    "core.fct_exchange_rates_daily",
]

# Hand-computed expected net worth at sampled dates.
#
# Fixture data (independently derived before running the pipeline):
#
# checking (account=networth-checking), starting balance 2024-01-01 = $5,000.00:
#   Jan 05: -10.00  → carry $4,990.00
#   Jan 07: -15.00  → carry $4,975.00
#   Jan 22: -20.00  → carry $4,955.00
#   Jan 28: -55.00  → carry $4,900.00
#   Assertion Jan 31 = $4,900.00 (matches carry: no gap)
#
# savings (account=networth-savings), starting balance 2024-01-01 = $5,000.00:
#   Jan 10: +500.00 → carry $5,500.00
#   Jan 25: +10.00  → carry $5,510.00
#   Assertion Jan 31 = $5,510.00 (matches carry: no gap)
#   Feb 10: +500.00 → carry $6,010.00
#   Feb 25: +10.00  → carry $6,020.00
#   Assertion Feb 29 = $6,020.00
#
# Sampled dates and expected net worth:
#   2024-01-15: checking carry = $4,975 (after Jan5:-10, Jan7:-15)
#               savings carry  = $5,500 (after Jan10:+500)
#               → net worth = $4,975 + $5,500 = $10,475.00
#   2024-01-31: checking = $4,900 (assertion), savings = $5,510 (assertion)
#               → net worth = $4,900 + $5,510 = $10,410.00
_EXPECTED: list[tuple[str, Decimal]] = [
    ("2024-01-15", Decimal("10475.00")),
    ("2024-01-31", Decimal("10410.00")),
]


@pytest.mark.scenarios
@pytest.mark.slow
def test_networth_correctness() -> None:
    """reports.net_worth matches hand-computed balances at sampled dates.

    Balance assertions are seeded directly between load_fixtures and transform
    because they must be present when SQLMesh materialises fct_balances_daily.
    Expected values are derived from fixture amounts before running the pipeline
    (per testing.md: no observe-and-paste).
    """
    scenario = load_shipped_scenario("networth-correctness")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        run_step("load_fixtures", scenario.setup, db, env=env)

        # Seed balance assertions computed from fixture data (hand-derived).
        # checking: start=$5,000; all txns total -$100 → end=$4,900
        # savings:  start=$5,000; Jan txns total +$510 → Jan end=$5,510
        db.execute(  # table literal; values parameterized
            """
            INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
            VALUES
              ('networth-checking', '2024-01-01', 5000.00),
              ('networth-checking', '2024-01-31', 4900.00),
              ('networth-savings',  '2024-01-01', 5000.00),
              ('networth-savings',  '2024-01-31', 5510.00)
            """
        )

        run_step("transform", scenario.setup, db, env=env)

        # The fixture loader never captures a currency for either account
        # (tests/scenarios/_runner/fixture_loader.py has no such field), so
        # both are the "unknown currency" segment: reports.net_worth's day
        # rung fails closed on that segment permanently, regardless of home
        # currency, until the account's own currency is actually known. Set
        # it the way a real user would (`accounts set --currency`, here via
        # AccountService.settings_update — app.account_settings, per
        # reports.md's documented remedy), then choose a home currency.
        svc = AccountService(db)
        svc.settings_update("networth-checking", currency_code="USD", actor="system")
        svc.settings_update("networth-savings", currency_code="USD", actor="system")
        ProfileSettingsRepo(db).set_home_currency("USD", actor="system")

        # Both app.account_settings and app.profile_settings are external to
        # SQLMesh, and core.fct_balances_daily / core.fct_exchange_rates_daily
        # are kind FULL: they captured the (then-unknown) currency at the
        # first transform and won't re-resolve it on their own. Restate them
        # explicitly so the correction actually reaches reports.net_worth,
        # which is a VIEW and re-evaluates on every read once its FULL inputs
        # are current.
        with sqlmesh_context(db) as ctx:
            ctx.plan(
                restate_models=_CURRENCY_RESTATE_MODELS,
                auto_apply=True,
                no_prompts=True,
            )

        for date_str, expected in _EXPECTED:
            row = db.execute(  # table name literal; date value parameterized
                "SELECT net_worth FROM reports.net_worth WHERE balance_date = ?",
                [date_str],
            ).fetchone()
            assert row is not None, f"no net_worth row for {date_str}"
            actual = Decimal(str(row[0]))
            assert abs(actual - expected) < Decimal("0.01"), (
                f"net_worth on {date_str}: expected ${expected}, got ${actual}"
            )
