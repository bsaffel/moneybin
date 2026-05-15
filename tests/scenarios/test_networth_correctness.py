"""Scenario: reports.net_worth daily totals match hand-computed ground truth."""

from __future__ import annotations

import pytest

from tests.scenarios._runner import load_shipped_scenario, scenario_env
from tests.scenarios._runner.steps import run_step

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
_EXPECTED: list[tuple[str, float]] = [
    ("2024-01-15", 10_475.00),
    ("2024-01-31", 10_410.00),
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
        db.execute(  # noqa: S608 — table literal; values parameterized
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

        for date_str, expected in _EXPECTED:
            row = db.execute(  # noqa: S608 — table name literal; date value parameterized
                "SELECT net_worth FROM reports.net_worth WHERE balance_date = ?",
                [date_str],
            ).fetchone()
            assert row is not None, f"no net_worth row for {date_str}"
            actual = float(row[0])
            assert abs(actual - expected) < 0.01, (
                f"net_worth on {date_str}: expected ${expected:.2f}, got ${actual:.2f}"
            )
