"""Scenario: manual entries are exempt from same-record dedup matching.

A manual transaction created on the same (account_id, transaction_date,
amount, description) as a CSV-imported row must NOT be auto-merged with
the imported row. Per transaction-curation spec Req 6 / Task 8, manual
rows are exempt from the matcher candidate-pair generation
(``a.source_type != 'manual' AND b.source_type != 'manual'`` in
``moneybin/matching/scoring.py``); the user retains intent and reconciles
by hand.

Negative coverage: this also asserts that ``app.match_decisions`` carries
zero rows linking the manual row to the CSV row, so a regression that
silently dropped the manual exemption from the SQL would fail here even
if the gold-record count happened to coincide.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.services.matching_service import MatchingService
from moneybin.services.transaction_service import TransactionService
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
from tests.scenarios._runner.loader import FixtureSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_manual_entry_dedup() -> None:
    scenario = load_shipped_scenario("manual-entry-dedup")
    assert scenario is not None

    csv_fixture = FixtureSpec(
        path="curation-manual-dedup/imported.csv",
        account="curation-checking",
        source_type="csv",
    )

    with scenario_env(scenario) as (db, _tmp, env):
        # Stage 1: import the CSV row, transform so dim_accounts +
        # fct_transactions are populated, then match. Match must run before
        # the next transform pass so ``app.seed_source_priority`` is seeded
        # — without it, ARG_MIN-based merge in int_transactions__merged
        # returns NULL for picked columns (description, amount, …).
        load_fixture_into_db(db, csv_fixture)
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)

        # Stage 2: create a manual transaction with the same (account_id,
        # transaction_date, amount, description) as the CSV row. The manual
        # write goes to raw.manual_transactions; transform unions it into
        # core.fct_transactions.
        TransactionService(db).create_manual_batch(
            [
                {
                    "account_id": "curation-checking",
                    "transaction_date": "2024-06-15",
                    "amount": Decimal("-4.50"),
                    "description": "COFFEE SHOP",
                }
            ],
            actor="cli",
        )
        run_step("transform", scenario.setup, db, env=env)

        # Stage 3: run the matcher. The manual row must NOT be paired with
        # the CSV row.
        MatchingService(db).run(auto_accept_transfers=True)

        # Assertion 1: two distinct gold records on the matched (date, amount,
        # description) — one CSV, one manual.
        rows = db.execute(
            """
            SELECT source_type, COUNT(*) AS n
              FROM core.fct_transactions
             WHERE transaction_date = DATE '2024-06-15'
               AND amount = -4.50
             GROUP BY source_type
             ORDER BY source_type
            """
        ).fetchall()
        # Hand-derived: one CSV row + one manual row = two distinct sources,
        # one row each. A failure here means either the manual row didn't
        # land in core.fct_transactions, or the matcher merged it.
        assert rows == [("csv", 1), ("manual", 1)], (
            f"expected one csv row + one manual row, got {rows}"
        )

        # Assertion 2: the matcher's candidate-pair table holds zero rows
        # involving the manual row. ``app.match_decisions`` records the
        # source_type of each side of the proposed pair, so we filter on
        # the columns directly without joining back to fct_transactions.
        manual_match_count = db.execute(
            """
            SELECT COUNT(*)
              FROM app.match_decisions
             WHERE source_type_a = 'manual' OR source_type_b = 'manual'
            """
        ).fetchone()
        assert manual_match_count is not None
        assert int(manual_match_count[0]) == 0, (
            f"matcher generated {manual_match_count[0]} candidate pairs "
            f"involving a manual row — manual exemption regressed"
        )
