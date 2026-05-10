"""Scenario: manual transactions are exempt from auto-rule training.

Per transaction-curation spec Req 7 / Task 8, when a curator categorizes a
manual transaction, that categorization must NOT seed an auto-rule
proposal — manual rows are user-curated by definition and using them as
rule-training evidence conflates authoring intent with the
rule-deactivation signal that auto-rule training reads. The same
categorization on an imported row, however, IS valid training input.

Negative coverage in the same test: contrast pair with identical
description and identical user-supplied category on a manual row vs an
imported row. The imported row's transaction_id MUST appear in
``app.proposed_rules.sample_txn_ids``; the manual row's MUST NOT.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.services.auto_rule_service import AutoRuleService
from moneybin.services.transaction_service import TransactionService
from tests.scenarios._runner import load_shipped_scenario
from tests.scenarios._runner.fixture_loader import load_fixture_into_db
from tests.scenarios._runner.loader import FixtureSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step


@pytest.mark.scenarios
@pytest.mark.slow
def test_manual_entry_auto_rule_training() -> None:
    scenario = load_shipped_scenario("manual-entry-auto-rule-training")
    assert scenario is not None

    csv_fixture = FixtureSpec(
        path="curation-auto-rule/imported.csv",
        account="curation-checking",
        source_type="csv",
    )

    with scenario_env(scenario) as (db, _tmp, env):
        # Stage 1: import CSV row, transform, then create a manual row with
        # the same description so the contrast pair is realistic. Two
        # transform passes — once after the CSV import, again after the
        # manual write — populate core.fct_transactions for both rows.
        load_fixture_into_db(db, csv_fixture)
        # Order matters: ``transform`` materializes the prep + core tables
        # the matcher reads from, and ``match`` populates
        # ``app.seed_source_priority`` from MatchingSettings.
        # ``int_transactions__merged`` LEFT-JOINs that table — without
        # rows, ARG_MIN(...) with a NULL priority returns NULL for the
        # picked columns (description, amount, …), so we re-transform
        # after match to refresh the fact with non-NULL descriptions
        # before driving auto-rule training.
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        manual_result = TransactionService(db).create_manual_batch(
            [
                {
                    "account_id": "curation-checking",
                    "transaction_date": "2024-07-11",
                    "amount": Decimal("-6.25"),
                    "description": "BLUE BOTTLE COFFEE",
                }
            ],
            actor="cli",
        )
        run_step("transform", scenario.setup, db, env=env)

        manual_txn_id = manual_result.results[0].transaction_id

        # Look up the imported row's transaction_id by source_transaction_id.
        # It's the gold key the matcher's unmatched branch assigned.
        row = db.execute(
            """
            SELECT transaction_id FROM core.fct_transactions
             WHERE source_type = 'csv'
               AND transaction_date = DATE '2024-07-10'
               AND amount = -6.25
            """
        ).fetchone()
        assert row is not None, "imported CSV row missing from fct_transactions"
        imported_txn_id = str(row[0])
        assert manual_txn_id != imported_txn_id

        # Stage 2: drive AutoRuleService.record_categorization for both
        # rows with the same user-supplied category. The manual exemption
        # must filter the manual one out before any proposal mutation;
        # the imported one must produce (or update) a proposal whose
        # sample_txn_ids contains the imported transaction_id.
        auto = AutoRuleService(db)
        manual_pid = auto.record_categorization(manual_txn_id, "Coffee")
        imported_pid = auto.record_categorization(imported_txn_id, "Coffee")

        # Assertion 1 (negative): manual categorization returned None
        # (filter early-returned) and produced no proposal.
        assert manual_pid is None, (
            f"manual categorization yielded proposed_rule_id={manual_pid!r} — "
            f"manual exemption regressed"
        )

        # Assertion 2 (positive): imported categorization produced a proposal.
        assert imported_pid is not None, (
            "imported categorization yielded no proposal — auto-rule training "
            "filtered the imported row, which is the wrong side of the contrast"
        )

        # Assertion 3 (negative content check): the manual transaction_id
        # must NOT appear in any proposal's sample_txn_ids — even one created
        # for an unrelated reason.
        manual_in_samples = db.execute(
            """
            SELECT COUNT(*) FROM app.proposed_rules
             WHERE list_contains(sample_txn_ids, ?)
            """,
            [manual_txn_id],
        ).fetchone()
        assert manual_in_samples is not None
        assert int(manual_in_samples[0]) == 0, (
            f"manual transaction_id={manual_txn_id} leaked into "
            f"proposed_rules.sample_txn_ids"
        )

        # Assertion 4 (positive content check): the imported transaction_id
        # IS recorded in the proposal's sample_txn_ids.
        imported_in_samples = db.execute(
            """
            SELECT COUNT(*) FROM app.proposed_rules
             WHERE proposed_rule_id = ?
               AND list_contains(sample_txn_ids, ?)
            """,
            [imported_pid, imported_txn_id],
        ).fetchone()
        assert imported_in_samples is not None
        assert int(imported_in_samples[0]) == 1, (
            f"imported transaction_id={imported_txn_id} not recorded in "
            f"proposal sample_txn_ids — auto-rule sample tracking regressed"
        )
