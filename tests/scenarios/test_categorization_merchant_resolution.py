"""Scenario: categorization resolves merchant by Plaid merchant_entity_id (M1T Task 7).

Two Plaid transactions that carry the SAME ``merchant_entity_id`` but different
descriptions must collapse to ONE canonical ``merchant_id`` — rung 4 mints a
merchant from the provider's data on the first transaction, rung 1 adopts the
freshly-minted binding on the second. The collapse happens through the real
MerchantResolver wired into the categorization orchestrator, not by pre-seeding
``app.merchant_links`` (per .claude/rules/testing.md "No Shortcuts").

This is a whole-pipeline test: it drives PlaidExtractor → account resolution →
SQLMesh transform so ``prep.int_transactions__merged`` and
``core.fct_transactions`` are real views before categorization runs.
"""

from __future__ import annotations

import pytest

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.services.account_resolution_types import SourceAccount
from moneybin.services.account_resolver import AccountResolver
from moneybin.services.categorization import CategorizationItem, CategorizationService
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

# Two transactions, one shared provider entity id, deliberately divergent
# descriptions so name matching alone would split them into two merchants.
_SHARED_ENTITY_ID = "entity_shared_coffee"
_PAYLOAD: dict[str, object] = {
    "accounts": [
        {
            "account_id": "acc_chase_check",
            "account_type": "depository",
            "account_subtype": "checking",
            "institution_name": "Chase",
            "official_name": "Total Checking",
            "mask": "1234",
        },
    ],
    "transactions": [
        {
            "transaction_id": "txn_coffee_a",
            "account_id": "acc_chase_check",
            "transaction_date": "2026-04-07",
            "amount": "4.25",
            "description": "STARBUCKS #1234 SEATTLE",
            "merchant_name": "Starbucks",
            "merchant_entity_id": _SHARED_ENTITY_ID,
            "pending": False,
        },
        {
            "transaction_id": "txn_coffee_b",
            "account_id": "acc_chase_check",
            "transaction_date": "2026-04-09",
            "amount": "6.10",
            "description": "SQ *STARBUCKS PIKE PLACE",
            "merchant_name": "Starbucks",
            "merchant_entity_id": _SHARED_ENTITY_ID,
            "pending": False,
        },
    ],
    "balances": [],
    "removed_transactions": [],
    "metadata": {
        "job_id": "550e8400-e29b-41d4-a716-446655440000",
        "synced_at": "2026-04-09T12:00:00Z",
        "institutions": [
            {
                "provider_item_id": "item_chase_abc",
                "institution_name": "Chase",
                "status": "completed",
                "transaction_count": 2,
            }
        ],
    },
}


@pytest.mark.scenarios
@pytest.mark.slow
def test_same_entity_id_collapses_to_one_merchant() -> None:
    """Two txns sharing merchant_entity_id resolve to a single merchant_id."""
    scenario = Scenario(
        scenario="categorization-merchant-entity-resolution",
        setup=SetupSpec(persona="curator"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        sync_data = SyncDataResponse.model_validate(_PAYLOAD)
        loader = PlaidExtractor(db)
        loader.load(sync_data, job_id=sync_data.metadata.job_id)

        # Resolve accounts so the staging JOIN produces canonical account ids.
        item_by_account = loader.build_account_to_item_map(sync_data)
        resolver = AccountResolver(db, actor="system")
        for acc in sync_data.accounts:
            resolver.resolve(
                SourceAccount(
                    source_type="plaid",
                    source_origin=item_by_account[acc.account_id],
                    source_account_key=acc.account_id,
                    account_name=acc.official_name or acc.account_id,
                    account_number=None,
                    last_four=acc.mask,
                    institution=acc.institution_name,
                )
            )

        # Materialize prep + core views (incl. prep.int_transactions__merged
        # and core.fct_transactions) through the real SQLMesh transform.
        run_step("transform", scenario.setup, db, env=env)

        # Ground truth derived from the input: both transactions carry the
        # shared entity id, so both gold rows must expose it in merged.
        gold_ids = [
            r[0]
            for r in db.execute(
                """
                SELECT t.transaction_id
                FROM core.fct_transactions AS t
                JOIN prep.int_transactions__merged AS m
                    ON t.transaction_id = m.transaction_id
                WHERE m.merchant_entity_id = ?
                ORDER BY t.transaction_date
                """,
                [_SHARED_ENTITY_ID],
            ).fetchall()
        ]
        assert len(gold_ids) == 2, (
            "both authored transactions must reach core.fct_transactions "
            "carrying the shared merchant_entity_id"
        )

        # Pick a valid active category so taxonomy validation accepts the items;
        # the merchant resolution under test is independent of which category.
        cat_row = db.execute(
            "SELECT category FROM core.dim_categories WHERE is_active "
            "ORDER BY category LIMIT 1"
        ).fetchone()
        category = cat_row[0] if cat_row else "Food & Drink"

        # Drive the real categorization path: the resolver mints on the first
        # item and adopts the binding on the second.
        result = CategorizationService(db).categorize_items([
            CategorizationItem(transaction_id=gold_ids[0], category=category),
            CategorizationItem(transaction_id=gold_ids[1], category=category),
        ])
        assert result.applied == 2, "both items must categorize"

        rows = db.execute(
            "SELECT transaction_id, merchant_id FROM app.transaction_categories "
            "WHERE transaction_id IN (?, ?)",
            [gold_ids[0], gold_ids[1]],
        ).fetchall()
        assert len(rows) == 2, "both transactions must have a categorization row"

        merchant_ids = {r[1] for r in rows}
        assert None not in merchant_ids, (
            "every entity-id-bearing transaction must resolve a merchant_id"
        )
        assert len(merchant_ids) == 1, (
            "two txns with the same merchant_entity_id must collapse to one "
            f"merchant_id, got {merchant_ids}"
        )
