"""Scenario: MerchantResolver.harvest() backfills bindings from pre-existing categorizations.

TDD for M1T Task 8. harvest() reads the join of prep.int_transactions__merged
with app.transaction_categories (with merchant_id set) and writes bindings
for unambiguous (one entity_id -> one merchant_id) cases, routing conflicts
(one entity_id -> two+ merchant_ids) to the review queue.

Categorizations are seeded directly via MatchApplier.write_categorization, NOT
via CategorizationService. The real CategorizationService triggers the Task 7
resolver path, which binds the entity_id during categorization — leaving nothing
for harvest to backfill. Seeding via write_categorization is faithful input
setup per .claude/rules/testing.md "No Shortcuts": harvest produces bindings
in app.merchant_links (which we must not pre-seed); categorizations are its
input, not its output.

Expected counts derived from input payloads, not from running the program:
- _PAYLOAD_SINGLE: 1 transaction with entity_id=ent_seeded; seeded to merchant_id="m_seeded"
  -> harvest must produce exactly 1 binding.
- _PAYLOAD_CONFLICT: 2 transactions with entity_id=ent_conflict; seeded to two
  different merchant_ids -> harvest must produce 1 conflict proposal, 0 bindings.
"""

from __future__ import annotations

import pytest

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.services.account_resolution_types import SourceAccount
from moneybin.services.account_resolver import AccountResolver
from moneybin.services.audit_service import AuditService
from moneybin.services.categorization.applier import MatchApplier
from moneybin.services.merchant_resolver import HarvestResult, MerchantResolver
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

# Entity IDs embedded in the Plaid payloads; constants so assertions and
# seeding reference the same literal without quoting risk.
_ENTITY_SEEDED = "ent_seeded"
_ENTITY_CONFLICT = "ent_conflict"

# One transaction, one merchant_entity_id — canonical single-merchant backfill case.
_PAYLOAD_SINGLE: dict[str, object] = {
    "accounts": [
        {
            "account_id": "acc_harvest_a",
            "account_type": "depository",
            "account_subtype": "checking",
            "institution_name": "HarvestBank",
            "official_name": "Harvest Checking",
            "mask": "1001",
        },
    ],
    "transactions": [
        {
            "transaction_id": "txn_harvest_001",
            "account_id": "acc_harvest_a",
            "transaction_date": "2026-05-10",
            "amount": "8.75",
            "description": "HARVEST CAFE SEATTLE",
            "merchant_name": "Harvest Cafe",
            "merchant_entity_id": _ENTITY_SEEDED,
            "pending": False,
        },
    ],
    "balances": [],
    "removed_transactions": [],
    "metadata": {
        "job_id": "11111111-1111-1111-1111-111111111111",
        "synced_at": "2026-05-10T12:00:00Z",
        "institutions": [
            {
                "provider_item_id": "item_harvest_a",
                "institution_name": "HarvestBank",
                "status": "completed",
                "transaction_count": 1,
            }
        ],
    },
}

# Two transactions sharing one merchant_entity_id — exercises conflict detection.
_PAYLOAD_CONFLICT: dict[str, object] = {
    "accounts": [
        {
            "account_id": "acc_harvest_b",
            "account_type": "depository",
            "account_subtype": "checking",
            "institution_name": "ConflictBank",
            "official_name": "Conflict Checking",
            "mask": "2001",
        },
    ],
    "transactions": [
        {
            "transaction_id": "txn_conflict_001",
            "account_id": "acc_harvest_b",
            "transaction_date": "2026-05-11",
            "amount": "15.00",
            "description": "CONFLICT STORE ALPHA",
            "merchant_name": "Conflict Store",
            "merchant_entity_id": _ENTITY_CONFLICT,
            "pending": False,
        },
        {
            "transaction_id": "txn_conflict_002",
            "account_id": "acc_harvest_b",
            "transaction_date": "2026-05-12",
            "amount": "20.00",
            "description": "CONFLICT STORE BETA",
            "merchant_name": "Conflict Store",
            "merchant_entity_id": _ENTITY_CONFLICT,
            "pending": False,
        },
    ],
    "balances": [],
    "removed_transactions": [],
    "metadata": {
        "job_id": "22222222-2222-2222-2222-222222222222",
        "synced_at": "2026-05-12T12:00:00Z",
        "institutions": [
            {
                "provider_item_id": "item_conflict_b",
                "institution_name": "ConflictBank",
                "status": "completed",
                "transaction_count": 2,
            }
        ],
    },
}


@pytest.mark.scenarios
@pytest.mark.slow
def test_harvest_binds_existing_categorizations() -> None:
    """harvest() binds an entity_id that maps to exactly one merchant_id across history.

    Ground truth from input: _PAYLOAD_SINGLE carries 1 transaction with
    entity_id=ent_seeded. We seed 1 categorization row pointing merchant_id="m_seeded".
    harvest() must emit 1 binding (ent_seeded -> m_seeded) and be idempotent on
    a second run (0 new bindings because ent_seeded is already in app.merchant_links).
    """
    scenario = Scenario(
        scenario="harvest-binds-single-merchant",
        setup=SetupSpec(persona="curator"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        # --- Load Plaid data, resolve accounts, materialize prep/core views ---
        sync_data = SyncDataResponse.model_validate(_PAYLOAD_SINGLE)
        loader = PlaidExtractor(db)
        loader.load(sync_data, job_id=sync_data.metadata.job_id)

        item_by_account = loader.build_account_to_item_map(sync_data)
        acct_resolver = AccountResolver(db, actor="system")
        for acc in sync_data.accounts:
            acct_resolver.resolve(
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

        run_step("transform", scenario.setup, db, env=env)

        # --- Locate the canonical transaction_id for our seeded entity ---
        # Derived from input: exactly 1 transaction carries ent_seeded.
        row = db.execute(
            "SELECT transaction_id FROM prep.int_transactions__merged "
            "WHERE merchant_entity_id = ? ORDER BY transaction_id LIMIT 1",
            [_ENTITY_SEEDED],
        ).fetchone()
        assert row is not None, (
            f"No merged row found for entity_id={_ENTITY_SEEDED!r}; "
            "check that the Plaid payload reached prep.int_transactions__merged"
        )
        gold_id = row[0]

        # --- Pick a valid category so the categorization row passes validation ---
        cat_row = db.execute(
            "SELECT category FROM core.dim_categories WHERE is_active "
            "ORDER BY category LIMIT 1"
        ).fetchone()
        category = cat_row[0] if cat_row else "Food & Drink"

        # --- Seed app.transaction_categories with merchant_id="m_seeded" ---
        # Direct path via MatchApplier, not CategorizationService: the latter
        # would trigger the Task 7 resolver and bind ent_seeded during
        # categorization, leaving nothing for harvest to backfill.
        applier = MatchApplier(db, audit=AuditService(db))
        outcome = applier.write_categorization(
            transaction_id=gold_id,
            category=category,
            subcategory=None,
            categorized_by="user",
            merchant_id="m_seeded",
        )
        assert outcome.written, "Seeding categorization must succeed"

        # --- Run harvest and assert ---
        r = MerchantResolver(db)
        result = r.harvest()

        assert result.bound >= 1, f"Expected >= 1 binding; got {result}"
        assert r._links.lookup("plaid", _ENTITY_SEEDED) == "m_seeded", (  # pyright: ignore[reportPrivateUsage]
            f"harvest must bind {_ENTITY_SEEDED!r} -> 'm_seeded'; "
            f"got {r._links.lookup('plaid', _ENTITY_SEEDED)!r}"  # pyright: ignore[reportPrivateUsage]
        )

        # Second run must be idempotent: ent_seeded is now in app.merchant_links,
        # so harvest skips it and returns bound=0.
        result2 = r.harvest()
        assert result2.bound == 0, (
            f"Second harvest run must be idempotent (bound=0); got bound={result2.bound}"
        )
        assert isinstance(result, HarvestResult)


@pytest.mark.scenarios
@pytest.mark.slow
def test_harvest_routes_conflict_to_review() -> None:
    """harvest() routes an entity_id mapped to 2+ merchants to the review queue without binding.

    Ground truth from input: _PAYLOAD_CONFLICT carries 2 transactions sharing
    entity_id=ent_conflict. We seed each to a different merchant_id
    (m_conflict_a vs m_conflict_b), so the harvest join produces 2 distinct
    (entity_id, merchant_id) pairs. harvest() must produce 1 conflict, 0 bindings,
    and 1 pending decision in app.merchant_link_decisions.
    """
    scenario = Scenario(
        scenario="harvest-conflict-entity",
        setup=SetupSpec(persona="curator"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        # --- Load Plaid data, resolve accounts, materialize prep/core views ---
        sync_data = SyncDataResponse.model_validate(_PAYLOAD_CONFLICT)
        loader = PlaidExtractor(db)
        loader.load(sync_data, job_id=sync_data.metadata.job_id)

        item_by_account = loader.build_account_to_item_map(sync_data)
        acct_resolver = AccountResolver(db, actor="system")
        for acc in sync_data.accounts:
            acct_resolver.resolve(
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

        run_step("transform", scenario.setup, db, env=env)

        # --- Locate both canonical transaction_ids for ent_conflict ---
        # Derived from input: exactly 2 transactions carry ent_conflict.
        rows = db.execute(
            "SELECT transaction_id FROM prep.int_transactions__merged "
            "WHERE merchant_entity_id = ? ORDER BY transaction_id",  # noqa: S608  # static identifiers
            [_ENTITY_CONFLICT],
        ).fetchall()
        assert len(rows) == 2, (
            f"Expected 2 merged rows for entity_id={_ENTITY_CONFLICT!r}; got {len(rows)}"
        )
        gold_id_a, gold_id_b = rows[0][0], rows[1][0]

        # --- Pick a valid category ---
        cat_row = db.execute(
            "SELECT category FROM core.dim_categories WHERE is_active "
            "ORDER BY category LIMIT 1"
        ).fetchone()
        category = cat_row[0] if cat_row else "Food & Drink"

        # --- Seed different merchant_ids on each transaction ---
        # txn_a -> m_conflict_a, txn_b -> m_conflict_b.
        # The harvest SQL groups by (source_type, entity_id, merchant_id) and sees
        # two distinct pairs for ent_conflict -> conflict branch fires.
        applier = MatchApplier(db, audit=AuditService(db))
        for gold_id, merchant_id in [
            (gold_id_a, "m_conflict_a"),
            (gold_id_b, "m_conflict_b"),
        ]:
            outcome = applier.write_categorization(
                transaction_id=gold_id,
                category=category,
                subcategory=None,
                categorized_by="user",
                merchant_id=merchant_id,
            )
            assert outcome.written, (
                f"Seeding categorization for {gold_id!r} must succeed"
            )

        # --- Run harvest and assert conflict handling ---
        r = MerchantResolver(db)
        result = r.harvest()

        assert result.conflicts == 1, (
            f"Expected 1 conflict for {_ENTITY_CONFLICT!r}; got {result}"
        )
        assert r._links.lookup("plaid", _ENTITY_CONFLICT) is None, (  # pyright: ignore[reportPrivateUsage]
            f"harvest must NOT silently bind a conflicted entity_id; "
            f"got {r._links.lookup('plaid', _ENTITY_CONFLICT)!r}"  # pyright: ignore[reportPrivateUsage]
        )
        pending_after_first = r._decisions.list_pending()  # pyright: ignore[reportPrivateUsage]
        assert len(pending_after_first) >= 1, (
            "harvest must route the conflict to app.merchant_link_decisions"
        )
        assert isinstance(result, HarvestResult)

        # Second run must be idempotent on the conflict path too: the dedup
        # guard in _propose skips re-proposing an already-pending conflict, so
        # the pending-decision count must not grow.
        result2 = r.harvest()
        assert result2.bound == 0, (
            f"second harvest must not bind a still-conflicted entity; got {result2}"
        )
        pending_after_second = r._decisions.list_pending()  # pyright: ignore[reportPrivateUsage]
        assert len(pending_after_second) == len(pending_after_first), (
            "re-running harvest must not stack duplicate pending conflict rows: "
            f"{len(pending_after_first)} → {len(pending_after_second)}"
        )

        # --- [5] A user-rejected conflict must NOT be re-proposed on re-run ---
        # Reject the pending conflict the real way (user picks "new"), then
        # re-harvest: the rejected (non-reversed) decision must suppress
        # re-proposal so the queue drains instead of re-filling every run.
        from moneybin.services.merchant_links_service import (  # noqa: PLC0415
            MerchantLinksService,
        )

        conflict_decision_id = next(
            d["decision_id"]
            for d in pending_after_second
            if d["ref_value"] == _ENTITY_CONFLICT
        )
        MerchantLinksService(db, actor="cli").set(
            conflict_decision_id, target_merchant_id=None
        )

        r.harvest()
        pending_after_reject = [
            d
            for d in r._decisions.list_pending()  # pyright: ignore[reportPrivateUsage]
            if d["ref_value"] == _ENTITY_CONFLICT
        ]
        assert pending_after_reject == [], (
            "a rejected conflict must not be re-proposed by a subsequent harvest; "
            f"got {len(pending_after_reject)} pending for {_ENTITY_CONFLICT!r}"
        )
