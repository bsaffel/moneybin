"""Scenario: a merchant_entity_id binds on the entity-ISSUING provider, never the merge winner.

Regression for the M1T final-review finding. A Plaid ``merchant_entity_id``
riding an OFX+Plaid-deduped transaction collapses to a merged row whose
``canonical_source_type`` is the merge winner (``ofx`` outranks ``plaid`` in
``seed_source_priority``). If merchant resolution keyed on
``canonical_source_type`` it would bind under ``('ofx', E)``, while a Plaid-only
sibling sharing the same entity id looks up ``('plaid', E)`` → miss → a SECOND
merchant minted for the same real merchant (a silent split).

The fix carries ``merchant_entity_source_type`` — the source_type of the merge
member that issued the entity id (always ``plaid`` here) — and keys resolution
on THAT. This scenario exercises the real chain end-to-end:

1. Plaid load (two txns sharing entity id ``E``) + an OFX twin of one of them,
   forced onto the same canonical account (explicit binding — a real user
   mechanism; cross-source *account* matching is proven separately in
   ``test_account_identity_cross_source``). The real dedup matcher then collapses
   the OFX+Plaid pair (same account, date, amount → auto-merge).
2. Merged-model assertion: the deduped row has ``canonical_source_type='ofx'``
   BUT ``merchant_entity_source_type='plaid'`` and carries the Plaid entity id.
3. Real categorization of both entity-bearing rows must land ONE shared
   ``merchant_id`` — no split.

All expected values are hand-derived from the input payloads, not observed output.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import polars as pl
import pytest

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.database import Database
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.services.account_resolution_types import SourceAccount
from moneybin.services.account_resolver import AccountResolver
from moneybin.services.categorization import CategorizationItem, CategorizationService
from moneybin.tables import OFX_ACCOUNTS, OFX_TRANSACTIONS
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

# Shared provider entity id carried by BOTH Plaid transactions.
_ENTITY = "ent_shared_split_guard"
_PLAID_ACCOUNT = "plaid_acct_split"
_OFX_ACCOUNT = "ofx_acct_split"
_DEDUP_DATE = "2026-05-10"
# Plaid raw amount is positive (Plaid: positive = expense); staging flips to
# -8.75. The OFX twin's raw amount must already be -8.75 so both unioned rows
# carry the identical signed amount and the cross-source dedup blocks match.
_PLAID_RAW_AMOUNT = "8.75"
_OFX_RAW_AMOUNT = Decimal("-8.75")

_PLAID_PAYLOAD: dict[str, object] = {
    "accounts": [
        {
            "account_id": _PLAID_ACCOUNT,
            "account_type": "depository",
            "account_subtype": "checking",
            "institution_name": "SplitGuardBank",
            "official_name": "Split Guard Checking",
            "mask": "9001",
        },
    ],
    "transactions": [
        {
            # Twin of the OFX row — collapses into one merged row (canonical
            # source_type becomes 'ofx', the merge winner).
            "transaction_id": "txn_plaid_dedup",
            "account_id": _PLAID_ACCOUNT,
            "transaction_date": _DEDUP_DATE,
            "amount": _PLAID_RAW_AMOUNT,
            "description": "SHARED MERCHANT CO",
            "merchant_name": "Shared Merchant Co",
            "merchant_entity_id": _ENTITY,
            "pending": False,
        },
        {
            # Plaid-only sibling sharing the SAME entity id; not deduped, so its
            # canonical_source_type stays 'plaid'. Pre-fix this looked up
            # ('plaid', E) while the deduped twin bound ('ofx', E) → split.
            "transaction_id": "txn_plaid_only",
            "account_id": _PLAID_ACCOUNT,
            "transaction_date": "2026-05-20",
            "amount": "12.00",
            "description": "SHARED MERCHANT CO",
            "merchant_name": "Shared Merchant Co",
            "merchant_entity_id": _ENTITY,
            "pending": False,
        },
    ],
    "balances": [],
    "removed_transactions": [],
    "metadata": {
        "job_id": "33333333-3333-3333-3333-333333333333",
        "synced_at": "2026-05-20T12:00:00Z",
        "institutions": [
            {
                "provider_item_id": "item_split_guard",
                "institution_name": "SplitGuardBank",
                "status": "completed",
                "transaction_count": 2,
            }
        ],
    },
}


def _seed_ofx_twin(db: Database, *, account_id: str) -> None:
    """Insert one raw OFX account + transaction (the dedup twin of the Plaid row).

    Mirrors ``tests.scenarios._runner.fixture_loader`` column shapes — faithful
    raw input, not a derived-state shortcut. ``source_origin='fixture'`` so the
    account-link JOIN in stg_ofx resolves the canonical account.
    """
    now = datetime.now(UTC)
    db.ingest_dataframe(
        OFX_ACCOUNTS.full_name,
        pl.DataFrame([
            {
                "account_id": account_id,
                "routing_number": None,
                "account_type": "CHECKING",
                "institution_org": "fixture",
                "institution_fid": None,
                "source_origin": "fixture",
                "source_file": "split-guard-fixture",
                "extracted_at": now,
            }
        ]),
        on_conflict="upsert",
    )
    db.ingest_dataframe(
        OFX_TRANSACTIONS.full_name,
        pl.DataFrame([
            {
                "source_transaction_id": "ofx_dedup_001",
                "account_id": account_id,
                "transaction_type": "DEBIT",
                "date_posted": datetime(2026, 5, 10, tzinfo=UTC).replace(tzinfo=None),
                "amount": _OFX_RAW_AMOUNT,
                "payee": "SHARED MERCHANT CO",
                "memo": None,
                "check_number": None,
                "source_file": "split-guard-fixture",
                "source_origin": "fixture",
                "extracted_at": now,
            }
        ]).with_columns(pl.col("amount").cast(pl.Decimal(18, 2))),
        on_conflict="insert",
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_entity_id_binds_on_issuing_provider_not_merge_winner() -> None:
    """OFX+Plaid dedup must not split a shared merchant_entity_id across two merchants.

    Ground truth from input: both Plaid txns carry ``E``; one has an OFX twin
    (same account/date/amount) that the matcher auto-merges. The deduped merged
    row's merge winner is 'ofx', but the entity id was issued by 'plaid'.
    """
    scenario = Scenario(
        scenario="merchant-entity-id-dedup-no-split",
        setup=SetupSpec(persona="curator"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        # --- Load Plaid, resolve its account → canonical id C ---
        sync_data = SyncDataResponse.model_validate(_PLAID_PAYLOAD)
        loader = PlaidExtractor(db)
        loader.load(sync_data, job_id=sync_data.metadata.job_id)

        item_by_account = loader.build_account_to_item_map(sync_data)
        acct_resolver = AccountResolver(db, actor="system")
        plaid_acc = sync_data.accounts[0]
        resolved = acct_resolver.resolve(
            SourceAccount(
                source_type="plaid",
                source_origin=item_by_account[plaid_acc.account_id],
                source_account_key=plaid_acc.account_id,
                account_name=plaid_acc.official_name or plaid_acc.account_id,
                account_number=None,
                last_four=plaid_acc.mask,
                institution=plaid_acc.institution_name,
            )
        )
        canonical_account_id = resolved.account_id

        # --- Seed the OFX twin and force its account onto the same canonical id ---
        # Explicit binding (decided_by='user') is a real mechanism; the *account*
        # match is not the subject under test (see module docstring).
        _seed_ofx_twin(db, account_id=_OFX_ACCOUNT)
        acct_resolver.resolve(
            SourceAccount(
                source_type="ofx",
                source_origin="fixture",
                source_account_key=_OFX_ACCOUNT,
                account_name="Split Guard Checking (OFX)",
                account_number=None,
                last_four="9001",
                institution="fixture",
                explicit_account_id=canonical_account_id,
            )
        )

        # --- Real pipeline: transform → dedup match → transform ---
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        # --- Merged-model assertion: the deduped row keys the entity id on plaid ---
        # Derived from input: exactly the txn_plaid_dedup/OFX pair collapses
        # (source_count = 2); its merge winner is 'ofx' but the entity issuer is
        # 'plaid'.
        dedup_row = db.execute(
            """
            SELECT canonical_source_type, merchant_entity_source_type,
                   merchant_entity_id, source_count
            FROM prep.int_transactions__merged
            WHERE merchant_entity_id = ? AND source_count = 2
            """,
            [_ENTITY],
        ).fetchone()
        assert dedup_row is not None, (
            "expected one OFX+Plaid-deduped merged row carrying the entity id"
        )
        assert dedup_row[0] == "ofx", (
            f"merge winner should be 'ofx' (outranks plaid); got {dedup_row[0]!r}"
        )
        assert dedup_row[1] == "plaid", (
            "merchant_entity_source_type must be the issuing provider 'plaid', "
            f"NOT the merge winner; got {dedup_row[1]!r}"
        )
        assert dedup_row[2] == _ENTITY

        # --- Locate the gold ids for both entity-bearing merged rows ---
        gold_rows = db.execute(
            """
            SELECT transaction_id, source_count
            FROM prep.int_transactions__merged
            WHERE merchant_entity_id = ?
            ORDER BY source_count DESC
            """,
            [_ENTITY],
        ).fetchall()
        # Derived from input: two entity-bearing merged rows — the dedup
        # (source_count 2) and the Plaid-only sibling (source_count 1).
        assert len(gold_rows) == 2, (
            f"expected 2 entity-bearing merged rows; got {len(gold_rows)}"
        )
        gold_dedup = gold_rows[0][0]
        gold_plaid_only = gold_rows[1][0]

        # --- Real categorization of both rows must produce ONE shared merchant ---
        cat_row = db.execute(
            "SELECT category FROM core.dim_categories WHERE is_active "
            "ORDER BY category LIMIT 1"
        ).fetchone()
        category = cat_row[0] if cat_row else "Food & Drink"

        result = CategorizationService(db).categorize_items([
            CategorizationItem(transaction_id=gold_dedup, category=category),
            CategorizationItem(transaction_id=gold_plaid_only, category=category),
        ])
        assert result.applied == 2, f"both rows must categorize; got {result}"

        merchant_ids = db.execute(
            """
            SELECT transaction_id, merchant_id
            FROM app.transaction_categories
            WHERE transaction_id IN (?, ?)
            ORDER BY transaction_id
            """,
            [gold_dedup, gold_plaid_only],
        ).fetchall()
        resolved_ids = dict(merchant_ids)
        assert resolved_ids.get(gold_dedup) is not None, (
            "deduped row must resolve a merchant_id"
        )
        assert resolved_ids.get(gold_dedup) == resolved_ids.get(gold_plaid_only), (
            "the shared merchant_entity_id must bind to ONE merchant across the "
            "OFX-deduped row and the Plaid-only sibling — no split. "
            f"got {resolved_ids}"
        )

        # --- [1] Entity-bearing member wins even when 'plaid' is absent from
        # source_priority. Drop plaid from the priority seed and re-read the
        # merged VIEW (recomputes on read): merchant_entity_source_type must
        # STILL be 'plaid'. Under the old sentinel the entity-bearing plaid
        # member and the entity-less ofx member tied at 2147483647 and ARG_MIN
        # could pick 'ofx' → the cross-source split returns. The 2147483646
        # sentinel for entity-bearing members breaks the tie toward the issuer
        # regardless of priority config.
        db.execute("DELETE FROM app.seed_source_priority WHERE source_type = 'plaid'")
        reread = db.execute(
            """
            SELECT merchant_entity_source_type
            FROM prep.int_transactions__merged
            WHERE merchant_entity_id = ? AND source_count = 2
            """,
            [_ENTITY],
        ).fetchone()
        assert reread is not None and reread[0] == "plaid", (
            "with plaid absent from source_priority, the entity-issuing member "
            f"must still win merchant_entity_source_type; got "
            f"{reread[0] if reread else None!r}"
        )
