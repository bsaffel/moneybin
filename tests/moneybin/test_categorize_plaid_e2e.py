"""End-to-end: raw Plaid PFC data -> staging -> categorize_pending -> core.fct_transactions.

Complements the unit-level ``apply_plaid_categories`` / ``categorize_pending``
coverage in ``test_categorization_service.py`` (which stubs
``prep.stg_plaid__transactions`` as a bare 4-column table and seeds one
synthetic ``seeds.category_source_map`` row per case) by proving the WHOLE
chain works against a REAL SQLMesh transform and the REAL curated bridge data
(``sqlmesh/models/seeds/category_source_map.csv`` + ``categories.csv``):

    raw.plaid_transactions -> prep.stg_plaid__transactions (sign-flip view)
    -> categorize_pending()'s apply_plaid_categories pass
    -> app.transaction_categories -> core.fct_transactions

No pre-inserted ``app.transaction_categories`` rows, no synthetic bridge
seeding, no mocked categorizer — every step is the real mechanism.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from moneybin.connectors.sync_models import (
    InstitutionResult,
    SyncAccount,
    SyncDataResponse,
    SyncMetadata,
    SyncTransaction,
)
from moneybin.database import Database, sqlmesh_context
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.services.categorization import CategorizationService

pytestmark = pytest.mark.integration

_JOB_ID = "job-plaid-pfc-e2e"
_ACCOUNT_ID = "acc_pfc_e2e_checking"
_ITEM_ID = "item_pfc_e2e"


def _build_sync_data() -> SyncDataResponse:
    """Four PFC-coded transactions, hand-picked against the real seed CSV.

    Expectations below are derived by reading
    ``sqlmesh/models/seeds/category_source_map.csv`` and
    ``sqlmesh/models/seeds/categories.csv`` directly, not by observing what
    the categorizer produces:

    - ``txn_pfc_detailed``: ``category_detailed='FOOD_AND_DRINK_GROCERIES'``
      is bridge-mapped at the detailed tier (-> category_id ``FND-GRC`` =
      "Food & Drink"/"Groceries"). The co-present primary code
      ``FOOD_AND_DRINK`` is ALSO bridge-mapped (-> ``FND``), so this
      transaction proves the QUALIFY dedup picks detailed over primary
      rather than just "the only mapped code wins". confidence=HIGH (0.90).
    - ``txn_pfc_primary_fallback``: detailed code
      ``TRANSPORTATION_BIKES_AND_SCOOTERS`` does NOT appear anywhere in the
      seed CSV; primary ``TRANSPORTATION`` DOES (-> ``TRP`` =
      "Transportation"/None) -> primary-fallback path.
      confidence=MEDIUM (0.70) exercises the `>= gate` boundary exactly
      (``PLAID_MIN_CONFIDENCE = 0.70``; gate rejects only `< 0.70`).
    - ``txn_pfc_low_confidence`` (NEGATIVE): detailed code
      ``FOOD_AND_DRINK_COFFEE`` IS bridge-mapped (-> ``FND-COF``), but
      confidence=LOW (0.40 < 0.70) -> must stay uncategorized despite a
      valid mapping existing.
    - ``txn_pfc_unmapped`` (NEGATIVE): neither the detailed nor the primary
      code appears anywhere in the seed CSV, confidence=VERY_HIGH -> proves
      a missing bridge row (not confidence) is what blocks categorization.
    """
    return SyncDataResponse(
        accounts=[
            SyncAccount(
                account_id=_ACCOUNT_ID,
                account_type="depository",
                account_subtype="checking",
                institution_name="PFC Test Bank",
                official_name="PFC E2E Checking",
                mask="9911",
            )
        ],
        transactions=[
            SyncTransaction(
                transaction_id="txn_pfc_detailed",
                account_id=_ACCOUNT_ID,
                transaction_date=date(2026, 6, 1),
                amount=Decimal("54.32"),
                description="ACME GROCERY TEST DETAILED",
                category="FOOD_AND_DRINK",
                category_detailed="FOOD_AND_DRINK_GROCERIES",
                category_confidence="HIGH",
                pending=False,
            ),
            SyncTransaction(
                transaction_id="txn_pfc_primary_fallback",
                account_id=_ACCOUNT_ID,
                transaction_date=date(2026, 6, 2),
                amount=Decimal("2.75"),
                description="ACME TRANSIT TEST FALLBACK",
                category="TRANSPORTATION",
                category_detailed="TRANSPORTATION_BIKES_AND_SCOOTERS",
                category_confidence="MEDIUM",
                pending=False,
            ),
            SyncTransaction(
                transaction_id="txn_pfc_low_confidence",
                account_id=_ACCOUNT_ID,
                transaction_date=date(2026, 6, 3),
                amount=Decimal("6.10"),
                description="ACME COFFEE TEST LOW CONFIDENCE",
                category="FOOD_AND_DRINK",
                category_detailed="FOOD_AND_DRINK_COFFEE",
                category_confidence="LOW",
                pending=False,
            ),
            SyncTransaction(
                transaction_id="txn_pfc_unmapped",
                account_id=_ACCOUNT_ID,
                transaction_date=date(2026, 6, 4),
                amount=Decimal("19.99"),
                description="ACME MYSTERY VENDOR TEST UNMAPPED",
                category="SOME_FUTURE_PRIMARY_CODE",
                category_detailed="SOME_FUTURE_DETAILED_CODE",
                category_confidence="VERY_HIGH",
                pending=False,
            ),
        ],
        balances=[],
        removed_transactions=[],
        metadata=SyncMetadata(
            job_id=_JOB_ID,
            synced_at=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
            institutions=[
                InstitutionResult(
                    provider_item_id=_ITEM_ID,
                    institution_name="PFC Test Bank",
                    status="completed",
                    transaction_count=4,
                )
            ],
        ),
    )


def _gold_id(db: Database, description: str) -> str:
    """Resolve a transaction's gold (merged) transaction_id via its description.

    core.fct_transactions.transaction_id is ALWAYS a SHA-256 content hash
    computed by prep.int_transactions__matched — never the source-native id
    a provider assigns — even for a transaction with zero dedup matches (see
    that model's ``group_gold_keys`` fallback). Every existing Plaid
    integration test in this suite (test_sync_e2e.py, test_stg_plaid.py)
    filters core.fct_transactions by description for exactly this reason;
    filtering by the native Plaid transaction_id against core.fct_transactions
    would never match anything, by design, for ANY source.
    """
    row = db.execute(
        "SELECT transaction_id FROM core.fct_transactions WHERE description = ?",
        [description],
    ).fetchone()
    assert row is not None, (
        f"transaction {description!r} did not reach core.fct_transactions"
    )
    return str(row[0])


@pytest.mark.slow
def test_plaid_pfc_categorization_end_to_end(db: Database) -> None:
    """Drives the real categorize_pending() entry point end-to-end.

    Loads raw Plaid data, runs the real SQLMesh transform (materializes
    prep.stg_plaid__transactions + the seeds.category_source_map /
    seeds.categories bridge), then calls categorize_pending() exactly as
    refresh_run/the post-import snowball would. Asserts on
    app.transaction_categories (the categorizer's own write) AND
    core.fct_transactions (the read surface CLI/MCP consumers use) so the
    whole chain is proven, not just the write.

    KNOWN FAILURE (found by this test, not yet fixed — see task report):
    apply_plaid_categories() (src/moneybin/services/categorization/applier.py)
    reads transaction_id from prep.stg_plaid__transactions — the PRE-MERGE
    staging view, keyed by the source-native Plaid transaction_id — and writes
    that native id straight into app.transaction_categories.transaction_id.
    Every other categorizer path (apply_rules/apply_merchant_categories via
    CategorizationMatcher.fetch_uncategorized_rows) and core.fct_transactions
    itself key off the GOLD merged transaction_id (a SHA-256 hash minted by
    prep.int_transactions__matched, ALWAYS different from the native id — see
    _gold_id() above). The result: apply_plaid_categories's write is real
    (categorize_pending()['plaid'] is nonzero, app.transaction_categories gets
    a row) but orphaned — core.fct_transactions's
    ``LEFT JOIN app.transaction_categories ON t.transaction_id = c.transaction_id``
    never matches it, so category/categorized_by never surface to
    core.fct_transactions, CLI, or MCP. The positive-case assertions below
    fail against current code for this reason; the negative-case assertions
    pass regardless (correctly not-categorizing doesn't depend on which id
    space is used).
    """
    sync_data = _build_sync_data()
    PlaidExtractor(db).load(sync_data, job_id=_JOB_ID)

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    detailed_id = _gold_id(db, "ACME GROCERY TEST DETAILED")
    fallback_id = _gold_id(db, "ACME TRANSIT TEST FALLBACK")
    low_confidence_id = _gold_id(db, "ACME COFFEE TEST LOW CONFIDENCE")
    unmapped_id = _gold_id(db, "ACME MYSTERY VENDOR TEST UNMAPPED")

    stats = CategorizationService(db).categorize_pending()
    assert stats["plaid"] == 2, (
        f"expected exactly the 2 confidently-mapped PFC rows to categorize, got {stats}"
    )

    # --- Positive: detailed code wins over a co-present mapped primary code ---
    row = db.execute(
        """
        SELECT category, subcategory, categorized_by
        FROM core.fct_transactions
        WHERE transaction_id = ?
        """,
        [detailed_id],
    ).fetchone()
    assert row == ("Food & Drink", "Groceries", "provider_native")

    row = db.execute(
        """
        SELECT categorized_by, source_type, confidence
        FROM app.transaction_categories
        WHERE transaction_id = ?
        """,
        [detailed_id],
    ).fetchone()
    assert row == ("provider_native", "plaid", Decimal("0.90"))

    # --- Positive: detailed code unmapped, primary code falls back ---
    row = db.execute(
        """
        SELECT category, subcategory, categorized_by
        FROM core.fct_transactions
        WHERE transaction_id = ?
        """,
        [fallback_id],
    ).fetchone()
    assert row == ("Transportation", None, "provider_native")

    row = db.execute(
        "SELECT confidence FROM app.transaction_categories WHERE transaction_id = ?",
        [fallback_id],
    ).fetchone()
    assert row == (Decimal("0.70"),), "MEDIUM (0.70) sits exactly at the >= gate"

    # --- Negative: bridge-mapped but below the confidence gate ---
    # categorized_by IS NULL is the precise "not categorized" signal here —
    # core.fct_transactions.category falls back to the raw Plaid-provided
    # category text (COALESCE(dc.category, c.category, t.category); see
    # fct_transactions.sql's column comment) whenever no MoneyBin category
    # has been assigned, so `category` legitimately shows "FOOD_AND_DRINK"
    # (the raw PFC primary code) even though the PFC categorizer correctly
    # declined to categorize this LOW-confidence row.
    row = db.execute(
        """
        SELECT category, categorized_by
        FROM core.fct_transactions
        WHERE transaction_id = ?
        """,
        [low_confidence_id],
    ).fetchone()
    assert row is not None
    assert row[0] == "FOOD_AND_DRINK", "raw Plaid category text still passes through"
    assert row[1] is None, (
        "LOW confidence must not categorize despite a valid bridge row"
    )
    assert (
        db.execute(
            "SELECT 1 FROM app.transaction_categories WHERE transaction_id = ?",
            [low_confidence_id],
        ).fetchone()
        is None
    )

    # --- Negative: no bridge row on either code, regardless of confidence ---
    row = db.execute(
        """
        SELECT category, categorized_by
        FROM core.fct_transactions
        WHERE transaction_id = ?
        """,
        [unmapped_id],
    ).fetchone()
    assert row is not None
    assert row[0] == "SOME_FUTURE_PRIMARY_CODE", (
        "raw Plaid category text still passes through"
    )
    assert row[1] is None, (
        "an unmapped PFC code must not categorize even at VERY_HIGH confidence"
    )
    assert (
        db.execute(
            "SELECT 1 FROM app.transaction_categories WHERE transaction_id = ?",
            [unmapped_id],
        ).fetchone()
        is None
    )
