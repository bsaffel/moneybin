"""Tests for the exemplar accumulator.

When the system creates a merchant from a categorized row, the row's normalized
match_text is stored as an exact exemplar in a oneOf set rather than as a
generalized contains pattern. Subsequent rows match via set-membership lookup.

Fixes bug 3 from categorization-matching-mechanics.md.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.categorization_service import (
    BulkCategorizationItem,
    CategorizationService,
)
from tests.moneybin.db_helpers import create_core_tables


@pytest.fixture()
def real_db(tmp_path: Path) -> Database:
    """Real DB with core + app schema (no SQLMesh)."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    db = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    create_core_tables(db)
    return db


def _seed_txn(
    db: Database, txn_id: str, description: str, memo: str | None = None
) -> None:
    db.execute(
        """
        INSERT INTO core.fct_transactions
        (transaction_id, account_id, transaction_date, amount,
         description, memo, source_type, is_transfer)
        VALUES (?, 'acct_test', '2026-05-10', -10.00, ?, ?, 'ofx', false)
        """,  # noqa: S608  # test input, not executing user SQL
        [txn_id, description, memo],
    )


def _items(
    *triples: tuple[str, str, str | None],
) -> list[BulkCategorizationItem]:
    return [
        BulkCategorizationItem(
            transaction_id=tid,
            category="Subscriptions",
            subcategory="Streaming",
            canonical_merchant_name=name,
        )
        for tid, _cat, name in triples
    ]


def test_first_categorization_creates_merchant_with_one_exemplar(
    real_db: Database,
) -> None:
    _seed_txn(real_db, "t1", "PAYPAL INST XFER", "GOOGLE YOUTUBE BRANDON SAFFEL")
    svc = CategorizationService(real_db)
    svc.bulk_categorize(_items(("t1", "Subscriptions", "YouTube")))
    row = real_db.execute(
        "SELECT canonical_name, raw_pattern, match_type, exemplars "
        "FROM app.user_merchants ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    canonical, raw_pattern, match_type, exemplars = row
    assert match_type == "oneOf"
    assert raw_pattern is None
    assert canonical == "YouTube"
    assert len(exemplars) == 1
    # The exemplar is the normalized match_text — concat of normalized
    # description + "\n" + normalized memo.
    assert "PAYPAL INST XFER" in exemplars[0]


def test_lookup_via_oneof_exemplar_match(real_db: Database) -> None:
    """A row whose match_text equals an existing merchant's exemplar matches via oneOf."""
    _seed_txn(real_db, "t1", "PAYPAL INST XFER", "GOOGLE YOUTUBE PREMIUM")
    svc = CategorizationService(real_db)
    svc.bulk_categorize(_items(("t1", "Subscriptions", "YouTube")))
    # New row with identical description+memo
    _seed_txn(real_db, "t2", "PAYPAL INST XFER", "GOOGLE YOUTUBE PREMIUM")
    # Run the deterministic matcher
    svc.categorize_pending()
    row = real_db.execute(
        "SELECT category, categorized_by FROM app.transaction_categories "
        "WHERE transaction_id = 't2'"
    ).fetchone()
    assert row is not None
    assert row[0] == "Subscriptions"
    assert row[1] == "rule"  # merchant fan-out writes 'rule' provenance


def test_aggregator_strings_do_not_overgeneralize(real_db: Database) -> None:
    """Two PayPal rows with different memos must not share a category (bug 3 regression check)."""
    _seed_txn(real_db, "t1", "PAYPAL INST XFER", "GOOGLE YOUTUBE PREMIUM")
    _seed_txn(real_db, "t2", "PAYPAL INST XFER", "ARBORIST CONSULTATION")
    svc = CategorizationService(real_db)
    # User categorizes the first as Subscriptions
    svc.bulk_categorize(_items(("t1", "Subscriptions", "YouTube")))
    # Run categorize_pending — t2 must NOT inherit Subscriptions because its
    # match_text differs (different memo)
    svc.categorize_pending()
    row = real_db.execute(
        "SELECT transaction_id FROM app.transaction_categories "
        "WHERE transaction_id = 't2'"
    ).fetchone()
    # t2 must remain uncategorized — the exemplar from t1 doesn't match t2's
    # match_text. THIS is the cure for bug 3.
    assert row is None


def test_second_categorization_appends_exemplar_to_existing_canonical(
    real_db: Database,
) -> None:
    """Repeat canonical_merchant_name appends to the existing merchant's exemplar set."""
    _seed_txn(real_db, "t1", "PAYPAL INST XFER", "GOOGLE YOUTUBE")
    _seed_txn(real_db, "t2", "PAYPAL INST XFER", "GOOGLE YOUTUBE PREMIUM")
    svc = CategorizationService(real_db)
    svc.bulk_categorize(_items(("t1", "Subscriptions", "YouTube")))
    svc.bulk_categorize(_items(("t2", "Subscriptions", "YouTube")))
    rows = real_db.execute(
        "SELECT canonical_name, exemplars FROM app.user_merchants "
        "WHERE canonical_name = 'YouTube'"
    ).fetchall()
    # One merchant with two exemplars (deduped if identical, distinct here).
    assert len(rows) == 1
    _canonical, exemplars = rows[0]
    assert len(exemplars) == 2
    assert all("PAYPAL INST XFER" in e for e in exemplars)


def test_append_exemplar_is_idempotent(real_db: Database) -> None:
    """Re-categorizing the same row with the same canonical name does not duplicate the exemplar."""
    _seed_txn(real_db, "t1", "PAYPAL INST XFER", "GOOGLE YOUTUBE")
    svc = CategorizationService(real_db)
    svc.bulk_categorize(_items(("t1", "Subscriptions", "YouTube")))
    svc.bulk_categorize(_items(("t1", "Subscriptions", "YouTube")))
    rows = real_db.execute(
        "SELECT exemplars FROM app.user_merchants WHERE canonical_name = 'YouTube'"
    ).fetchall()
    assert len(rows) == 1
    assert len(rows[0][0]) == 1  # deduped via list_distinct


def test_canonical_name_collision_with_different_category_does_not_merge(
    real_db: Database,
) -> None:
    """Two oneOf merchants can share a canonical name across categories.

    Without category/subcategory filtering, a second AMAZON row categorized
    as Shopping would append its exemplar to the existing Subscriptions
    AMAZON merchant — cross-polluting future matches. The lookup must
    co-filter by (canonical_name, category, subcategory) so each category
    gets its own merchant row and its own exemplar set.
    """
    _seed_txn(real_db, "t1", "AMZN GROCERY")
    _seed_txn(real_db, "t2", "AMZN PRIME VIDEO")
    svc = CategorizationService(real_db)

    svc.bulk_categorize([
        BulkCategorizationItem(
            transaction_id="t1",
            category="Shopping",
            subcategory="Online",
            canonical_merchant_name="Amazon",
        ),
    ])
    svc.bulk_categorize([
        BulkCategorizationItem(
            transaction_id="t2",
            category="Subscriptions",
            subcategory="Streaming",
            canonical_merchant_name="Amazon",
        ),
    ])

    rows = real_db.execute(
        "SELECT canonical_name, category, subcategory, exemplars "
        "FROM app.user_merchants WHERE canonical_name = 'Amazon' "
        "ORDER BY category"
    ).fetchall()
    # Two distinct Amazon merchants, one per category — not one merchant
    # with both exemplars merged.
    assert len(rows) == 2
    shopping = next(r for r in rows if r[1] == "Shopping")
    streaming = next(r for r in rows if r[1] == "Subscriptions")
    assert shopping[2] == "Online"
    assert streaming[2] == "Streaming"
    assert len(shopping[3]) == 1
    assert len(streaming[3]) == 1
    assert "AMZN GROCERY" in shopping[3][0]
    assert "AMZN PRIME VIDEO" in streaming[3][0]
