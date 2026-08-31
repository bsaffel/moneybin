"""Tests for the categorization-link maintenance backfill."""

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.services.categorization import CategorizationService
from scripts.backfill_categorization_links import backfill
from tests.moneybin.db_helpers import create_core_tables


def test_backfill_routes_link_updates_through_audited_repository(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_core_tables(db)
    db.execute(
        """
        INSERT INTO core.fct_transactions
            (transaction_id, account_id, amount, description)
        VALUES
            ('txn-existing-rule', 'account-1', -5.00, 'Existing Merchant'),
            ('txn-new-rule', 'account-1', -7.00, 'Coffee Shop')
        """
    )
    db.execute(
        """
        INSERT INTO app.transaction_categories
            (transaction_id, category, subcategory, category_id,
             categorized_by, confidence, merchant_id, rule_id)
        VALUES
            ('txn-existing-rule', 'Dining', 'Lunch', 'category-1',
             'user', 0.90, NULL, 'existing-rule'),
            ('txn-new-rule', 'Dining', 'Coffee', 'category-2',
             'rule', 0.80, NULL, NULL)
        """
    )
    db.execute(
        """
        INSERT INTO app.categorization_rules
            (rule_id, name, merchant_pattern, match_type, category, subcategory)
        VALUES
            ('new-rule', 'Coffee', 'coffee', 'contains', 'Dining', 'Coffee')
        """
    )

    merchants: dict[str, dict[str, str | None]] = {
        "Existing Merchant": {"merchant_id": "merchant-existing"},
        "Coffee Shop": {"merchant_id": "merchant-new"},
    }

    def match_merchant(
        _service: CategorizationService,
        description: str,
        memo: str | None = None,
    ) -> dict[str, str | None] | None:
        del memo
        return merchants.get(description)

    monkeypatch.setattr(
        CategorizationService,
        "match_merchant",
        match_merchant,
    )

    assert backfill(db) == {"merchant_ids": 2, "rule_ids": 1}

    rows = db.execute(
        """
        SELECT transaction_id, category, subcategory, category_id,
               categorized_by, confidence, merchant_id, rule_id
        FROM app.transaction_categories
        ORDER BY transaction_id
        """
    ).fetchall()
    assert rows == [
        (
            "txn-existing-rule",
            "Dining",
            "Lunch",
            "category-1",
            "user",
            Decimal("0.90"),
            "merchant-existing",
            "existing-rule",
        ),
        (
            "txn-new-rule",
            "Dining",
            "Coffee",
            "category-2",
            "rule",
            Decimal("0.80"),
            "merchant-new",
            "new-rule",
        ),
    ]
    audit_counts = db.execute(
        """
        SELECT target_id, COUNT(*)
        FROM app.audit_log
        WHERE target_table = 'transaction_categories'
        GROUP BY target_id
        ORDER BY target_id
        """
    ).fetchall()
    assert audit_counts == [("txn-existing-rule", 1), ("txn-new-rule", 2)]
