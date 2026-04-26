"""Unit tests for auto_rule_service."""

from unittest.mock import MagicMock

import pytest

from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database
from moneybin.services import auto_rule_service
from tests.moneybin.db_helpers import create_core_tables


def _mock_db_with_merchant(
    merchant_id: str = "m_abc", canonical_name: str = "STARBUCKS"
):
    db = MagicMock()
    # transaction_categories row -> merchant_id
    db.execute.return_value.fetchone.side_effect = [
        (merchant_id,),  # SELECT merchant_id FROM transaction_categories
        (canonical_name,),  # SELECT canonical_name FROM merchants
    ]
    return db


def test_extract_pattern_uses_merchant_canonical_name_when_present():
    """Extract pattern prefers merchant canonical name when present."""
    db = _mock_db_with_merchant()
    pattern = auto_rule_service.extract_pattern(db, transaction_id="t_1")
    assert pattern == "STARBUCKS"


def test_extract_pattern_falls_back_to_normalized_description():
    """Extract pattern falls back to normalized description when no merchant_id."""
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [
        (None,),  # no merchant_id on the categorization row
        ("SQ *STARBUCKS #1234 SEATTLE WA",),  # raw description
    ]
    pattern = auto_rule_service.extract_pattern(db, transaction_id="t_2")
    assert pattern == "STARBUCKS"


def test_extract_pattern_returns_none_when_description_empty():
    """Extract pattern returns None when description is empty."""
    db = MagicMock()
    db.execute.return_value.fetchone.side_effect = [(None,), ("",)]
    assert auto_rule_service.extract_pattern(db, transaction_id="t_3") is None


@pytest.fixture
def real_db(tmp_path):
    """A real DB with schema initialized."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    db = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    create_core_tables(db)
    yield db
    db.close()


def _seed_transaction(
    db: Database,
    txn_id: str,
    description: str = "STARBUCKS",
    merchant_id: str | None = None,
) -> None:
    db.execute(
        "INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date, amount, description, source_type) "
        "VALUES (?, 'a1', DATE '2026-01-01', -5.00, ?, 'csv')",
        [txn_id, description],
    )
    db.execute(
        "INSERT INTO app.transaction_categories (transaction_id, category, categorized_at, categorized_by, merchant_id) "
        "VALUES (?, 'Food & Drink', CURRENT_TIMESTAMP, 'user', ?)",
        [txn_id, merchant_id],
    )


def test_record_creates_proposal_on_first_categorization(real_db):
    """Creating a proposal on the first categorization stores the expected row."""
    _seed_transaction(real_db, "t1")
    auto_rule_service.record_categorization(
        real_db, "t1", "Food & Drink", subcategory="Coffee"
    )

    rows = real_db.execute(
        "SELECT merchant_pattern, category, subcategory, trigger_count, status FROM app.proposed_rules"
    ).fetchall()
    assert rows == [("STARBUCKS", "Food & Drink", "Coffee", 1, "pending")]


def test_record_increments_trigger_count_on_same_pattern_and_category(real_db):
    """Repeated categorizations with the same pattern and category increment trigger_count."""
    _seed_transaction(real_db, "t1")
    _seed_transaction(real_db, "t2")
    auto_rule_service.record_categorization(
        real_db, "t1", "Food & Drink", subcategory="Coffee"
    )
    auto_rule_service.record_categorization(
        real_db, "t2", "Food & Drink", subcategory="Coffee"
    )

    rows = real_db.execute(
        "SELECT trigger_count, sample_txn_ids FROM app.proposed_rules"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 2
    assert sorted(rows[0][1]) == ["t1", "t2"]


def test_record_supersedes_when_same_pattern_different_category(real_db):
    """Categorizing a same-pattern txn with a different category supersedes the prior proposal."""
    _seed_transaction(real_db, "t1")
    _seed_transaction(real_db, "t2")
    auto_rule_service.record_categorization(real_db, "t1", "Food & Drink")
    auto_rule_service.record_categorization(real_db, "t2", "Groceries")

    rows = real_db.execute(
        "SELECT category, status FROM app.proposed_rules ORDER BY proposed_at"
    ).fetchall()
    assert rows == [("Food & Drink", "superseded"), ("Groceries", "pending")]


def test_record_skips_when_active_rule_already_covers_pattern(real_db):
    """No proposal is created when an active rule already covers the merchant pattern."""
    _seed_transaction(real_db, "t1")
    real_db.execute(
        "INSERT INTO app.categorization_rules (rule_id, name, merchant_pattern, match_type, category, priority, is_active) "
        "VALUES ('r1', 'starbucks', 'STARBUCKS', 'contains', 'Food & Drink', 100, true)"
    )
    auto_rule_service.record_categorization(real_db, "t1", "Food & Drink")

    count = real_db.execute("SELECT COUNT(*) FROM app.proposed_rules").fetchone()[0]
    assert count == 0


def test_record_respects_proposal_threshold(real_db, monkeypatch):
    """Proposals stay in 'tracking' status until trigger_count reaches the configured threshold."""
    monkeypatch.setenv("MONEYBIN_CATEGORIZATION__AUTO_RULE_PROPOSAL_THRESHOLD", "3")
    clear_settings_cache()
    set_current_profile("test")

    _seed_transaction(real_db, "t1")
    _seed_transaction(real_db, "t2")
    _seed_transaction(real_db, "t3")
    auto_rule_service.record_categorization(real_db, "t1", "Food & Drink")
    auto_rule_service.record_categorization(real_db, "t2", "Food & Drink")
    pending = real_db.execute(
        "SELECT COUNT(*) FROM app.proposed_rules WHERE status = 'pending'"
    ).fetchone()[0]
    assert pending == 0

    auto_rule_service.record_categorization(real_db, "t3", "Food & Drink")
    pending = real_db.execute(
        "SELECT COUNT(*) FROM app.proposed_rules WHERE status = 'pending'"
    ).fetchone()[0]
    assert pending == 1
