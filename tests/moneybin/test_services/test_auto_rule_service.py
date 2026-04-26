"""Unit tests for auto_rule_service."""

from unittest.mock import MagicMock

from moneybin.services import auto_rule_service


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
