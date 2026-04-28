"""Unit tests for the per-record expectation verifier."""

from __future__ import annotations

from unittest.mock import MagicMock

from moneybin.testing.scenarios.expectations import (
    ExpectationResult,
    verify_expectations,
)
from moneybin.testing.scenarios.loader import ExpectationSpec


def test_match_decision_passes_when_pair_matched() -> None:
    """match_decision passes when all source txns resolve to one gold record."""
    db = MagicMock()
    # Both source rows resolve to the same gold record id.
    db.execute.return_value.fetchall.return_value = [("gold-1",)]
    spec = ExpectationSpec.model_validate({
        "kind": "match_decision",
        "description": "Chase OFX == Amazon CSV",
        "transactions": [
            {"source_transaction_id": "SYN20240315001", "source_type": "ofx"},
            {
                "source_transaction_id": "TBL_2024-03-15_AMZN_47.99",
                "source_type": "csv",
            },
        ],
        "expected": "matched",
        "expected_match_type": "same_record",
        "expected_confidence_min": 0.9,
    })
    results = verify_expectations(db, [spec])
    assert results[0].passed
    assert isinstance(results[0], ExpectationResult)
    assert results[0].kind == "match_decision"


def test_gold_record_count_fails_when_actual_differs() -> None:
    """gold_record_count fails when COUNT(*) differs from expected."""
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = (5,)
    spec = ExpectationSpec.model_validate({
        "kind": "gold_record_count",
        "description": "should collapse to 3",
        "expected_collapsed_count": 3,
    })
    [r] = verify_expectations(db, [spec])
    assert not r.passed
    assert r.details["actual"] == 5
    assert r.details["expected"] == 3


def test_category_for_transaction_passes_when_matches() -> None:
    """category_for_transaction passes when category and source both match."""
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = ("Groceries", "rule")
    spec = ExpectationSpec.model_validate({
        "kind": "category_for_transaction",
        "description": "Whole Foods is Groceries",
        "transaction_id": "txn-abc",
        "expected_category": "Groceries",
        "expected_categorized_by": "rule",
    })
    [r] = verify_expectations(db, [spec])
    assert r.passed
    assert r.details["actual"] == "Groceries"
    assert r.details["actual_source"] == "rule"


def test_category_for_transaction_fails_when_transaction_missing() -> None:
    """category_for_transaction fails (with reason) when txn id is not found."""
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    spec = ExpectationSpec.model_validate({
        "kind": "category_for_transaction",
        "transaction_id": "missing-txn",
        "expected_category": "Groceries",
    })
    [r] = verify_expectations(db, [spec])
    assert not r.passed
    assert r.details["reason"] == "transaction not found"


def test_provenance_for_transaction_passes_when_sources_match() -> None:
    """provenance_for_transaction passes when source rows equal expected (order-free)."""
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = [
        ("SYN20240315001", "ofx"),
        ("TBL_2024-03-15_AMZN_47.99", "csv"),
    ]
    spec = ExpectationSpec.model_validate({
        "kind": "provenance_for_transaction",
        "description": "Gold txn merges OFX + CSV",
        "transaction_id": "gold-1",
        "expected_sources": [
            {
                "source_transaction_id": "TBL_2024-03-15_AMZN_47.99",
                "source_type": "csv",
            },
            {"source_transaction_id": "SYN20240315001", "source_type": "ofx"},
        ],
    })
    [r] = verify_expectations(db, [spec])
    assert r.passed


def test_match_decision_fails_when_sources_resolve_to_different_records() -> None:
    """match_decision (expected=matched) fails when txns map to >1 gold record."""
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = [("gold-1",), ("gold-2",)]
    spec = ExpectationSpec.model_validate({
        "kind": "match_decision",
        "description": "should not match",
        "transactions": [
            {"source_transaction_id": "A", "source_type": "ofx"},
            {"source_transaction_id": "B", "source_type": "csv"},
        ],
        "expected": "matched",
    })
    [r] = verify_expectations(db, [spec])
    assert not r.passed
