"""Unit tests for the per-record expectation verifier."""

from __future__ import annotations

from unittest.mock import MagicMock

from moneybin.testing.scenarios.expectations import (
    ExpectationResult,
    verify_expectations,
)
from moneybin.testing.scenarios.loader import ExpectationSpec


def _mock_db(
    *,
    provenance_rows: list[tuple[str, str, str, str]],
    confidence: float | None = None,
) -> MagicMock:
    """Build a MagicMock Database that returns provenance rows then a confidence row.

    The match_decision verifier issues two queries on the matched path:
    a join against provenance + match_decisions, then a single
    match_confidence lookup. ``side_effect`` returns a fresh cursor mock
    per call so each ``db.execute(...)`` gets its own ``fetchall`` /
    ``fetchone`` payload.
    """
    db = MagicMock()
    cursors: list[MagicMock] = []
    join_cursor = MagicMock()
    join_cursor.fetchall.return_value = provenance_rows
    cursors.append(join_cursor)
    if confidence is not None:
        conf_cursor = MagicMock()
        conf_cursor.fetchone.return_value = (confidence,)
        cursors.append(conf_cursor)
    db.execute.side_effect = cursors
    return db


def test_match_decision_passes_when_pair_matched() -> None:
    """match_decision passes when sources collapse to one gold row above the floor."""
    db = _mock_db(
        provenance_rows=[
            ("SYN20240315001", "ofx", "gold-1", "dedup"),
            ("TBL_2024-03-15_AMZN_47.99", "csv", "gold-1", "dedup"),
        ],
        confidence=0.95,
    )
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
        "expected_match_type": "dedup",
        "expected_confidence_min": 0.9,
    })
    results = verify_expectations(db, [spec])
    assert results[0].passed, results[0].details
    assert isinstance(results[0], ExpectationResult)
    assert results[0].kind == "match_decision"


def test_match_decision_fails_when_confidence_below_floor() -> None:
    """match_decision fails when collapse is correct but confidence is too low."""
    db = _mock_db(
        provenance_rows=[
            ("A", "ofx", "gold-1", "dedup"),
            ("B", "csv", "gold-1", "dedup"),
        ],
        confidence=0.5,
    )
    spec = ExpectationSpec.model_validate({
        "kind": "match_decision",
        "transactions": [
            {"source_transaction_id": "A", "source_type": "ofx"},
            {"source_transaction_id": "B", "source_type": "csv"},
        ],
        "expected": "matched",
        "expected_confidence_min": 0.9,
    })
    [r] = verify_expectations(db, [spec])
    assert not r.passed
    assert r.details["actual_confidence"] == 0.5


def test_match_decision_fails_when_source_missing_from_provenance() -> None:
    """match_decision fails when a listed source isn't in provenance at all."""
    db = _mock_db(
        provenance_rows=[("A", "ofx", "gold-1", "dedup")],
        confidence=0.95,
    )
    spec = ExpectationSpec.model_validate({
        "kind": "match_decision",
        "transactions": [
            {"source_transaction_id": "A", "source_type": "ofx"},
            {"source_transaction_id": "B", "source_type": "csv"},
        ],
        "expected": "matched",
    })
    [r] = verify_expectations(db, [spec])
    assert not r.passed
    assert ["B", "csv"] in r.details["missing_sources"]


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


def test_transfers_match_ground_truth_passes_when_pairs_align() -> None:
    """All labeled pairs map to a single non-null predicted_pair → passes."""
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = [
        ("gold-pair-1", "src-A", "txn-1", "pred-1"),
        ("gold-pair-1", "src-B", "txn-2", "pred-1"),
        ("gold-pair-2", "src-C", "txn-3", "pred-2"),
        ("gold-pair-2", "src-D", "txn-4", "pred-2"),
    ]
    spec = ExpectationSpec.model_validate({
        "kind": "transfers_match_ground_truth",
        "description": "all labeled pairs detected",
    })
    [r] = verify_expectations(db, [spec])
    assert r.passed
    assert r.details["labeled_pair_count"] == 2
    assert r.details["failure_count"] == 0


def test_transfers_match_ground_truth_fails_when_leg_predicts_null() -> None:
    """One leg with NULL predicted_pair fails the pair (matcher missed it)."""
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = [
        ("gold-pair-1", "src-A", "txn-1", "pred-1"),
        ("gold-pair-1", "src-B", "txn-2", None),
    ]
    spec = ExpectationSpec.model_validate({
        "kind": "transfers_match_ground_truth",
    })
    [r] = verify_expectations(db, [spec])
    assert not r.passed
    assert r.details["failure_count"] == 1


def test_transfers_match_ground_truth_fails_when_legs_split_pairs() -> None:
    """Two legs predicted into different pair_ids fails (split detection)."""
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = [
        ("gold-pair-1", "src-A", "txn-1", "pred-1"),
        ("gold-pair-1", "src-B", "txn-2", "pred-2"),
    ]
    spec = ExpectationSpec.model_validate({
        "kind": "transfers_match_ground_truth",
    })
    [r] = verify_expectations(db, [spec])
    assert not r.passed
    assert r.details["failure_count"] == 1


def test_match_decision_fails_when_sources_resolve_to_different_records() -> None:
    """match_decision (expected=matched) fails when txns map to >1 gold record."""
    db = _mock_db(
        provenance_rows=[
            ("A", "ofx", "gold-1", "dedup"),
            ("B", "csv", "gold-2", "dedup"),
        ],
    )
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
