"""Tests for evaluation primitives that score pipeline output against ground truth."""

from __future__ import annotations

from datetime import datetime

import pytest

from moneybin.database import Database
from moneybin.validation.evaluations import (
    GroundTruthMissingError,
    score_categorization,
    score_dedup,
    score_transfer_detection,
)


def _create_core_fct_transactions(db: Database) -> None:
    """Create a minimal core.fct_transactions table stub for evaluation tests."""
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS core.fct_transactions (
            transaction_id VARCHAR PRIMARY KEY,
            category VARCHAR,
            transfer_pair_id VARCHAR
        )
        """
    )


def _create_prep_matched_view(db: Database) -> None:
    """Create a prep.int_transactions__matched stub mapping source_transaction_id → transaction_id."""
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS prep.int_transactions__matched (
            transaction_id VARCHAR,
            source_transaction_id VARCHAR
        )
        """
    )


def _create_ground_truth(db: Database) -> None:
    """Create the synthetic.ground_truth table (mirrors src/moneybin/sql/schema/synthetic_ground_truth.sql)."""
    db.execute("CREATE SCHEMA IF NOT EXISTS synthetic")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS synthetic.ground_truth (
            source_transaction_id VARCHAR PRIMARY KEY,
            account_id VARCHAR NOT NULL,
            expected_category VARCHAR,
            transfer_pair_id VARCHAR,
            persona VARCHAR NOT NULL,
            seed INTEGER NOT NULL,
            generated_at TIMESTAMP NOT NULL
        )
        """
    )


def _seed_categorization(
    db: Database,
    *,
    rows: list[tuple[str, str, str]],
) -> None:
    """Seed fct_transactions, matched view, and ground_truth.

    Each tuple is (source_transaction_id, predicted_category, expected_category).
    The transaction_id is derived deterministically as 'gold_' + source_transaction_id.
    """
    _create_core_fct_transactions(db)
    _create_prep_matched_view(db)
    _create_ground_truth(db)

    now = datetime(2025, 1, 1, 0, 0, 0)
    for source_id, predicted, expected in rows:
        gold_id = f"gold_{source_id}"
        db.execute(
            "INSERT INTO core.fct_transactions VALUES (?, ?, NULL)",
            [gold_id, predicted],
        )
        db.execute(
            "INSERT INTO prep.int_transactions__matched VALUES (?, ?)",
            [gold_id, source_id],
        )
        db.execute(
            "INSERT INTO synthetic.ground_truth VALUES (?, ?, ?, NULL, ?, ?, ?)",
            [source_id, "acct1", expected, "test_persona", 42, now],
        )


def _seed_transfers(
    db: Database,
    *,
    true_pairs: list[tuple[str, str, str]],
    predicted_pairs: list[tuple[str, str, str]],
) -> None:
    """Seed transfer ground-truth pairs and predicted pairs.

    true_pairs: list of (source_id_a, source_id_b, transfer_pair_id) — labels.
    predicted_pairs: list of (source_id_a, source_id_b, transfer_pair_id) — what the
        pipeline predicted; each source_id is mapped to a gold transaction_id.
    """
    _create_core_fct_transactions(db)
    _create_prep_matched_view(db)
    _create_ground_truth(db)

    now = datetime(2025, 1, 1, 0, 0, 0)
    seen_sources: set[str] = set()
    for a, b, pair_id in true_pairs:
        for s in (a, b):
            if s not in seen_sources:
                db.execute(
                    "INSERT INTO synthetic.ground_truth VALUES (?, ?, NULL, ?, ?, ?, ?)",
                    [s, "acct1", pair_id, "test_persona", 42, now],
                )
                seen_sources.add(s)

    seen_gold: set[str] = set()
    for a, b, pair_id in predicted_pairs:
        for s in (a, b):
            gold_id = f"gold_{s}"
            if gold_id not in seen_gold:
                db.execute(
                    "INSERT INTO core.fct_transactions VALUES (?, NULL, ?)",
                    [gold_id, pair_id],
                )
                db.execute(
                    "INSERT INTO prep.int_transactions__matched VALUES (?, ?)",
                    [gold_id, s],
                )
                seen_gold.add(gold_id)


class TestCategorizationScoring:
    """Categorization accuracy scoring against ground-truth labels."""

    def test_accuracy_above_threshold_passes(self, db: Database) -> None:
        # 4 of 5 correct → 0.80 accuracy
        _seed_categorization(
            db,
            rows=[
                ("s1", "groceries", "groceries"),
                ("s2", "dining", "dining"),
                ("s3", "transport", "transport"),
                ("s4", "utilities", "utilities"),
                ("s5", "groceries", "dining"),  # wrong
            ],
        )
        r = score_categorization(db, threshold=0.75)
        assert r.passed
        assert r.metric == "accuracy"
        assert r.value == 0.8
        assert r.breakdown["total_labeled"] == 5
        assert "per_category" in r.breakdown

    def test_accuracy_below_threshold_fails(self, db: Database) -> None:
        _seed_categorization(
            db,
            rows=[
                ("s1", "groceries", "dining"),
                ("s2", "dining", "groceries"),
            ],
        )
        r = score_categorization(db, threshold=0.80)
        assert not r.passed
        assert r.value == 0.0

    def test_missing_ground_truth_raises(self, db: Database) -> None:
        # No synthetic.ground_truth table at all.
        with pytest.raises(GroundTruthMissingError):
            score_categorization(db, threshold=0.80)


class TestTransferDetection:
    """Transfer-pair F1 scoring against synthetic ground-truth pairs."""

    def test_perfect_f1(self, db: Database) -> None:
        _seed_transfers(
            db,
            true_pairs=[("a1", "a2", "T1"), ("b1", "b2", "T2")],
            predicted_pairs=[("a1", "a2", "P1"), ("b1", "b2", "P2")],
        )
        r = score_transfer_detection(db, threshold=0.85)
        assert r.passed
        assert r.metric == "f1"
        assert r.value == 1.0
        assert r.breakdown["true_pairs"] == 2
        assert r.breakdown["predicted_pairs"] == 2
        assert r.breakdown["tp"] == 2

    def test_f1_breakdown_with_partial_overlap(self, db: Database) -> None:
        # 2 true pairs, 2 predicted pairs, only 1 overlap.
        _seed_transfers(
            db,
            true_pairs=[("a1", "a2", "T1"), ("b1", "b2", "T2")],
            predicted_pairs=[
                ("a1", "a2", "P1"),  # tp
                ("c1", "c2", "P2"),  # fp (not in truth)
            ],
        )
        r = score_transfer_detection(db, threshold=0.85)
        assert not r.passed
        assert "true_pairs" in r.breakdown
        assert "predicted_pairs" in r.breakdown
        assert r.breakdown["tp"] == 1
        assert r.breakdown["fp"] == 1
        assert r.breakdown["fn"] == 1

    def test_missing_ground_truth_raises(self, db: Database) -> None:
        with pytest.raises(GroundTruthMissingError):
            score_transfer_detection(db, threshold=0.85)


class TestDedupScoring:
    """Dedup quality scoring based on collapsed gold-record count."""

    def test_perfect_match(self, db: Database) -> None:
        _create_core_fct_transactions(db)
        for i in range(10):
            db.execute(
                "INSERT INTO core.fct_transactions VALUES (?, NULL, NULL)",
                [f"gold_{i}"],
            )
        r = score_dedup(db, threshold=0.9, expected_collapsed_count=10)
        assert r.passed
        assert r.value == 1.0
        assert r.breakdown["actual_gold_records"] == 10
        assert r.breakdown["expected_collapsed_count"] == 10

    def test_off_by_some(self, db: Database) -> None:
        _create_core_fct_transactions(db)
        for i in range(8):
            db.execute(
                "INSERT INTO core.fct_transactions VALUES (?, NULL, NULL)",
                [f"gold_{i}"],
            )
        # actual=8, expected=10 → delta=2 → f1 = 1 - 0.2 = 0.8
        r = score_dedup(db, threshold=0.9, expected_collapsed_count=10)
        assert not r.passed
        assert r.value == 0.8
