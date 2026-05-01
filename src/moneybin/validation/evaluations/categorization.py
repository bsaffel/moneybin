"""Categorization accuracy / per-category precision-recall."""

from __future__ import annotations

from collections import defaultdict

from moneybin.database import Database
from moneybin.tables import FCT_TRANSACTIONS, GROUND_TRUTH, INT_TRANSACTIONS_MATCHED
from moneybin.validation.evaluations._common import (
    GroundTruthMissingError,
    has_ground_truth,
    safe_div,
)
from moneybin.validation.result import EvaluationResult


def score_categorization(db: Database, *, threshold: float) -> EvaluationResult:
    """Score categorization accuracy of `core.fct_transactions.category` against `synthetic.ground_truth.expected_category`.

    The join bridges through `prep.int_transactions__matched`, which maps the
    synthetic `source_transaction_id` to the gold `transaction_id` produced
    by the dedup/matching pipeline.
    """
    if not has_ground_truth(db):
        raise GroundTruthMissingError("synthetic.ground_truth not present")

    rows = db.execute(
        f"""
        SELECT t.transaction_id, t.category AS predicted, gt.expected_category
        FROM {FCT_TRANSACTIONS.full_name} t
        JOIN {INT_TRANSACTIONS_MATCHED.full_name} m
          ON m.transaction_id = t.transaction_id
        JOIN {GROUND_TRUTH.full_name} gt
          ON gt.source_transaction_id = m.source_transaction_id
        WHERE gt.expected_category IS NOT NULL
        """  # noqa: S608 — TableRef constants
    ).fetchall()

    if not rows:
        return EvaluationResult(
            name="categorization_accuracy",
            metric="accuracy",
            value=0.0,
            threshold=threshold,
            passed=False,
            breakdown={"reason": "no labeled rows"},
        )

    correct = sum(1 for _, p, e in rows if p == e)
    accuracy = correct / len(rows)

    per_cat: dict[str, dict[str, int]] = defaultdict(
        lambda: {"tp": 0, "fp": 0, "fn": 0, "support": 0}
    )
    for _, predicted, expected in rows:
        per_cat[expected]["support"] += 1
        if predicted == expected:
            per_cat[expected]["tp"] += 1
        else:
            per_cat[expected]["fn"] += 1
            per_cat[predicted]["fp"] += 1

    # Skip phantom categories: predicted-only labels that never appear as
    # ground truth (support == 0) inflate the breakdown without adding signal.
    breakdown = {
        "per_category": {
            cat: {
                "precision": safe_div(s["tp"], s["tp"] + s["fp"]),
                "recall": safe_div(s["tp"], s["tp"] + s["fn"]),
                "support": s["support"],
            }
            for cat, s in per_cat.items()
            if s["support"] > 0
        },
        "total_labeled": len(rows),
    }

    return EvaluationResult(
        name="categorization_accuracy",
        metric="accuracy",
        value=round(accuracy, 4),
        threshold=threshold,
        passed=accuracy >= threshold,
        breakdown=breakdown,
    )
