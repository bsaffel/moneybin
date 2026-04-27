"""Categorization accuracy / per-category precision-recall."""

from __future__ import annotations

from collections import defaultdict

from moneybin.database import Database
from moneybin.validation.evaluations import GroundTruthMissingError
from moneybin.validation.result import EvaluationResult


def score_categorization(db: Database, *, threshold: float) -> EvaluationResult:
    """Score categorization accuracy of `core.fct_transactions.category` against `synthetic.ground_truth.expected_category`.

    The join bridges through `prep.int_transactions__matched`, which maps the
    synthetic `source_transaction_id` to the gold `transaction_id` produced
    by the dedup/matching pipeline.
    """
    if not _has_ground_truth(db):
        raise GroundTruthMissingError("synthetic.ground_truth not present")

    rows = db.execute(
        """
        SELECT t.transaction_id, t.category AS predicted, gt.expected_category
        FROM core.fct_transactions t
        JOIN prep.int_transactions__matched m
          ON m.transaction_id = t.transaction_id
        JOIN synthetic.ground_truth gt
          ON gt.source_transaction_id = m.source_transaction_id
        WHERE gt.expected_category IS NOT NULL
        """
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

    breakdown = {
        "per_category": {
            cat: {
                "precision": _safe_div(s["tp"], s["tp"] + s["fp"]),
                "recall": _safe_div(s["tp"], s["tp"] + s["fn"]),
                "support": s["support"],
            }
            for cat, s in per_cat.items()
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


def _safe_div(num: int, denom: int) -> float:
    return round(num / denom, 4) if denom else 0.0


def _has_ground_truth(db: Database) -> bool:
    rows = db.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'synthetic' AND table_name = 'ground_truth'
        """
    ).fetchall()
    return bool(rows)
