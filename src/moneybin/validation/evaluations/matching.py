"""Transfer detection + dedup F1 scoring."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.validation.evaluations._common import (
    GroundTruthMissingError,
    has_ground_truth,
)
from moneybin.validation.result import EvaluationResult


def score_transfer_detection(db: Database, *, threshold: float) -> EvaluationResult:
    """Score transfer-detection F1 of `core.fct_transactions.transfer_pair_id`.

    Both true and predicted pairs are normalized into 2-tuples of
    `source_transaction_id` (sorted MIN/MAX) so they share an identifier
    space. Predicted pairs are mapped from gold `transaction_id` to
    `source_transaction_id` via `prep.int_transactions__matched`.

    Note: the `HAVING COUNT(*) = 2` filter on both sides intentionally
    excludes 3+-leg transfer chains (e.g., A → B → C funneled through a
    shared `transfer_pair_id`) from both true and predicted sets. This is
    acceptable for v1 since the synthetic dataset only models 2-leg
    transfers; a future enhancement could lift this constraint and score
    higher-arity chains via set-of-sets matching.
    """
    if not has_ground_truth(db):
        raise GroundTruthMissingError("synthetic.ground_truth required")

    true_pairs = _pair_set(
        db,
        """
        SELECT MIN(source_transaction_id), MAX(source_transaction_id)
        FROM synthetic.ground_truth
        WHERE transfer_pair_id IS NOT NULL
        GROUP BY transfer_pair_id
        HAVING COUNT(*) = 2
        """,
    )
    predicted_pairs = _pair_set(
        db,
        """
        WITH predicted AS (
            SELECT t.transfer_pair_id, m.source_transaction_id
            FROM core.fct_transactions t
            JOIN prep.int_transactions__matched m
              ON m.transaction_id = t.transaction_id
            WHERE t.transfer_pair_id IS NOT NULL
        )
        SELECT MIN(source_transaction_id), MAX(source_transaction_id)
        FROM predicted
        GROUP BY transfer_pair_id
        HAVING COUNT(*) = 2
        """,
    )

    tp = len(true_pairs & predicted_pairs)
    fp = len(predicted_pairs - true_pairs)
    fn = len(true_pairs - predicted_pairs)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return EvaluationResult(
        name="transfer_f1",
        metric="f1",
        value=round(f1, 4),
        threshold=threshold,
        passed=f1 >= threshold,
        breakdown={
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "true_pairs": len(true_pairs),
            "predicted_pairs": len(predicted_pairs),
            "tp": tp,
            "fp": fp,
            "fn": fn,
        },
    )


def score_dedup(
    db: Database, *, threshold: float, expected_collapsed_count: int
) -> EvaluationResult:
    """Compare actual gold-record count against the expected collapsed count.

    `expected_collapsed_count` comes from the labeled-overlap fixture metadata —
    the known number of gold records that should remain after dedup collapses
    cross-source duplicates. The score is `1 - |delta|/expected`, clamped at 0.
    """
    actual_row = db.execute("SELECT COUNT(*) FROM core.fct_transactions").fetchone()
    actual = actual_row[0] if actual_row is not None else 0
    delta = abs(actual - expected_collapsed_count)
    score = max(0.0, 1.0 - delta / max(expected_collapsed_count, 1))
    return EvaluationResult(
        name="dedup_quality",
        metric="dedup_score",
        value=round(score, 4),
        threshold=threshold,
        passed=score >= threshold,
        breakdown={
            "actual_gold_records": actual,
            "expected_collapsed_count": expected_collapsed_count,
        },
    )


def _pair_set(db: Database, sql: str) -> set[tuple[str, str]]:
    return {(a, b) for a, b in db.execute(sql).fetchall()}
