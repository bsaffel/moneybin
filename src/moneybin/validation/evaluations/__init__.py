"""Evaluations — score pipeline output against synthetic.ground_truth."""

from __future__ import annotations

from moneybin.validation.evaluations._common import GroundTruthMissingError
from moneybin.validation.evaluations.categorization import score_categorization
from moneybin.validation.evaluations.matching import (
    score_dedup,
    score_transfer_detection,
)

__all__ = [
    "GroundTruthMissingError",
    "score_categorization",
    "score_dedup",
    "score_transfer_detection",
]
