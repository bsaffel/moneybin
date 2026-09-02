"""Evaluations — score pipeline output against synthetic.ground_truth."""

from __future__ import annotations

from tests.validation.evaluations._common import GroundTruthMissingError
from tests.validation.evaluations.categorization import score_categorization
from tests.validation.evaluations.matching import (
    score_dedup,
    score_transfer_detection,
)

__all__ = [
    "GroundTruthMissingError",
    "score_categorization",
    "score_dedup",
    "score_transfer_detection",
]
