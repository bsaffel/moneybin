"""Evaluations — score pipeline output against synthetic.ground_truth."""

from __future__ import annotations


class GroundTruthMissingError(RuntimeError):
    """Raised when an evaluation runs against a DB without `synthetic.ground_truth`."""


from moneybin.validation.evaluations.categorization import (  # noqa: E402  # circular import — error class must be defined before submodule import
    score_categorization,
)
from moneybin.validation.evaluations.matching import (  # noqa: E402  # circular import — error class must be defined before submodule import
    score_dedup,
    score_transfer_detection,
)

__all__ = [
    "GroundTruthMissingError",
    "score_categorization",
    "score_dedup",
    "score_transfer_detection",
]
