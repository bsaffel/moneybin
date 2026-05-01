"""Expectation primitives — per-record predicates returning ExpectationResult."""

from moneybin.validation.expectations._types import SourceTransactionRef
from moneybin.validation.expectations.matching import (
    verify_match_decision,
    verify_transfers_match_ground_truth,
)

__all__ = [
    "SourceTransactionRef",
    "verify_match_decision",
    "verify_transfers_match_ground_truth",
]
