"""Expectation primitives — per-record predicates returning ExpectationResult."""

from moneybin.validation.expectations._types import SourceTransactionRef
from moneybin.validation.expectations.matching import (
    verify_match_decision,
    verify_transfers_match_ground_truth,
)
from moneybin.validation.expectations.transactions import (
    verify_category_for_transaction,
    verify_gold_record_count,
    verify_provenance_for_transaction,
)

__all__ = [
    "SourceTransactionRef",
    "verify_category_for_transaction",
    "verify_gold_record_count",
    "verify_match_decision",
    "verify_provenance_for_transaction",
    "verify_transfers_match_ground_truth",
]
