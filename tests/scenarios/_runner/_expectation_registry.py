"""YAML-driven adapter for expectation predicates.

The library predicates in moneybin.validation.expectations take typed kwargs.
Scenario YAML provides loosely-typed dicts via ExpectationSpec. This module is
the only place that translates between the two.

Adding a new ExpectationSpec.kind:
1. Add a Literal to loader.ExpectationSpec.kind.
2. Implement the predicate in moneybin.validation.expectations.
3. Register an adapter here.
"""

from __future__ import annotations

from collections.abc import Callable

from moneybin.database import Database
from moneybin.validation.expectations import (
    SourceTransactionRef,
    verify_category_for_transaction,
    verify_gold_record_count,
    verify_match_decision,
    verify_provenance_for_transaction,
    verify_transfers_match_ground_truth,
)
from moneybin.validation.result import ExpectationResult
from tests.scenarios._runner.loader import ExpectationSpec

ExpectationAdapter = Callable[[Database, ExpectationSpec], ExpectationResult]


def _adapt_match_decision(db: Database, spec: ExpectationSpec) -> ExpectationResult:
    body = spec.model_dump()
    transactions_raw = body.get("transactions")
    if not transactions_raw:
        raise ValueError("match_decision spec requires a non-empty 'transactions' list")
    return verify_match_decision(
        db,
        transactions=[SourceTransactionRef(**t) for t in transactions_raw],
        expected=body.get("expected", "matched"),
        expected_match_type=body.get("expected_match_type"),
        expected_confidence_min=float(body.get("expected_confidence_min", 0.0)),
        description=spec.description,
    )


def _adapt_gold_record_count(db: Database, spec: ExpectationSpec) -> ExpectationResult:
    body = spec.model_dump()
    return verify_gold_record_count(
        db,
        expected_collapsed_count=int(body["expected_collapsed_count"]),
        fixture_source_ids=body.get("fixture_source_ids"),
        description=spec.description,
    )


def _adapt_category_for_transaction(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    body = spec.model_dump()
    return verify_category_for_transaction(
        db,
        transaction_id=body["transaction_id"],
        expected_category=body["expected_category"],
        expected_categorized_by=body.get("expected_categorized_by"),
        description=spec.description,
    )


def _adapt_provenance_for_transaction(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    body = spec.model_dump()
    expected_sources_raw = body.get("expected_sources")
    if not expected_sources_raw:
        raise ValueError(
            "provenance_for_transaction spec requires a non-empty 'expected_sources' list"
        )
    return verify_provenance_for_transaction(
        db,
        transaction_id=body["transaction_id"],
        expected_sources=[SourceTransactionRef(**s) for s in expected_sources_raw],
        description=spec.description,
    )


def _adapt_transfers_match_ground_truth(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    return verify_transfers_match_ground_truth(db, description=spec.description)


EXPECTATION_REGISTRY: dict[str, ExpectationAdapter] = {
    "match_decision": _adapt_match_decision,
    "gold_record_count": _adapt_gold_record_count,
    "category_for_transaction": _adapt_category_for_transaction,
    "provenance_for_transaction": _adapt_provenance_for_transaction,
    "transfers_match_ground_truth": _adapt_transfers_match_ground_truth,
}


def resolve_expectation(kind: str) -> ExpectationAdapter:
    """Return the adapter registered under ``kind`` or raise KeyError."""
    if kind not in EXPECTATION_REGISTRY:
        raise KeyError(f"unknown expectation kind: {kind!r}")
    return EXPECTATION_REGISTRY[kind]


def verify_expectations(
    db: Database, specs: list[ExpectationSpec]
) -> list[ExpectationResult]:
    """Dispatch each spec through its registered adapter and return results."""
    return [resolve_expectation(s.kind)(db, s) for s in specs]


__all__ = [
    "EXPECTATION_REGISTRY",
    "ExpectationAdapter",
    "resolve_expectation",
    "verify_expectations",
]
