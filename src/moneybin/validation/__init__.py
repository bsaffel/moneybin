"""Validation primitives reusable across synthetic scenario runs and live data verification.

Public stable contract (consumed by data-reconciliation):

- ``moneybin.validation.{AssertionResult, EvaluationResult, ExpectationResult}``
- ``moneybin.validation.assertions.{schema, completeness, uniqueness, integrity, domain, distribution, infrastructure}``
- ``moneybin.validation.expectations.{matching, transactions}``
- ``moneybin.validation.evaluations.{categorization, matching}``

Stability rules: additive kwargs OK; rename/remove requires deprecation alias for one
release. ``details``/``breakdown`` dicts are per-function, not cross-function contract.
"""

from moneybin.validation.result import (
    AssertionResult,
    EvaluationResult,
    ExpectationResult,
)

__all__ = ["AssertionResult", "EvaluationResult", "ExpectationResult"]
