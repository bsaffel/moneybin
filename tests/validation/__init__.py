"""Test-support primitives for the scenario runner: assertions, expectations, evaluations.

Not the home of MoneyBin's data-quality checks. A check on a ``core.*``
relation is defined once, as SQLMesh audit SQL under
``src/moneybin/sqlmesh/audits/``, and every surface reads that one definition:
``moneybin system doctor`` through ``moneybin.audits.runner``, and scenario
YAML through ``assert_transform_audit``. What lives here is the scaffolding a
scenario needs around those checks — parameterized shape and schema
assertions, per-record expectations, and scored evaluations against synthetic
ground truth — none of which a per-model audit can express.

Package layout:

- ``tests.validation.{AssertionResult, EvaluationResult, ExpectationResult}``
- ``tests.validation.assertions.{audits, schema, completeness, uniqueness, integrity, domain, distribution, infrastructure}``
- ``tests.validation.expectations.{matching, transactions}``
- ``tests.validation.evaluations.{categorization, matching}``

Every primitive takes ``Database`` as its first positional argument.
"""

from tests.validation.result import (
    AssertionResult,
    EvaluationResult,
    ExpectationResult,
)

__all__ = ["AssertionResult", "EvaluationResult", "ExpectationResult"]
