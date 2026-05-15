"""Scenario: user-set category must survive the auto-rule/merchant engine."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.validation.result import AssertionResult
from tests.scenarios._runner import load_shipped_scenario, run_scenario

# Source identifiers from the fixture CSV (categorization-priority-hierarchy.csv).
# The gold transaction_id in fct_transactions is SHA256(source_type|source_id|account)[:16],
# which cannot be expressed as a readable YAML literal. Assertions are therefore
# written here as extra_assertions callbacks using source_transaction_id lookups
# through meta.fct_transaction_provenance.
_USER_OVERRIDE_SRC_ID = "USER_OVERRIDE_2024_03_01"
_AUTO_RULE_SRC_ID = "MCDONALDS_2024_03_05"


def _check_category(
    db: Database,
    source_transaction_id: str,
    expected_category: str,
    expected_categorized_by: str,
    description: str,
) -> AssertionResult:
    """Assert category + categorized_by for a transaction looked up by source_id."""
    row = db.execute(  # noqa: S608 — table name literal; value parameterized
        """
        SELECT t.category, t.categorized_by
        FROM core.fct_transactions AS t
        INNER JOIN meta.fct_transaction_provenance AS p
            ON t.transaction_id = p.transaction_id
        WHERE p.source_transaction_id = ?
        ORDER BY t.transaction_id
        LIMIT 1
        """,
        [source_transaction_id],
    ).fetchone()
    if not row:
        return AssertionResult(
            name=description,
            passed=False,
            details={
                "reason": "transaction not found",
                "source_id": source_transaction_id,
            },
        )
    actual_cat, actual_by = row
    passed = actual_cat == expected_category and actual_by == expected_categorized_by
    return AssertionResult(
        name=description,
        passed=passed,
        details={
            "expected_category": expected_category,
            "actual_category": actual_cat,
            "expected_categorized_by": expected_categorized_by,
            "actual_categorized_by": actual_by,
        },
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_categorization_priority_hierarchy() -> None:
    """User-categorized transaction stays unchanged after categorize_pending() runs.

    Ground truth (independently derived from fixture design):
    - USER_OVERRIDE_2024_03_01 is pre-seeded with categorized_by='user' via
      FixtureSpec.categories before transform. The categorize step skips it
      (queries WHERE c.transaction_id IS NULL).
    - MCDONALDS_2024_03_05 has no pre-seed; the basic persona's dining merchant
      catalog maps MCDONALDS (contains) → Food & Drink with categorized_by='rule',
      proving the engine ran but respected the user override.
    """
    scenario = load_shipped_scenario("categorization-priority-hierarchy")
    assert scenario is not None

    def extra(db: Database) -> list[AssertionResult]:
        return [
            _check_category(
                db,
                _USER_OVERRIDE_SRC_ID,
                expected_category="Food & Drink",
                expected_categorized_by="user",
                description="user-categorized transaction preserves categorized_by=user",
            ),
            _check_category(
                db,
                _AUTO_RULE_SRC_ID,
                expected_category="Food & Drink",
                expected_categorized_by="rule",
                description="non-overridden MCDONALDS gets categorized_by=rule (engine ran)",
            ),
        ]

    result = run_scenario(scenario, extra_assertions=extra)
    assert result.passed, result.failure_summary()
