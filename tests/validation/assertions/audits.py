"""Run a canonical SQLMesh audit and report it as a scenario assertion."""

from __future__ import annotations

from moneybin.audits.runner import run_standalone_audits
from moneybin.database import Database
from tests.validation.result import AssertionResult

_MAX_REPORTED_IDS = 20


def assert_transform_audit(db: Database, *, audit: str) -> AssertionResult:
    """Assert the standalone audit named ``audit`` finds no violations.

    The audit SQL under ``src/moneybin/sqlmesh/audits/`` is the one definition
    of the check; a scenario asserts on the same rows ``moneybin system
    doctor`` reports. Standalone audits are non-blocking in SQLMesh, so a
    scenario that does not assert on one never learns it fired.

    Raises ``KeyError`` when the project declares no such audit — the scenario
    runner turns that into a failed assertion rather than a silent pass.
    """
    outcome = run_standalone_audits(db, names=[audit])[0]
    return AssertionResult(
        name=outcome.name,
        passed=outcome.passed,
        details={
            "violations": len(outcome.violation_ids),
            "violation_ids": outcome.violation_ids[:_MAX_REPORTED_IDS],
        },
        error=outcome.error,
    )
