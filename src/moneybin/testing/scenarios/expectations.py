"""Verifiers for per-record expectations declared in scenario YAML.

Each `ExpectationSpec.kind` maps to a verifier that queries the live database
and returns an `ExpectationResult`. Verifiers are intentionally thin — they
execute a single parameterized query and compare against the spec's expected
value. Used by the scenario runner (Task 10) and the dedup fixture (Task 12).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from moneybin.database import Database
from moneybin.testing.scenarios.loader import ExpectationSpec


@dataclass(frozen=True, slots=True)
class ExpectationResult:
    """Outcome of verifying a single `ExpectationSpec` against the database."""

    name: str
    kind: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)


def verify_expectations(
    db: Database, specs: list[ExpectationSpec]
) -> list[ExpectationResult]:
    """Run each expectation spec against `db` and return one result per spec."""
    return [_VERIFIERS[s.kind](db, s) for s in specs]


def _verify_match_decision(db: Database, spec: ExpectationSpec) -> ExpectationResult:
    """Verify that all listed source txns resolve to one (or distinct) gold rows."""
    body = spec.model_dump()
    txns = body["transactions"]
    expected_match_type = body.get("expected_match_type")
    confidence_floor = float(body.get("expected_confidence_min", 0.0))
    expected = body.get("expected", "matched")

    # Build a row-IN-(VALUES ...) clause with one (?,?) placeholder pair per txn.
    # The placeholder count is derived from a typed list, never from user-supplied
    # SQL — values themselves are bound via `?` parameters.
    placeholders = ",".join(["(?, ?)"] * len(txns))
    params: list[str] = []
    for t in txns:
        params.extend([t["source_transaction_id"], t["source_type"]])
    sql = f"""
        SELECT DISTINCT transaction_id
        FROM meta.fct_transaction_provenance
        WHERE (source_transaction_id, source_type) IN (VALUES {placeholders})
    """  # noqa: S608 — placeholders is a typed-list count, values are bound via ?
    rows = db.execute(sql, params).fetchall()

    if expected == "matched":
        passed = len(rows) == 1
    else:  # expected == "not_matched"
        passed = len(rows) >= 2

    # TODO: enforce expected_match_type and expected_confidence_min once we
    # surface match_type / confidence on the provenance row. For now these are
    # logged in details but not asserted.
    return ExpectationResult(
        name=spec.description or "match_decision",
        kind="match_decision",
        passed=passed,
        details={
            "expected": expected,
            "gold_record_ids": [r[0] for r in rows],
            "expected_match_type": expected_match_type,
            "confidence_floor": confidence_floor,
        },
    )


def _verify_gold_record_count(db: Database, spec: ExpectationSpec) -> ExpectationResult:
    """Verify gold record count, optionally scoped to fixture-derived source IDs.

    When ``fixture_source_ids`` is supplied, count only the distinct gold rows
    whose provenance includes one of those source IDs — letting dedup scenarios
    assert collapse counts on a known input set without depending on whole-table
    totals (which include unrelated synthetic data).
    """
    body = spec.model_dump()
    expected = int(body["expected_collapsed_count"])
    fixture_ids: list[str] = list(body.get("fixture_source_ids") or [])
    if fixture_ids:
        placeholders = ",".join(["?"] * len(fixture_ids))
        sql = f"""
            SELECT COUNT(DISTINCT transaction_id)
            FROM meta.fct_transaction_provenance
            WHERE source_transaction_id IN ({placeholders})
        """  # noqa: S608 — placeholders count derived from typed list; values bound
        row = db.execute(sql, fixture_ids).fetchone()
    else:
        row = db.execute("SELECT COUNT(*) FROM core.fct_transactions").fetchone()
    actual = int(row[0]) if row is not None else 0
    return ExpectationResult(
        name=spec.description or "gold_record_count",
        kind="gold_record_count",
        passed=actual == expected,
        details={"expected": expected, "actual": actual},
    )


def _verify_category_for_transaction(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    """Verify a transaction's category (and optionally its categorizer source)."""
    body = spec.model_dump()
    txn_id = body["transaction_id"]
    expected_category = body["expected_category"]
    expected_source = body.get("expected_categorized_by")
    row = db.execute(
        "SELECT category, categorized_by "
        "FROM core.fct_transactions "
        "WHERE transaction_id = ?",
        [txn_id],
    ).fetchone()
    if not row:
        return ExpectationResult(
            name=spec.description or "category_for_transaction",
            kind="category_for_transaction",
            passed=False,
            details={"reason": "transaction not found", "transaction_id": txn_id},
        )
    actual_cat, actual_src = row
    passed = actual_cat == expected_category and (
        expected_source is None or actual_src == expected_source
    )
    return ExpectationResult(
        name=spec.description or "category_for_transaction",
        kind="category_for_transaction",
        passed=passed,
        details={
            "expected": expected_category,
            "actual": actual_cat,
            "expected_source": expected_source,
            "actual_source": actual_src,
        },
    )


def _verify_provenance_for_transaction(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    """Verify the provenance source rows for a gold transaction match expected."""
    body = spec.model_dump()
    txn_id = body["transaction_id"]
    expected_sources = sorted(
        (s["source_transaction_id"], s["source_type"]) for s in body["expected_sources"]
    )
    rows = sorted(
        db.execute(
            "SELECT source_transaction_id, source_type "
            "FROM meta.fct_transaction_provenance "
            "WHERE transaction_id = ?",
            [txn_id],
        ).fetchall()
    )
    return ExpectationResult(
        name=spec.description or "provenance_for_transaction",
        kind="provenance_for_transaction",
        passed=rows == expected_sources,
        details={"expected": expected_sources, "actual": rows},
    )


_VERIFIERS: dict[str, Callable[[Database, ExpectationSpec], ExpectationResult]] = {
    "match_decision": _verify_match_decision,
    "gold_record_count": _verify_gold_record_count,
    "category_for_transaction": _verify_category_for_transaction,
    "provenance_for_transaction": _verify_provenance_for_transaction,
}
