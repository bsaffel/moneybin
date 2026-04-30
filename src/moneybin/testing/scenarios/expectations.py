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
from moneybin.tables import (
    FCT_TRANSACTION_PROVENANCE,
    FCT_TRANSACTIONS,
    GROUND_TRUTH,
    MATCH_DECISIONS,
)
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
    """Verify that all listed source txns resolve to one (or distinct) gold rows.

    Strict matched-branch semantics — all four conditions are gating:

    1. **Coverage**: every listed (source_transaction_id, source_type) is
       present in ``meta.fct_transaction_provenance``.
    2. **Collapse**: those provenance rows all point to exactly one gold
       ``transaction_id`` in ``core.fct_transactions``.
    3. **Confidence**: the gold row's ``match_confidence`` meets or
       exceeds ``expected_confidence_min``.
    4. **Match type**: when ``expected_match_type`` is set, the
       provenance row's underlying ``app.match_decisions.match_type``
       must equal it. Currently only ``dedup`` is reachable through this
       path (the provenance view filters to dedup), but the validation
       guards against future widening of that filter.

    For ``expected: not_matched`` the criterion is simpler: the listed
    sources resolve to two or more distinct gold rows.
    """
    body = spec.model_dump()
    txns = body["transactions"]
    expected_match_type = body.get("expected_match_type")
    confidence_floor = float(body.get("expected_confidence_min", 0.0))
    expected = body.get("expected", "matched")

    if expected_match_type is not None and expected_match_type not in {
        "dedup",
        "transfer",
    }:
        return ExpectationResult(
            name=spec.description or "match_decision",
            kind="match_decision",
            passed=False,
            details={"reason": f"unknown expected_match_type: {expected_match_type!r}"},
        )

    # Row-IN-(VALUES ...) clause: placeholder count derived from a typed list,
    # values bound via `?`.
    placeholders = ",".join(["(?, ?)"] * len(txns))
    params: list[str] = []
    for t in txns:
        params.extend([t["source_transaction_id"], t["source_type"]])
    sql = f"""
        SELECT
          p.source_transaction_id,
          p.source_type,
          p.transaction_id,
          md.match_type
        FROM {FCT_TRANSACTION_PROVENANCE.full_name} AS p
        LEFT JOIN {MATCH_DECISIONS.full_name} AS md ON md.match_id = p.match_id
        WHERE (p.source_transaction_id, p.source_type) IN (VALUES {placeholders})
    """  # noqa: S608 — placeholders is a typed-list count, values are bound via ?
    rows = db.execute(sql, params).fetchall()

    found_pairs = {(r[0], r[1]) for r in rows}
    expected_pairs = {(t["source_transaction_id"], t["source_type"]) for t in txns}
    missing = sorted(expected_pairs - found_pairs)
    gold_ids = sorted({r[2] for r in rows})
    match_types = {r[3] for r in rows if r[3] is not None}

    details: dict[str, Any] = {
        "expected": expected,
        "gold_record_ids": gold_ids,
        "missing_sources": [list(p) for p in missing],
        "match_types": sorted(match_types),
        "expected_match_type": expected_match_type,
        "confidence_floor": confidence_floor,
    }

    if expected != "matched":
        # not_matched: listed sources must resolve to ≥2 distinct gold rows.
        # Coverage isn't required — a missing source is itself "not matched".
        return ExpectationResult(
            name=spec.description or "match_decision",
            kind="match_decision",
            passed=len(gold_ids) >= 2,
            details=details,
        )

    if missing or len(gold_ids) != 1:
        return ExpectationResult(
            name=spec.description or "match_decision",
            kind="match_decision",
            passed=False,
            details=details,
        )

    gold_id = gold_ids[0]
    confidence_row = db.execute(
        f"SELECT match_confidence FROM {FCT_TRANSACTIONS.full_name} "  # noqa: S608 — TableRef constant, no user input
        "WHERE transaction_id = ?",
        [gold_id],
    ).fetchone()
    actual_confidence = (
        float(confidence_row[0])
        if confidence_row is not None and confidence_row[0] is not None
        else 0.0
    )
    details["actual_confidence"] = actual_confidence

    confidence_ok = actual_confidence >= confidence_floor
    type_ok = expected_match_type is None or match_types == {expected_match_type}

    return ExpectationResult(
        name=spec.description or "match_decision",
        kind="match_decision",
        passed=confidence_ok and type_ok,
        details=details,
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
            FROM {FCT_TRANSACTION_PROVENANCE.full_name}
            WHERE source_transaction_id IN ({placeholders})
        """  # noqa: S608 — placeholders count derived from typed list; values bound
        row = db.execute(sql, fixture_ids).fetchone()
    else:
        row = db.execute(
            f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name}"  # noqa: S608 — TableRef constant
        ).fetchone()
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
        "SELECT category, categorized_by "  # noqa: S608 — TableRef constant
        f"FROM {FCT_TRANSACTIONS.full_name} "
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
            "SELECT source_transaction_id, source_type "  # noqa: S608 — TableRef constant
            f"FROM {FCT_TRANSACTION_PROVENANCE.full_name} "
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


def _verify_transfers_match_ground_truth(
    db: Database, spec: ExpectationSpec
) -> ExpectationResult:
    """Assert every labeled transfer pair lands as one ``transfer_pair_id``.

    Reads ``synthetic.ground_truth`` for transfer pairs (two source rows
    sharing a ``transfer_pair_id``), maps each gold ``source_transaction_id``
    forward through ``prep.int_transactions__matched`` to its
    ``core.fct_transactions.transaction_id``, and asserts both legs of each
    labeled pair end up under the same non-null ``transfer_pair_id``.

    Complements ``score_transfer_detection`` (graded F1) with a binary
    all-or-nothing pass/fail signal — useful when you want a regression
    that drops even one labeled pair to fail the scenario.
    """
    rows = db.execute(f"""
        WITH gold_pairs AS (
            SELECT transfer_pair_id, source_transaction_id
            FROM {GROUND_TRUTH.full_name}
            WHERE transfer_pair_id IS NOT NULL
        )
        SELECT
            g.transfer_pair_id,
            g.source_transaction_id,
            t.transaction_id,
            t.transfer_pair_id AS predicted_pair
        FROM gold_pairs g
        LEFT JOIN prep.int_transactions__matched m
          ON m.source_transaction_id = g.source_transaction_id
        LEFT JOIN {FCT_TRANSACTIONS.full_name} t
          ON t.transaction_id = m.transaction_id
    """).fetchall()  # noqa: S608 — TableRef constants

    pairs: dict[Any, list[tuple[Any, Any]]] = {}
    for gold_pair_id, source_id, _txn_id, predicted_pair in rows:
        pairs.setdefault(gold_pair_id, []).append((source_id, predicted_pair))

    failures: list[dict[str, Any]] = []
    for gold_pair_id, legs in pairs.items():
        predicted_pair_ids = {p for _src, p in legs}
        # Pass criteria: exactly one non-null predicted_pair_id, shared by both legs.
        if None in predicted_pair_ids or len(predicted_pair_ids) != 1:
            failures.append({
                "gold_transfer_pair_id": gold_pair_id,
                "legs": [{"source_id": s, "predicted_pair_id": p} for s, p in legs],
            })

    return ExpectationResult(
        name=spec.description or "transfers_match_ground_truth",
        kind="transfers_match_ground_truth",
        passed=not failures,
        details={
            "labeled_pair_count": len(pairs),
            "failure_count": len(failures),
            "failures": failures[:10],  # cap output; full count in failure_count
        },
    )


_VERIFIERS: dict[str, Callable[[Database, ExpectationSpec], ExpectationResult]] = {
    "match_decision": _verify_match_decision,
    "gold_record_count": _verify_gold_record_count,
    "category_for_transaction": _verify_category_for_transaction,
    "provenance_for_transaction": _verify_provenance_for_transaction,
    "transfers_match_ground_truth": _verify_transfers_match_ground_truth,
}
