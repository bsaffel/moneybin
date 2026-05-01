"""Per-record expectations about matching outcomes."""

from __future__ import annotations

from typing import Any, Literal

from moneybin.database import Database
from moneybin.tables import (
    FCT_TRANSACTION_PROVENANCE,
    FCT_TRANSACTIONS,
    GROUND_TRUTH,
    INT_TRANSACTIONS_MATCHED,
    MATCH_DECISIONS,
)
from moneybin.validation.expectations._types import SourceTransactionRef
from moneybin.validation.result import ExpectationResult


def verify_match_decision(
    db: Database,
    *,
    transactions: list[SourceTransactionRef],
    expected: Literal["matched", "not_matched"] = "matched",
    expected_match_type: Literal["dedup", "transfer"] | None = None,
    expected_confidence_min: float = 0.0,
    description: str = "",
) -> ExpectationResult:
    """Verify that listed source txns resolve to one (or distinct) gold rows.

    See docs/specs/testing-scenario-comprehensive.md §R1 Tier 2 for the
    matched-branch semantics (coverage, collapse, confidence, match_type).
    """
    if expected not in {"matched", "not_matched"}:
        return ExpectationResult(
            name=description or "match_decision",
            kind="match_decision",
            passed=False,
            details={"reason": f"unknown expected mode: {expected!r}"},
        )

    if expected_match_type is not None and expected_match_type not in {
        "dedup",
        "transfer",
    }:
        return ExpectationResult(
            name=description or "match_decision",
            kind="match_decision",
            passed=False,
            details={"reason": f"unknown expected_match_type: {expected_match_type!r}"},
        )

    # Row-IN-(VALUES ...) clause: placeholder count derived from a typed list,
    # values bound via `?`.
    placeholders = ",".join(["(?, ?)"] * len(transactions))
    params: list[str] = []
    for t in transactions:
        params.extend([t.source_transaction_id, t.source_type])
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
    expected_pairs = {(t.source_transaction_id, t.source_type) for t in transactions}
    missing = sorted(expected_pairs - found_pairs)
    gold_ids = sorted({r[2] for r in rows})
    match_types = {r[3] for r in rows if r[3] is not None}

    details: dict[str, Any] = {
        "expected": expected,
        "gold_record_ids": gold_ids,
        "missing_sources": [list(p) for p in missing],
        "match_types": sorted(match_types),
        "expected_match_type": expected_match_type,
        "confidence_floor": expected_confidence_min,
    }

    if expected != "matched":
        # not_matched: listed sources must resolve to ≥2 distinct gold rows.
        # Coverage isn't required — a missing source is itself "not matched".
        return ExpectationResult(
            name=description or "match_decision",
            kind="match_decision",
            passed=len(gold_ids) >= 2,
            details=details,
        )

    if missing or len(gold_ids) != 1:
        return ExpectationResult(
            name=description or "match_decision",
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

    confidence_ok = actual_confidence >= expected_confidence_min
    type_ok = expected_match_type is None or match_types == {expected_match_type}

    return ExpectationResult(
        name=description or "match_decision",
        kind="match_decision",
        passed=confidence_ok and type_ok,
        details=details,
    )


def verify_transfers_match_ground_truth(
    db: Database, *, description: str = ""
) -> ExpectationResult:
    """Assert every labeled transfer pair lands as one transfer_pair_id.

    See _verify_transfers_match_ground_truth in the legacy module for
    detailed semantics.
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
        LEFT JOIN {INT_TRANSACTIONS_MATCHED.full_name} m
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
        name=description or "transfers_match_ground_truth",
        kind="transfers_match_ground_truth",
        passed=not failures,
        details={
            "labeled_pair_count": len(pairs),
            "failure_count": len(failures),
            "failures": failures[:10],  # cap output; full count in failure_count
        },
    )
