"""Candidate blocking and confidence scoring for transaction matching.

Blocking: SQL queries against DuckDB that return narrow candidate sets
based on exact account, exact amount, and date-window constraints.

Scoring: Combines date distance and description similarity into a single
confidence score. Weights are tunable but defaults are spec-compliant.
"""

import logging
from dataclasses import dataclass
from typing import Any

from moneybin.database import Database

logger = logging.getLogger(__name__)

# Scoring weights — sum to 1.0.
_WEIGHT_DATE = 0.40
_WEIGHT_DESCRIPTION = 0.60


@dataclass(frozen=True)
class CandidatePair:
    """A scored candidate pair from blocking + scoring."""

    source_transaction_id_a: str
    source_type_a: str
    source_origin_a: str
    source_transaction_id_b: str
    source_type_b: str
    source_origin_b: str
    account_id: str
    date_distance_days: int
    description_similarity: float
    confidence_score: float
    description_a: str
    description_b: str


def compute_confidence(
    *,
    date_distance_days: int,
    description_similarity: float,
    date_window_days: int = 3,
) -> float:
    """Compute a confidence score from matching signals."""
    date_score = (
        max(0.0, 1.0 - (date_distance_days / date_window_days))
        if date_window_days > 0
        else 1.0
    )
    return (_WEIGHT_DATE * date_score) + (_WEIGHT_DESCRIPTION * description_similarity)


def get_candidates_cross_source(
    db: Database,
    *,
    table: str = "prep.int_transactions__unioned",
    date_window_days: int = 3,
    excluded_ids: set[str] | None = None,
    rejected_pairs: list[dict[str, Any]] | None = None,
) -> list[CandidatePair]:
    """Find cross-source candidate pairs (Tier 3).

    Blocking: same account_id, same amount, date within window,
    different source_type OR different source_origin.
    """
    return _get_candidates(
        db,
        table=table,
        date_window_days=date_window_days,
        tier="3",
        excluded_ids=excluded_ids,
        rejected_pairs=rejected_pairs,
    )


def get_candidates_within_source(
    db: Database,
    *,
    table: str = "prep.int_transactions__unioned",
    date_window_days: int = 3,
    excluded_ids: set[str] | None = None,
    rejected_pairs: list[dict[str, Any]] | None = None,
) -> list[CandidatePair]:
    """Find within-source candidate pairs (Tier 2b).

    Same as cross-source but requires same source_origin AND source_type,
    different source_file.
    """
    return _get_candidates(
        db,
        table=table,
        date_window_days=date_window_days,
        tier="2b",
        excluded_ids=excluded_ids,
        rejected_pairs=rejected_pairs,
    )


def _get_candidates(
    db: Database,
    *,
    table: str,
    date_window_days: int,
    tier: str,
    excluded_ids: set[str] | None,
    rejected_pairs: list[dict[str, Any]] | None,
) -> list[CandidatePair]:
    """Internal: run blocking + scoring query for a given tier."""
    if tier == "2b":
        source_filter = """
            AND a.source_type = b.source_type
            AND a.source_origin = b.source_origin
            AND a.source_file != b.source_file
        """
    else:
        source_filter = """
            AND (a.source_type != b.source_type OR a.source_origin != b.source_origin)
        """

    # Validate table name for non-defaults
    if table != "prep.int_transactions__unioned":
        from sqlglot import exp

        parts = table.split(".")
        if len(parts) == 2:
            safe_schema = exp.to_identifier(parts[0], quoted=True).sql("duckdb")
            safe_table = exp.to_identifier(parts[1], quoted=True).sql("duckdb")
            table = f"{safe_schema}.{safe_table}"

    query = f"""
        SELECT
            a.source_transaction_id AS stid_a,
            a.source_type AS st_a,
            a.source_origin AS so_a,
            a.description AS desc_a,
            b.source_transaction_id AS stid_b,
            b.source_type AS st_b,
            b.source_origin AS so_b,
            b.description AS desc_b,
            a.account_id,
            ABS(DATEDIFF('day', a.transaction_date, b.transaction_date)) AS date_dist,
            jaro_winkler_similarity(
                COALESCE(a.description, ''),
                COALESCE(b.description, '')
            ) AS desc_sim
        FROM {table} AS a
        JOIN {table} AS b
            ON a.account_id = b.account_id
            AND a.amount = b.amount
            AND ABS(DATEDIFF('day', a.transaction_date, b.transaction_date)) <= ?
            AND a.source_transaction_id < b.source_transaction_id
            {source_filter}
        ORDER BY desc_sim DESC
    """  # noqa: S608 — table name validated above; date_window_days is parameterized

    rows = db.execute(query, [date_window_days]).fetchall()

    # Build rejected pair set for fast lookup
    rejected_set: set[tuple[str, ...]] = set()
    if rejected_pairs:
        for rp in rejected_pairs:
            rejected_set.add((
                rp["source_type_a"],
                rp["source_transaction_id_a"],
                rp["source_type_b"],
                rp["source_transaction_id_b"],
                rp["account_id"],
            ))
            rejected_set.add((
                rp["source_type_b"],
                rp["source_transaction_id_b"],
                rp["source_type_a"],
                rp["source_transaction_id_a"],
                rp["account_id"],
            ))

    results: list[CandidatePair] = []
    for row in rows:
        (
            stid_a,
            st_a,
            so_a,
            desc_a,
            stid_b,
            st_b,
            so_b,
            desc_b,
            acct,
            date_dist,
            desc_sim,
        ) = row

        if excluded_ids and (stid_a in excluded_ids or stid_b in excluded_ids):
            continue

        if (st_a, stid_a, st_b, stid_b, acct) in rejected_set:
            continue

        confidence = compute_confidence(
            date_distance_days=int(date_dist),
            description_similarity=float(desc_sim),
            date_window_days=date_window_days,
        )

        results.append(
            CandidatePair(
                source_transaction_id_a=stid_a,
                source_type_a=st_a,
                source_origin_a=so_a,
                source_transaction_id_b=stid_b,
                source_type_b=st_b,
                source_origin_b=so_b,
                account_id=acct,
                date_distance_days=int(date_dist),
                description_similarity=float(desc_sim),
                confidence_score=confidence,
                description_a=desc_a or "",
                description_b=desc_b or "",
            )
        )

    return results
