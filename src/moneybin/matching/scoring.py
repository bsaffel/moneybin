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
from moneybin.matching import UNIONED_TABLE, quote_table_ref
from moneybin.matching.persistence import MatchTier

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
    # Source file per side — the cardinality unit for the assign_components guard
    # (two rows from the same file are always distinct txns, never duplicates).
    # None when unknown (e.g. unit-test fixtures); the guard then does not fire.
    source_file_a: str | None = None
    source_file_b: str | None = None
    # Transfer-only; dedup pairs leave these None (used by _claim_key for slot scoping).
    account_id_a: str | None = None
    account_id_b: str | None = None


def compute_confidence(
    *,
    date_distance_days: int,
    description_similarity: float,
    date_window_days: int = 3,
    exact_key_floor: float | None = None,
) -> float:
    """Compute a confidence score from matching signals.

    When ``exact_key_floor`` is set and the pair is exact-key
    (``date_distance_days == 0``), confidence is lifted into
    ``[exact_key_floor, 1.0]``: same account + exact amount + same day is a
    near-certain duplicate regardless of how differently two sources render the
    description (OFX truncates differently from CSV). ``description_similarity``
    is kept as a monotonic *tiebreaker* — it orders which 1:1 pairing wins in
    ``assign_components`` — never as an accept/reject gate. For
    ``date_distance_days > 0`` the floor is ignored and the weighted formula
    applies, so description still matters when dates differ.
    """
    date_score = (
        max(0.0, 1.0 - (date_distance_days / date_window_days))
        if date_window_days > 0
        else 1.0
    )
    if exact_key_floor is not None and date_distance_days == 0:
        return exact_key_floor + (1.0 - exact_key_floor) * description_similarity
    return (_WEIGHT_DATE * date_score) + (_WEIGHT_DESCRIPTION * description_similarity)


def get_candidates_cross_source(
    db: Database,
    *,
    table: str = UNIONED_TABLE,
    date_window_days: int = 3,
    excluded_ids: set[tuple[str, str]] | None = None,
    rejected_pairs: list[dict[str, Any]] | None = None,
    high_confidence_threshold: float | None = None,
) -> list[CandidatePair]:
    """Find cross-source candidate pairs (Tier 3).

    Blocking: same account_id, same amount, date within window,
    different source_type OR different source_origin.

    When ``high_confidence_threshold`` is supplied, exact-key pairs
    (``date_distance == 0``) are scored at/above it so they auto-merge
    regardless of description similarity (see ``compute_confidence``).
    """
    return _get_candidates(
        db,
        table=table,
        date_window_days=date_window_days,
        tier="3",
        excluded_ids=excluded_ids,
        rejected_pairs=rejected_pairs,
        high_confidence_threshold=high_confidence_threshold,
    )


def get_candidates_within_source(
    db: Database,
    *,
    table: str = UNIONED_TABLE,
    date_window_days: int = 3,
    excluded_ids: set[tuple[str, str]] | None = None,
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
    tier: MatchTier,
    excluded_ids: set[tuple[str, str]] | None,
    rejected_pairs: list[dict[str, Any]] | None,
    high_confidence_threshold: float | None = None,
) -> list[CandidatePair]:
    """Internal: run blocking + scoring query for a given tier."""
    # Exact-key auto-merge is a cross-source-only rule (Tier 3). Within-source
    # (Tier 2b) keeps the weighted formula so its acceptance is unchanged.
    exact_key_floor = high_confidence_threshold if tier == "3" else None
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

    table = quote_table_ref(table)

    # Manual-source exemption: per transaction-curation spec Req 6, manual rows
    # are excluded as candidates in *either* direction — never matched against
    # imported rows, never matched against other manual rows. Predicate is
    # applied to both sides of the self-join.
    query = f"""
        SELECT
            a.source_transaction_id AS stid_a,
            a.source_type AS st_a,
            a.source_origin AS so_a,
            a.source_file AS sf_a,
            a.description AS desc_a,
            b.source_transaction_id AS stid_b,
            b.source_type AS st_b,
            b.source_origin AS so_b,
            b.source_file AS sf_b,
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
            AND a.source_type != 'manual'
            AND b.source_type != 'manual'
            AND (
                a.source_type,
                a.source_origin,
                a.source_transaction_id
            ) < (
                b.source_type,
                b.source_origin,
                b.source_transaction_id
            )
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
            sf_a,
            desc_a,
            stid_b,
            st_b,
            so_b,
            sf_b,
            desc_b,
            acct,
            date_dist,
            desc_sim,
        ) = row

        if excluded_ids and (
            (stid_a, acct) in excluded_ids or (stid_b, acct) in excluded_ids
        ):
            continue

        if (st_a, stid_a, st_b, stid_b, acct) in rejected_set:
            continue

        confidence = compute_confidence(
            date_distance_days=int(date_dist),
            description_similarity=float(desc_sim),
            date_window_days=date_window_days,
            exact_key_floor=exact_key_floor,
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
                source_file_a=sf_a,
                source_file_b=sf_b,
            )
        )

    return results
