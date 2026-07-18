"""Read queries for app.match_decisions.

All database access uses parameterized queries via the Database class. Mutations
(insert / status update / reverse) live in
``moneybin.repositories.match_decisions_repo.MatchDecisionsRepo`` so every write
emits a paired ``app.audit_log`` row (Invariant 10); this module keeps the read
projections the matcher and CLI consume.
"""

import logging
from typing import Any, Literal, get_args

from moneybin.database import Database

logger = logging.getLogger(__name__)

MatchType = Literal["dedup", "transfer"]
MatchStatus = Literal["accepted", "pending", "rejected", "reversed"]
MatchTier = Literal["2b", "3"]

VALID_MATCH_TYPES = frozenset(get_args(MatchType))

# Column order matches the CREATE TABLE in app.match_decisions migration; kept
# in sync with the schema so SELECT/zip never re-derives it at runtime.
_MATCH_DECISION_COLUMNS: tuple[str, ...] = (
    "match_id",
    "source_transaction_id_a",
    "source_type_a",
    "source_origin_a",
    "source_transaction_id_b",
    "source_type_b",
    "source_origin_b",
    "account_id",
    "confidence_score",
    "match_signals",
    "match_type",
    "match_tier",
    "account_id_b",
    "match_status",
    "match_reason",
    "decided_by",
    "decided_at",
    "reversed_at",
    "reversed_by",
)
_MATCH_DECISION_SELECT = ", ".join(_MATCH_DECISION_COLUMNS)


def get_active_matches(
    db: Database, match_type: str | None = None
) -> list[dict[str, Any]]:
    """Return accepted, non-reversed match decisions."""
    where = "WHERE match_status = 'accepted' AND reversed_at IS NULL"
    params: list[Any] = []
    if match_type is not None:
        if match_type not in VALID_MATCH_TYPES:
            raise ValueError(f"Invalid match_type: {match_type!r}")
        where += " AND match_type = ?"
        params.append(match_type)
    rows = db.execute(
        f"""
        SELECT {_MATCH_DECISION_SELECT} FROM app.match_decisions
        {where}
        ORDER BY decided_at DESC
        """,  # noqa: S608 — match_type validated above
        params,
    ).fetchall()
    return [dict(zip(_MATCH_DECISION_COLUMNS, row, strict=True)) for row in rows]


def get_pending_matches(
    db: Database, match_type: str | None = None, *, limit: int | None = None
) -> list[dict[str, Any]]:
    """Return pending match decisions awaiting user review.

    Args:
        db: Database instance.
        match_type: Filter by type ('dedup', 'transfer'), or None for all.
        limit: Max rows (pushed to SQL ``LIMIT``), or None for all pending.
    """
    where = "WHERE match_status = 'pending' AND reversed_at IS NULL"
    params: list[Any] = []
    if match_type is not None:
        if match_type not in VALID_MATCH_TYPES:
            raise ValueError(f"Invalid match_type: {match_type!r}")
        where += " AND match_type = ?"
        params.append(match_type)
    limit_clause = ""
    if limit is not None:
        limit_clause = "LIMIT ?"
        params.append(limit)
    rows = db.execute(
        f"""
        SELECT {_MATCH_DECISION_SELECT} FROM app.match_decisions
        {where}
        ORDER BY confidence_score DESC
        {limit_clause}
        """,  # noqa: S608 — match_type validated above; limit is parameterized
        params,
    ).fetchall()
    return [dict(zip(_MATCH_DECISION_COLUMNS, row, strict=True)) for row in rows]


def get_match_decision(db: Database, match_id: str) -> dict[str, Any] | None:
    """Return one match decision by id, or None if absent."""
    row = db.execute(
        f"""
        SELECT {_MATCH_DECISION_SELECT} FROM app.match_decisions
        WHERE match_id = ?
        """,  # noqa: S608 — column list is a module constant, not user input
        [match_id],
    ).fetchone()
    if row is None:
        return None
    return dict(zip(_MATCH_DECISION_COLUMNS, row, strict=True))


def get_active_dedup_edges(
    db: Database,
) -> list[dict[str, str]]:
    """Return all active (accepted + pending, non-reversed) dedup edges.

    Each row carries the four fields needed to build UnionFind components:
    ``source_type_a``, ``source_transaction_id_a``, ``source_type_b``,
    ``source_transaction_id_b``, and ``account_id``.

    Used by MatchingService.get_pending to compute component_key for pending
    rows — the same component identity the prep fold uses for match_group_id.
    """
    rows = db.execute(
        """
        SELECT source_type_a, source_transaction_id_a,
               source_type_b, source_transaction_id_b,
               account_id
        FROM app.match_decisions
        WHERE match_type = 'dedup'
          AND match_status IN ('accepted', 'pending')
          AND reversed_at IS NULL
        ORDER BY account_id, source_type_a, source_transaction_id_a,
                 source_type_b, source_transaction_id_b
        """,  # noqa: S608 — no user-supplied values; all literals
    ).fetchall()
    cols = (
        "source_type_a",
        "source_transaction_id_a",
        "source_type_b",
        "source_transaction_id_b",
        "account_id",
    )
    return [dict(zip(cols, row, strict=True)) for row in rows]


def get_rejected_pairs(
    db: Database, match_type: MatchType = "dedup"
) -> list[dict[str, Any]]:
    """Return rejected pair keys to avoid re-proposing them."""
    rows = db.execute(
        """
        SELECT source_type_a, source_transaction_id_a, source_origin_a,
               source_type_b, source_transaction_id_b, source_origin_b,
               account_id, account_id_b
        FROM app.match_decisions
        WHERE match_status = 'rejected'
          AND match_type = ?
        """,
        [match_type],
    ).fetchall()
    columns = [
        "source_type_a",
        "source_transaction_id_a",
        "source_origin_a",
        "source_type_b",
        "source_transaction_id_b",
        "source_origin_b",
        "account_id",
        "account_id_b",
    ]
    return [dict(zip(columns, row, strict=True)) for row in rows]


def get_match_log(
    db: Database, *, limit: int | None = 50, match_type: str | None = None
) -> list[dict[str, Any]]:
    """Return recent match *decisions* for display.

    Excludes ``pending`` rows: a pending proposal is not yet a decision, and its
    ``decided_at`` holds the proposal time, not a decision time. The pending
    queue is read via :func:`get_pending_matches`.
    """
    where = "WHERE match_status != 'pending'"
    params: list[Any] = []
    if match_type is not None:
        if match_type not in VALID_MATCH_TYPES:
            raise ValueError(f"Invalid match_type: {match_type!r}")
        where += " AND match_type = ?"
        params.append(match_type)
    limit_clause = ""
    if limit is not None:
        limit_clause = "LIMIT ?"
        params.append(limit)
    rows = db.execute(
        f"""
        SELECT {_MATCH_DECISION_SELECT} FROM app.match_decisions
        {where}
        ORDER BY decided_at DESC, match_id DESC
        {limit_clause}
        """,  # noqa: S608 — match_type validated above; limit is parameterized
        params,
    ).fetchall()
    return [dict(zip(_MATCH_DECISION_COLUMNS, row, strict=True)) for row in rows]
