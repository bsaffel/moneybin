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
    if match_type:
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
    db: Database, match_type: str | None = None
) -> list[dict[str, Any]]:
    """Return pending match decisions awaiting user review.

    Args:
        db: Database instance.
        match_type: Filter by type ('dedup', 'transfer'), or None for all.
    """
    where = "WHERE match_status = 'pending' AND reversed_at IS NULL"
    params: list[Any] = []
    if match_type:
        if match_type not in VALID_MATCH_TYPES:
            raise ValueError(f"Invalid match_type: {match_type!r}")
        where += " AND match_type = ?"
        params.append(match_type)
    rows = db.execute(
        f"""
        SELECT {_MATCH_DECISION_SELECT} FROM app.match_decisions
        {where}
        ORDER BY confidence_score DESC
        """,  # noqa: S608 — match_type validated above
        params,
    ).fetchall()
    return [dict(zip(_MATCH_DECISION_COLUMNS, row, strict=True)) for row in rows]


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
    db: Database, *, limit: int = 50, match_type: str | None = None
) -> list[dict[str, Any]]:
    """Return recent match decisions for display."""
    where = "WHERE 1=1"
    params: list[Any] = []
    if match_type:
        if match_type not in VALID_MATCH_TYPES:
            raise ValueError(f"Invalid match_type: {match_type!r}")
        where += " AND match_type = ?"
        params.append(match_type)
    params.append(limit)
    rows = db.execute(
        f"""
        SELECT {_MATCH_DECISION_SELECT} FROM app.match_decisions
        {where}
        ORDER BY decided_at DESC
        LIMIT ?
        """,  # noqa: S608 — match_type validated above; limit is parameterized
        params,
    ).fetchall()
    return [dict(zip(_MATCH_DECISION_COLUMNS, row, strict=True)) for row in rows]
