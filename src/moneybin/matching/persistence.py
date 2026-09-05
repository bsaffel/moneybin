"""Read queries for app.match_decisions.

All database access uses parameterized queries via the Database class. Mutations
(insert / status update / reverse) live in
``moneybin.repositories.match_decisions_repo.MatchDecisionsRepo`` so every write
emits a paired ``app.audit_log`` row (Invariant 10); this module keeps the read
projections the matcher and CLI consume.
"""

import logging
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Literal, get_args

import duckdb

from moneybin.database import Database
from moneybin.tables import MATCH_DECISIONS

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
        SELECT {_MATCH_DECISION_SELECT} FROM {MATCH_DECISIONS.full_name}
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
        SELECT {_MATCH_DECISION_SELECT} FROM {MATCH_DECISIONS.full_name}
        {where}
        ORDER BY confidence_score DESC
        {limit_clause}
        """,  # noqa: S608 — match_type validated above; limit is parameterized
        params,
    ).fetchall()
    return [dict(zip(_MATCH_DECISION_COLUMNS, row, strict=True)) for row in rows]


def count_pending_matches(db: Database, *, match_type: str | None = None) -> int:
    """Match decisions awaiting user review; ``match_type`` narrows to one type."""
    where = "WHERE match_status = 'pending' AND reversed_at IS NULL"
    params: list[Any] = []
    if match_type is not None:
        if match_type not in VALID_MATCH_TYPES:
            raise ValueError(f"Invalid match_type: {match_type!r}")
        where += " AND match_type = ?"
        params.append(match_type)
    return _count(db, where, params)


def count_matches_settled_since(
    db: Database, since: datetime | None, *, match_type: str | None = None
) -> int:
    """Decisions accepted, rejected or reversed after ``since``.

    The complement of :func:`count_pending_matches` for a reader that has to
    know whether a *materialized* model still holds pre-decision rows: leaving
    ``pending`` does not rewrite a ``kind="FULL"`` table, only the next refresh
    does. ``since`` is the model's last rebuild, aware or naive UTC; ``None``
    means no rebuild stamp is available, so no decision can be assumed
    reflected and every settled one counts.
    """
    where = "WHERE match_status <> 'pending'"
    params: list[Any] = []
    if since is not None:
        # `decided_at`/`reversed_at` are naive local (a `CURRENT_TIMESTAMP`
        # cast into a TIMESTAMP column); the rebuild stamp is UTC. Cast both to
        # instants rather than comparing wall clocks an offset apart.
        where += " AND COALESCE(reversed_at, decided_at)::TIMESTAMPTZ > ?"
        params.append(since)
    if match_type is not None:
        if match_type not in VALID_MATCH_TYPES:
            raise ValueError(f"Invalid match_type: {match_type!r}")
        where += " AND match_type = ?"
        params.append(match_type)
    return _count(db, where, params)


def _count(db: Database, where: str, params: list[Any]) -> int:
    """Run one counting projection over the match-decision queue."""
    try:
        row = db.execute(
            f"""
            SELECT COUNT(*) FROM {MATCH_DECISIONS.full_name}
            {where}
            """,  # noqa: S608  # TableRef constant + literal where; values parameterized
            params,
        ).fetchone()
    except duckdb.CatalogException:
        return 0  # table not created until the first matcher run
    return int(row[0]) if row else 0


def get_match_decision(db: Database, match_id: str) -> dict[str, Any] | None:
    """Return one match decision by id, or None if absent."""
    row = db.execute(
        f"""
        SELECT {_MATCH_DECISION_SELECT} FROM {MATCH_DECISIONS.full_name}
        WHERE match_id = ?
        """,  # noqa: S608 — column list is a module constant, not user input
        [match_id],
    ).fetchone()
    if row is None:
        return None
    return dict(zip(_MATCH_DECISION_COLUMNS, row, strict=True))


def get_match_statuses(db: Database, match_ids: Sequence[str]) -> dict[str, str]:
    """Return current status for each existing match ID.

    A batch result identifies which decisions reconciliation reversed, not only
    how many.
    """
    if not match_ids:
        return {}
    placeholders = ", ".join("?" for _ in match_ids)
    rows = db.execute(
        f"""
        SELECT match_id, match_status FROM {MATCH_DECISIONS.full_name}
        WHERE match_id IN ({placeholders})
        """,  # noqa: S608 — placeholders are '?' literals; every value is parameterized
        list(match_ids),
    ).fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


def get_active_dedup_edges(
    db: Database,
    *,
    statuses: tuple[MatchStatus, ...],
) -> list[dict[str, str]]:
    """Return non-reversed dedup edges in the requested statuses.

    Each row carries the four fields needed to build UnionFind components:
    ``source_type_a``, ``source_transaction_id_a``, ``source_type_b``,
    ``source_transaction_id_b``, and ``account_id``.

    ``statuses`` is required and has no default because the two answers mean
    different things and picking the wrong one is not visible at the call site.
    ``('accepted', 'pending')`` is the *prospective* graph — what the matcher
    has proposed — and is what the engine seeds union-find with and what
    ``MatchingService.get_pending`` clusters the review queue by. Only
    ``('accepted',)`` describes what has actually collapsed: the prep fold
    (``int_transactions__matched``) folds accepted rows alone, so a pending edge
    leaves both source rows distinct in ``core``. Anything acting on rows that
    really did merge — and especially anything destructive — asks for accepted
    only.
    """
    placeholders = ", ".join("?" for _ in statuses)
    rows = db.execute(
        f"""
        SELECT source_type_a, source_transaction_id_a,
               source_type_b, source_transaction_id_b,
               account_id
        FROM {MATCH_DECISIONS.full_name}
        WHERE match_type = 'dedup'
          AND match_status IN ({placeholders})
          AND reversed_at IS NULL
        ORDER BY account_id, source_type_a, source_transaction_id_a,
                 source_type_b, source_transaction_id_b
        """,  # noqa: S608 — placeholders only; every status is bound
        list(statuses),
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
        f"""
        SELECT source_type_a, source_transaction_id_a, source_origin_a,
               source_type_b, source_transaction_id_b, source_origin_b,
               account_id, account_id_b
        FROM {MATCH_DECISIONS.full_name}
        WHERE match_status = 'rejected'
          AND match_type = ?
        """,  # noqa: S608 — TableRef constant; match_type is parameterized
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
        SELECT {_MATCH_DECISION_SELECT} FROM {MATCH_DECISIONS.full_name}
        {where}
        ORDER BY decided_at DESC, match_id DESC
        {limit_clause}
        """,  # noqa: S608 — match_type validated above; limit is parameterized
        params,
    ).fetchall()
    return [dict(zip(_MATCH_DECISION_COLUMNS, row, strict=True)) for row in rows]
