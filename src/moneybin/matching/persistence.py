"""CRUD operations for app.match_decisions.

All database access uses parameterized queries via the Database class.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any

from moneybin.database import Database

logger = logging.getLogger(__name__)

VALID_MATCH_TYPES = {"dedup", "transfer"}


def _columns(db: Database) -> list[str]:
    """Return column names for app.match_decisions."""
    return [
        desc[0]
        for desc in db.execute("SELECT * FROM app.match_decisions LIMIT 0").description
    ]


def create_match_decision(
    db: Database,
    *,
    match_id: str,
    source_transaction_id_a: str,
    source_type_a: str,
    source_origin_a: str,
    source_transaction_id_b: str,
    source_type_b: str,
    source_origin_b: str,
    account_id: str,
    confidence_score: float,
    match_signals: dict[str, Any],
    match_tier: str | None,
    match_status: str,
    decided_by: str,
    match_reason: str | None = None,
    match_type: str = "dedup",
    account_id_b: str | None = None,
) -> None:
    """Insert a new match decision."""
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            match_id,
            source_transaction_id_a,
            source_type_a,
            source_origin_a,
            source_transaction_id_b,
            source_type_b,
            source_origin_b,
            account_id,
            confidence_score,
            json.dumps(match_signals),
            match_type,
            match_tier,
            account_id_b,
            match_status,
            match_reason,
            decided_by,
            datetime.now(tz=UTC).isoformat(),
        ],
    )


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
        SELECT * FROM app.match_decisions
        {where}
        ORDER BY decided_at DESC
        """,  # noqa: S608 — match_type validated above
        params,
    ).fetchall()
    cols = _columns(db)
    return [dict(zip(cols, row, strict=True)) for row in rows]


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
        SELECT * FROM app.match_decisions
        {where}
        ORDER BY confidence_score DESC
        """,  # noqa: S608 — match_type validated above
        params,
    ).fetchall()
    cols = _columns(db)
    return [dict(zip(cols, row, strict=True)) for row in rows]


def update_match_status(
    db: Database, match_id: str, *, status: str, decided_by: str
) -> None:
    """Update the status of a match decision (e.g., pending -> accepted)."""
    db.execute(
        """
        UPDATE app.match_decisions
        SET match_status = ?, decided_by = ?, decided_at = ?
        WHERE match_id = ?
        """,
        [status, decided_by, datetime.now(tz=UTC).isoformat(), match_id],
    )


def undo_match(db: Database, match_id: str, *, reversed_by: str) -> None:
    """Reverse a match decision. Sets reversed_at, reversed_by, and match_status."""
    row = db.execute(
        "SELECT match_id FROM app.match_decisions WHERE match_id = ?",
        [match_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Match not found: {match_id}")
    db.execute(
        """
        UPDATE app.match_decisions
        SET reversed_at = ?, reversed_by = ?, match_status = 'reversed'
        WHERE match_id = ?
        """,
        [datetime.now(tz=UTC).isoformat(), reversed_by, match_id],
    )


def get_rejected_pairs(db: Database, match_type: str = "dedup") -> list[dict[str, Any]]:
    """Return rejected pair keys to avoid re-proposing them."""
    rows = db.execute(
        """
        SELECT source_type_a, source_transaction_id_a, source_origin_a,
               source_type_b, source_transaction_id_b, source_origin_b,
               account_id
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
        SELECT * FROM app.match_decisions
        {where}
        ORDER BY decided_at DESC
        LIMIT ?
        """,  # noqa: S608 — match_type validated above; limit is parameterized
        params,
    ).fetchall()
    cols = _columns(db)
    return [dict(zip(cols, row, strict=True)) for row in rows]
