"""V036: create and deterministically backfill categorization decisions."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any

import duckdb

from moneybin.repositories.categorization_decisions_repo import (
    categorization_decision_id,
)

logger = logging.getLogger(__name__)

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS app.categorization_decisions (
    decision_id VARCHAR PRIMARY KEY,
    transaction_id VARCHAR NOT NULL UNIQUE,
    status VARCHAR NOT NULL
        CHECK (status IN ('pending', 'accepted', 'rejected')),
    category_id VARCHAR,
    merchant_id VARCHAR,
    proposed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    decided_at TIMESTAMP,
    decided_by VARCHAR
        CHECK (decided_by IS NULL OR decided_by IN ('user', 'system')),
    CHECK (
        (status = 'pending' AND decided_at IS NULL AND decided_by IS NULL)
        OR (
            status IN ('accepted', 'rejected')
            AND decided_at IS NOT NULL
            AND decided_by IS NOT NULL
        )
    ),
    CHECK (
        status != 'rejected'
        OR (category_id IS NULL AND merchant_id IS NULL)
    )
)
"""

_COMMENTS = (
    (
        "decision_id",
        "Deterministic cat_<sha256[:16]> identifier bound to transaction_id",
    ),
    ("transaction_id", "Canonical core.fct_transactions transaction under review"),
    ("status", "Explicit pending, accepted, or rejected lifecycle state"),
    ("category_id", "Accepted canonical category target"),
    ("merchant_id", "Accepted canonical merchant target when assigned"),
    ("proposed_at", "When the pending proposal was materialized"),
    ("decided_at", "When accepted or rejected; NULL while pending"),
    ("decided_by", "user for MCP/CLI decisions; system for migration backfill"),
)


def migrate(conn: object) -> None:
    """Create the table and audit deterministic accepted-history backfill."""
    db = conn
    if not isinstance(db, duckdb.DuckDBPyConnection):
        raise TypeError("V036 requires a DuckDB connection")
    logger.debug("V036: CREATE TABLE app.categorization_decisions")
    db.execute(_CREATE_SQL)
    for column, comment in _COMMENTS:
        escaped = comment.replace("'", "''")
        db.execute(
            f"COMMENT ON COLUMN app.categorization_decisions.{column} "  # noqa: S608  # static identifier + escaped literal
            f"IS '{escaped}'"
        )
    rows: list[tuple[str, str | None, str | None, datetime]] = db.execute(
        """
        SELECT transaction_id, category_id, merchant_id, categorized_at
        FROM app.transaction_categories
        ORDER BY transaction_id
        """
    ).fetchall()
    for transaction_id, category_id, merchant_id, categorized_at in rows:
        decision_id = categorization_decision_id(str(transaction_id))
        exists = db.execute(
            "SELECT 1 FROM app.categorization_decisions WHERE decision_id = ?",
            [decision_id],
        ).fetchone()
        if exists is not None:
            continue
        db.execute(
            """
            INSERT INTO app.categorization_decisions (
                decision_id, transaction_id, status, category_id, merchant_id,
                proposed_at, decided_at, decided_by
            ) VALUES (?, ?, 'accepted', ?, ?, ?, ?, 'system')
            """,
            [
                decision_id,
                transaction_id,
                category_id,
                merchant_id,
                categorized_at,
                categorized_at,
            ],
        )
        after: dict[str, Any] = {
            "decision_id": decision_id,
            "transaction_id": str(transaction_id),
            "status": "accepted",
            "category_id": str(category_id) if category_id is not None else None,
            "merchant_id": str(merchant_id) if merchant_id is not None else None,
            "proposed_at": categorized_at.isoformat(),
            "decided_at": categorized_at.isoformat(),
            "decided_by": "system",
        }
        db.execute(
            """
            INSERT INTO app.audit_log (
                audit_id, actor, action, target_schema, target_table, target_id,
                before_value, after_value, operation_id, context_json
            ) VALUES (?, 'migration', 'categorization_decision.backfill',
                      'app', 'categorization_decisions', ?, NULL, ?, ?, ?)
            """,
            [
                uuid.uuid4().hex,
                decision_id,
                json.dumps(after),
                f"op_migration_v036_{decision_id}",
                json.dumps({"migration": "V036"}),
            ],
        )
    logger.debug(f"V036: backfilled {len(rows)} categorization decision(s)")
