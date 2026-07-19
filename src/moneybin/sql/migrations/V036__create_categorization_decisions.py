"""V036: create and deterministically backfill categorization attempts."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

import duckdb

from moneybin.repositories.categorization_decisions_repo import (
    categorization_decision_id,
)

logger = logging.getLogger(__name__)

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS app.categorization_decisions (
    decision_id VARCHAR PRIMARY KEY,
    transaction_id VARCHAR NOT NULL,
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    status VARCHAR NOT NULL
        CHECK (status IN ('pending', 'accepted', 'rejected', 'superseded')),
    category_id VARCHAR,
    merchant_id VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    categorized_by VARCHAR,
    confidence DECIMAL(3, 2),
    rule_id VARCHAR,
    source_type VARCHAR,
    category_revision BIGINT NOT NULL CHECK (category_revision >= 0),
    proposed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    decided_at TIMESTAMP,
    decided_by VARCHAR
        CHECK (decided_by IS NULL OR decided_by IN ('user', 'system')),
    reversed_at TIMESTAMP,
    reversed_by VARCHAR,
    UNIQUE (transaction_id, attempt_number),
    CHECK (
        (status = 'pending' AND decided_at IS NULL AND decided_by IS NULL)
        OR (
            status IN ('accepted', 'rejected', 'superseded')
            AND decided_at IS NOT NULL
            AND decided_by IS NOT NULL
        )
    ),
    CHECK (
        status != 'accepted'
        OR (category_id IS NOT NULL AND category IS NOT NULL)
    ),
    CHECK (
        status = 'accepted'
        OR (
            category_id IS NULL
            AND merchant_id IS NULL
            AND category IS NULL
            AND subcategory IS NULL
            AND categorized_by IS NULL
            AND confidence IS NULL
            AND rule_id IS NULL
            AND source_type IS NULL
        )
    ),
    CHECK (
        (reversed_at IS NULL AND reversed_by IS NULL)
        OR (
            reversed_at IS NOT NULL
            AND reversed_by IS NOT NULL
            AND status IN ('accepted', 'rejected')
        )
    )
)
"""

_COMMENTS = (
    ("decision_id", "Deterministic transaction-bound proposal-attempt identifier"),
    ("transaction_id", "Canonical transaction under review"),
    ("attempt_number", "Monotonic proposal attempt for this transaction"),
    ("status", "Pending, accepted, rejected, or superseded attempt state"),
    ("category_id", "Immutable accepted canonical category target"),
    ("merchant_id", "Immutable accepted canonical merchant target"),
    ("category", "Immutable accepted category display snapshot"),
    ("subcategory", "Immutable accepted subcategory display snapshot"),
    ("categorized_by", "Immutable accepted assignment-method snapshot"),
    ("confidence", "Immutable accepted confidence snapshot"),
    ("rule_id", "Immutable accepted categorization-rule snapshot"),
    ("source_type", "Immutable accepted source snapshot"),
    ("category_revision", "Category audit revision observed by this attempt"),
    ("proposed_at", "When the pending attempt was materialized"),
    ("decided_at", "When the attempt became terminal"),
    ("decided_by", "User or system actor class that terminalized the attempt"),
    ("reversed_at", "When audit undo reversed this outcome without deleting it"),
    ("reversed_by", "Actor that reversed this outcome"),
)


def _validate_legacy_rows(db: duckdb.DuckDBPyConnection) -> None:
    for column in ("category_id", "categorized_at"):
        row = db.execute(
            f"""
            SELECT COUNT(*)
            FROM app.transaction_categories
            WHERE {column} IS NULL
            """,  # noqa: S608  # Static migration-owned column identifiers
        ).fetchone()
        count = int(row[0]) if row is not None else 0
        if count:
            raise ValueError(
                "V036 cannot backfill accepted categorization attempts: "
                f"{count} app.transaction_categories row(s) have NULL {column}. "
                f"Populate a canonical {column} for every legacy row, then retry "
                "the migration."
            )


def migrate(conn: object) -> None:
    """Create versioned attempts and audit deterministic accepted backfill."""
    db = conn
    if not isinstance(db, duckdb.DuckDBPyConnection):
        raise TypeError("V036 requires a DuckDB connection")
    _validate_legacy_rows(db)
    logger.debug("V036: CREATE TABLE app.categorization_decisions")
    db.execute(_CREATE_SQL)
    for column, comment in _COMMENTS:
        escaped = comment.replace("'", "''")
        db.execute(
            f"COMMENT ON COLUMN app.categorization_decisions.{column} "  # noqa: S608  # Static identifier + escaped literal
            f"IS '{escaped}'"
        )
    rows: list[
        tuple[
            str,
            str,
            str | None,
            str,
            str | None,
            datetime,
            str | None,
            Decimal | None,
            str | None,
            str,
            int,
        ]
    ] = db.execute(
        """
        SELECT tc.transaction_id, tc.category, tc.subcategory, tc.category_id,
               tc.merchant_id, tc.categorized_at, tc.categorized_by,
               tc.confidence, tc.rule_id, tc.source_type,
               (
                   SELECT COUNT(*)
                   FROM app.audit_log AS audit
                   WHERE audit.target_schema = 'app'
                     AND audit.target_table = 'transaction_categories'
                     AND audit.target_id = tc.transaction_id
               ) AS category_revision
        FROM app.transaction_categories AS tc
        ORDER BY tc.transaction_id
        """
    ).fetchall()
    inserted = 0
    for (
        transaction_id,
        category,
        subcategory,
        category_id,
        merchant_id,
        categorized_at,
        categorized_by,
        confidence,
        rule_id,
        source_type,
        category_revision,
    ) in rows:
        decision_id = categorization_decision_id(transaction_id)
        exists = db.execute(
            """
            SELECT 1
            FROM app.categorization_decisions
            WHERE transaction_id = ? AND attempt_number = 1
            """,
            [transaction_id],
        ).fetchone()
        if exists is not None:
            continue
        db.execute(
            """
            INSERT INTO app.categorization_decisions (
                decision_id, transaction_id, attempt_number, status,
                category_id, merchant_id, category, subcategory,
                categorized_by, confidence, rule_id, source_type,
                category_revision, proposed_at, decided_at, decided_by
            ) VALUES (
                ?, ?, 1, 'accepted', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'system'
            )
            """,
            [
                decision_id,
                transaction_id,
                category_id,
                merchant_id,
                category,
                subcategory,
                categorized_by,
                confidence,
                rule_id,
                source_type,
                category_revision,
                categorized_at,
                categorized_at,
            ],
        )
        after: dict[str, Any] = {
            "decision_id": decision_id,
            "transaction_id": transaction_id,
            "attempt_number": 1,
            "status": "accepted",
            "category_id": category_id,
            "merchant_id": merchant_id,
            "category": category,
            "subcategory": subcategory,
            "categorized_by": categorized_by,
            "confidence": str(confidence) if confidence is not None else None,
            "rule_id": rule_id,
            "source_type": source_type,
            "category_revision": category_revision,
            "proposed_at": categorized_at.isoformat(),
            "decided_at": categorized_at.isoformat(),
            "decided_by": "system",
            "reversed_at": None,
            "reversed_by": None,
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
        inserted += 1
    logger.debug(f"V036: backfilled {inserted} categorization decision(s)")
