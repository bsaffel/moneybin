"""V026: add predicted ``transaction_id`` column to ``raw.manual_transactions``.

``transactions_create`` returns the predicted gold-key ``transaction_id``
to the caller *before* ``refresh_run`` materializes the row into
``core.fct_transactions`` — so an agent can write notes/tags against that
id immediately, well before the next transform. Without this column,
``_run_orphan_app_state`` (PR4) can't tell pending-but-legitimate rows
apart from truly orphaned curation, and the doctor recipe would prescribe
deletion of valid user state (data-loss path; see PR #231 reviews).

The column carries the same SHA256-truncated hash
``_predict_manual_gold_key`` computes in Python and the SQLMesh
``int_transactions__matched`` model computes in SQL. The doctor audit
joins this column to ``app.transaction_notes.transaction_id`` /
``app.transaction_tags.transaction_id`` to suppress the false-positive.

Pure additive DDL (``ADD COLUMN IF NOT EXISTS ... NULL``) plus a Python
backfill for pre-existing rows: the hash is deterministic from
``source_transaction_id`` + ``account_id``, both already present, so the
backfill never invents data.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, cast

logger = logging.getLogger(__name__)

# Must stay in lockstep with ``_MANUAL_SOURCE_TYPE`` /
# ``_predict_manual_gold_key`` in ``src/moneybin/services/transaction_service.py``
# and the unmatched-row branch of ``src/moneybin/sqlmesh/models/int/int_transactions__matched.sql``.
# A drift here re-introduces the false-positive this migration fixes.
_MANUAL_SOURCE_TYPE = "manual"


def _predict(source_transaction_id: str, account_id: str) -> str:
    raw = f"{_MANUAL_SOURCE_TYPE}|{source_transaction_id}|{account_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def migrate(conn: object) -> None:
    """Add ``raw.manual_transactions.transaction_id`` + backfill. Idempotent."""
    logger.info("V026: ADD COLUMN IF NOT EXISTS raw.manual_transactions.transaction_id")
    conn.execute(  # type: ignore[union-attr]
        "ALTER TABLE raw.manual_transactions "
        "ADD COLUMN IF NOT EXISTS transaction_id VARCHAR"
    )

    # COMMENT ON COLUMN is idempotent — safe on replay.
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN raw.manual_transactions.transaction_id IS "
        "'Predicted gold-key transaction_id (SHA256 of "
        "''manual|source_transaction_id|account_id'' truncated to 16 hex); "
        "matches the value the SQLMesh transform will assign in "
        "core.fct_transactions. Populated at INSERT time so the doctor "
        "orphan_app_state audit can suppress false-positives for notes/tags "
        "written before the next refresh materializes the row.'"
    )

    # Backfill any pre-existing rows where the column is NULL. Deterministic
    # hash over already-present columns — no fabricated data.
    fetched = cast(
        list[tuple[Any, ...]],
        conn.execute(  # type: ignore[union-attr]
            "SELECT source_transaction_id, account_id "
            "FROM raw.manual_transactions WHERE transaction_id IS NULL"
        ).fetchall(),
    )
    rows: list[tuple[str, str]] = [(str(r[0]), str(r[1])) for r in fetched]
    if rows:
        logger.info(
            f"V026: backfilling transaction_id for {len(rows)} pre-existing manual row(s)"
        )
        for source_transaction_id, account_id in rows:
            conn.execute(  # type: ignore[union-attr]
                "UPDATE raw.manual_transactions SET transaction_id = ? "
                "WHERE source_transaction_id = ?",
                [_predict(source_transaction_id, account_id), source_transaction_id],
            )
