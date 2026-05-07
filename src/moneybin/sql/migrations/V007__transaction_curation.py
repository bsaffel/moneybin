"""Schema foundation for the transaction-curation feature.

Adds five new tables (raw.manual_transactions, app.transaction_tags,
app.transaction_splits, app.imports, app.audit_log), reshapes
app.transaction_notes from single-note to multi-note, and retires
app.ai_audit_log by re-routing its rows into the unified app.audit_log.

Table creation for the new tables is handled by ``init_schemas`` via the
registered schema files in ``src/moneybin/sql/schema/*.sql`` — those run on
every startup before migrations, so this module only handles the parts that
``init_schemas`` cannot do idempotently:

- Reshape an existing single-note app.transaction_notes table (the old DDL had
  the same name, so CREATE TABLE IF NOT EXISTS in the new schema file is a
  no-op when a legacy table is already there).
- Move pre-existing app.ai_audit_log rows into app.audit_log (when the table
  exists at all — most installs will not have it) and drop the old table.

Idempotent: probes the live schema before doing any work and returns early
when no migration is needed.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, cast

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Reshape transaction_notes and retire ai_audit_log."""
    _reshape_transaction_notes(conn)
    _retire_ai_audit_log(conn)


def _columns(conn: object, schema: str, table: str) -> set[str]:
    rows = conn.execute(  # type: ignore[union-attr]
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchall()
    return {r[0] for r in rows}


def _table_exists(conn: object, schema: str, table: str) -> bool:
    row = conn.execute(  # type: ignore[union-attr]
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row is not None


def _reshape_transaction_notes(conn: object) -> None:
    """Convert legacy single-note app.transaction_notes to multi-note shape.

    Legacy shape: PK transaction_id, columns (transaction_id, note, created_at).
    New shape: PK note_id, columns (note_id, transaction_id, text, author,
    created_at). Legacy rows get a fresh 12-hex-char note_id and author='legacy';
    text comes from the legacy note column; created_at is preserved.
    """
    cols = _columns(conn, "app", "transaction_notes")
    if not cols:
        # table absent — init_schemas handles fresh creation
        return
    if "note_id" in cols:
        return
    if "note" not in cols:
        # Unrecognized shape — bail out rather than corrupt data.
        logger.warning(
            "app.transaction_notes has neither 'note_id' nor 'note' column; "
            "skipping reshape"
        )
        return

    logger.info("Reshaping app.transaction_notes: single-note → multi-note")
    legacy_rows = cast(
        list[tuple[Any, ...]],
        conn.execute(  # type: ignore[union-attr]
            "SELECT transaction_id, note, created_at FROM app.transaction_notes"
        ).fetchall(),
    )

    conn.execute("DROP TABLE app.transaction_notes")  # type: ignore[union-attr]
    conn.execute(  # type: ignore[union-attr]
        """
        CREATE TABLE app.transaction_notes (
            note_id        VARCHAR PRIMARY KEY,
            transaction_id VARCHAR NOT NULL,
            text           VARCHAR NOT NULL,
            author         VARCHAR NOT NULL,
            created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(  # type: ignore[union-attr]
        "CREATE INDEX IF NOT EXISTS idx_transaction_notes_txn "
        "ON app.transaction_notes(transaction_id)"
    )

    for transaction_id, note_text, created_at in legacy_rows:
        note_id = uuid.uuid4().hex[:12]
        conn.execute(  # type: ignore[union-attr]
            "INSERT INTO app.transaction_notes "
            "(note_id, transaction_id, text, author, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [note_id, transaction_id, note_text, "legacy", created_at],
        )

    logger.info(f"Reshaped {len(legacy_rows)} legacy transaction notes")


def _retire_ai_audit_log(conn: object) -> None:
    """Move app.ai_audit_log rows into app.audit_log and drop the old table.

    The retired table was specified in privacy-and-ai-trust.md but was not
    actually created in any prior migration, so most installs won't have it.
    Probe before doing anything.
    """
    if not _table_exists(conn, "app", "ai_audit_log"):
        return

    logger.info("Retiring app.ai_audit_log → app.audit_log")
    cols = _columns(conn, "app", "ai_audit_log")
    # Read all rows with column names so we can pack unknown extras into
    # context_json conservatively rather than assuming the spec's column list
    # is exhaustive.
    col_list = sorted(cols)
    # column names come from information_schema (allowlisted catalog read), not user input
    select_sql = "SELECT " + ", ".join(col_list) + " FROM app.ai_audit_log"  # noqa: S608
    rows = cast(
        list[tuple[Any, ...]],
        conn.execute(select_sql).fetchall(),  # type: ignore[union-attr]
    )

    for row in rows:
        record: dict[str, Any] = dict(zip(col_list, row, strict=True))
        audit_id = record.get("audit_id") or uuid.uuid4().hex[:12]
        occurred_at = record.get("timestamp")
        backend = record.get("backend") or "unknown"
        model = record.get("model") or "unknown"
        actor = f"ai:{backend}:{model}"

        # Pack everything except audit_id/timestamp into context_json so any
        # column the spec didn't enumerate still rides along.
        context: dict[str, Any] = {
            k: v for k, v in record.items() if k not in {"audit_id", "timestamp"}
        }
        context_json = json.dumps(context, default=str)

        conn.execute(  # type: ignore[union-attr]
            "INSERT INTO app.audit_log "
            "(audit_id, occurred_at, actor, action, context_json) "
            "VALUES (?, ?, ?, ?, ?)",
            [audit_id, occurred_at, actor, "ai.external_call", context_json],
        )

    conn.execute("DROP TABLE app.ai_audit_log")  # type: ignore[union-attr]
    logger.info(f"Migrated {len(rows)} ai_audit_log rows into app.audit_log")
