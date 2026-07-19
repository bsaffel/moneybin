"""Unified audit log emission and query.

Every in-scope mutating service calls ``record_audit_event()`` inside the same
DuckDB transaction as its mutation. The surface (CLI/MCP) supplies the actor;
the service supplies action + target + before/after.

See ``docs/specs/transaction-curation.md`` (§Audit log, Req 25–31) and the
schema in ``src/moneybin/sql/schema/app_audit_log.sql``.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any

from moneybin.database import Database
from moneybin.metrics.registry import audit_events_emitted_total
from moneybin.services.mutation_context import current_operation_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuditEvent:
    """One row of ``app.audit_log``.

    ``occurred_at`` may be empty when returned from ``record_audit_event``
    (the DB defaults it via ``CURRENT_TIMESTAMP``); it is always populated
    when read back via ``list_events`` or ``chain_for``.
    """

    audit_id: str
    occurred_at: str
    actor: str
    action: str
    target_schema: str | None
    target_table: str | None
    target_id: str | None
    before_value: dict[str, Any] | None
    after_value: dict[str, Any] | None
    parent_audit_id: str | None
    operation_id: str
    context_json: dict[str, Any] | None = None
    is_undo: bool = False
    undoes_operation_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dict (CLI/MCP envelope payload)."""
        return {
            "audit_id": self.audit_id,
            "occurred_at": self.occurred_at,
            "actor": self.actor,
            "action": self.action,
            "target_schema": self.target_schema,
            "target_table": self.target_table,
            "target_id": self.target_id,
            "before_value": self.before_value,
            "after_value": self.after_value,
            "parent_audit_id": self.parent_audit_id,
            "operation_id": self.operation_id,
            "context_json": self.context_json,
            "is_undo": self.is_undo,
            "undoes_operation_id": self.undoes_operation_id,
        }


class AuditService:
    """Emit and query ``app.audit_log``."""

    def __init__(self, db: Database) -> None:
        """Bind the service to an open Database connection."""
        self._db = db
        self._has_undo_columns: bool | None = None

    def _undo_columns_sql(self) -> str:
        """SELECT fragment for the V024 undo columns, degrading on a pre-V024 schema.

        ``get_database(read_only=True)`` skips migrations, so a V023 ``audit_log``
        (not yet opened in write mode) lacks ``is_undo`` / ``undoes_operation_id``.
        Substituting literal defaults keeps the existing read tools working —
        pre-V024 rows have no undo data — instead of failing every audit SELECT
        with a missing-column error. Column positions are preserved so
        ``_row_to_event`` (indices 12/13) is unaffected. Probed once and cached.
        """
        if self._has_undo_columns is None:
            row = self._db.conn.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = 'app' AND table_name = 'audit_log' "
                "AND column_name = 'is_undo'"
            ).fetchone()
            self._has_undo_columns = row is not None
        return (
            "is_undo, undoes_operation_id"
            if self._has_undo_columns
            else "FALSE AS is_undo, NULL AS undoes_operation_id"
        )

    def record_audit_event(
        self,
        *,
        action: str,
        target: tuple[str | None, str | None, str | None],
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
        actor: str,
        parent_audit_id: str | None = None,
        context: dict[str, Any] | None = None,
        is_undo: bool = False,
        undoes_operation_id: str | None = None,
    ) -> AuditEvent:
        """Insert one audit event. Caller manages the surrounding txn.

        ``is_undo`` / ``undoes_operation_id`` mark rows written by the undo
        consumer (REC-PR3); a normal mutation leaves them at the defaults.
        """
        target_schema, target_table, target_id = target
        # Full UUID4 hex (32 chars). Audit log grows with every mutation plus
        # per-row tag.rename_row children — well past identifiers.md's 100K-row
        # threshold for full UUIDs over short app entity lifetimes. Internal
        # id; readability is not a constraint here.
        audit_id = uuid.uuid4().hex
        # Group key for this MCP/CLI call, read from the ambient MutationContext
        # set at the surface seam. Outside any context (a bare repo call) the
        # getter mints a fresh op_<uuid4_hex> so a lone mutation is its own
        # operation — operation_id is NOT NULL by design.
        operation_id = current_operation_id()
        self._db.conn.execute(
            """
            INSERT INTO app.audit_log (
                audit_id, actor, action,
                target_schema, target_table, target_id,
                before_value, after_value, parent_audit_id, operation_id,
                context_json, is_undo, undoes_operation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                audit_id,
                actor,
                action,
                target_schema,
                target_table,
                target_id,
                json.dumps(before) if before is not None else None,
                json.dumps(after) if after is not None else None,
                parent_audit_id,
                operation_id,
                json.dumps(context) if context is not None else None,
                is_undo,
                undoes_operation_id,
            ],
        )
        audit_events_emitted_total.labels(action=action, actor=actor).inc()
        logger.debug(f"audit_event audit_id={audit_id} action={action} actor={actor}")
        return AuditEvent(
            audit_id=audit_id,
            occurred_at="",
            actor=actor,
            action=action,
            target_schema=target_schema,
            target_table=target_table,
            target_id=target_id,
            before_value=before,
            after_value=after,
            parent_audit_id=parent_audit_id,
            operation_id=operation_id,
            context_json=context,
            is_undo=is_undo,
            undoes_operation_id=undoes_operation_id,
        )

    def list_events(
        self,
        *,
        actor: str | None = None,
        action_pattern: str | None = None,
        target_table: str | None = None,
        target_id: str | None = None,
        from_ts: str | None = None,
        to_ts: str | None = None,
        limit: int | None = 100,
    ) -> list[AuditEvent]:
        """Return filtered events in stable newest-first order."""
        clauses: list[str] = []
        params: list[Any] = []
        if actor is not None:
            clauses.append("actor = ?")
            params.append(actor)
        if action_pattern is not None:
            clauses.append("action LIKE ?")
            params.append(action_pattern)
        if target_table is not None:
            clauses.append("target_table = ?")
            params.append(target_table)
        if target_id is not None:
            clauses.append("target_id = ?")
            params.append(target_id)
        if from_ts is not None:
            clauses.append("occurred_at >= ?")
            params.append(from_ts)
        if to_ts is not None:
            clauses.append("occurred_at <= ?")
            params.append(to_ts)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        limit_sql = ""
        if limit is not None:
            limit_sql = "LIMIT ?"
            params.append(limit)
        rows = self._db.conn.execute(
            f"""
            SELECT audit_id, occurred_at, actor, action,
                   target_schema, target_table, target_id,
                   before_value, after_value, parent_audit_id,
                   operation_id, context_json, {self._undo_columns_sql()}
              FROM app.audit_log
              {where}
              ORDER BY occurred_at DESC, audit_id DESC
              {limit_sql}
            """,  # noqa: S608  # controlled fragments + parameterized filters
            params,
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def events_for_transaction(
        self, transaction_id: str, *, limit: int = 100
    ) -> list[AuditEvent]:
        """Return audit rows relating to one transaction, newest first.

        Row-grain ``target_id`` (REC-PR3) keys a child entity's audit row by its own
        PK (``note_id``, ``split_id``, ``transaction_id:tag``), not the parent — so
        this matches both ``target_id`` (transaction-level mutations like
        ``category.set``) and the ``transaction_id`` captured in the before/after row
        image (child mutations), keeping the per-transaction audit view complete.
        """
        rows = self._db.conn.execute(
            f"""
            SELECT audit_id, occurred_at, actor, action,
                   target_schema, target_table, target_id,
                   before_value, after_value, parent_audit_id,
                   operation_id, context_json, {self._undo_columns_sql()}
              FROM app.audit_log
             WHERE target_id = ?
                OR json_extract_string(before_value, '$.transaction_id') = ?
                OR json_extract_string(after_value, '$.transaction_id') = ?
             ORDER BY occurred_at DESC, rowid DESC
             LIMIT ?
            """,  # noqa: S608  # undo-columns fragment is a controlled literal
            [transaction_id, transaction_id, transaction_id, limit],
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def events_for_operation(self, operation_id: str) -> list[AuditEvent]:
        """Return every audit row written under one ``operation_id``, oldest first.

        The undo consumer (REC-PR3) loads an operation as a unit: ``UndoService``
        reverses the rows newest-first and ``system_audit_get`` exposes their
        full before/after.

        Tiebreak on ``rowid`` (monotonic, append-only), never ``audit_id``: every
        row in one operation shares ``occurred_at`` (DuckDB ``CURRENT_TIMESTAMP``
        is transaction-stable) and ``audit_id`` is a random uuid4, so ordering by
        it would scramble replay order. ``rowid`` is the same write-order key
        ``UndoService._cascade_blockers`` relies on.
        """
        rows = self._db.conn.execute(
            f"""
            SELECT audit_id, occurred_at, actor, action,
                   target_schema, target_table, target_id,
                   before_value, after_value, parent_audit_id,
                   operation_id, context_json, {self._undo_columns_sql()}
              FROM app.audit_log
             WHERE operation_id = ?
             ORDER BY occurred_at ASC, rowid ASC
            """,  # noqa: S608  # undo-columns fragment is a controlled literal
            [operation_id],
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def chain_for(self, audit_id: str) -> list[AuditEvent]:
        """Return the parent event plus all events whose ``parent_audit_id`` matches."""
        rows = self._db.conn.execute(
            f"""
            SELECT audit_id, occurred_at, actor, action,
                   target_schema, target_table, target_id,
                   before_value, after_value, parent_audit_id,
                   operation_id, context_json, {self._undo_columns_sql()}
              FROM app.audit_log
             WHERE audit_id = ? OR parent_audit_id = ?
             ORDER BY occurred_at ASC, audit_id ASC
            """,  # noqa: S608  # undo-columns fragment is a controlled literal
            [audit_id, audit_id],
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    @staticmethod
    def _row_to_event(row: tuple[Any, ...]) -> AuditEvent:
        return AuditEvent(
            audit_id=row[0],
            occurred_at=str(row[1]),
            actor=row[2],
            action=row[3],
            target_schema=row[4],
            target_table=row[5],
            target_id=row[6],
            before_value=json.loads(row[7]) if row[7] is not None else None,
            after_value=json.loads(row[8]) if row[8] is not None else None,
            parent_audit_id=row[9],
            operation_id=row[10],
            context_json=json.loads(row[11]) if row[11] is not None else None,
            is_undo=bool(row[12]),
            undoes_operation_id=row[13],
        )
