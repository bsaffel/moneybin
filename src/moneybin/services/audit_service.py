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
    context_json: dict[str, Any] | None = None

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
            "context_json": self.context_json,
        }


class AuditService:
    """Emit and query ``app.audit_log``."""

    def __init__(self, db: Database) -> None:
        """Bind the service to an open Database connection."""
        self._db = db

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
    ) -> AuditEvent:
        """Insert one audit event. Caller manages the surrounding txn."""
        target_schema, target_table, target_id = target
        # Full UUID4 hex (32 chars). Audit log grows with every mutation plus
        # per-row tag.rename_row children — well past identifiers.md's 100K-row
        # threshold for full UUIDs over short app entity lifetimes. Internal
        # id; readability is not a constraint here.
        audit_id = uuid.uuid4().hex
        self._db.conn.execute(
            """
            INSERT INTO app.audit_log (
                audit_id, actor, action,
                target_schema, target_table, target_id,
                before_value, after_value, parent_audit_id, context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                json.dumps(context) if context is not None else None,
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
            context_json=context,
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
        limit: int = 100,
    ) -> list[AuditEvent]:
        """Return filtered events ordered by ``occurred_at DESC``."""
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
        params.append(limit)
        rows = self._db.conn.execute(
            f"""
            SELECT audit_id, occurred_at, actor, action,
                   target_schema, target_table, target_id,
                   before_value, after_value, parent_audit_id, context_json
              FROM app.audit_log
              {where}
              ORDER BY occurred_at DESC
              LIMIT ?
            """,
            params,
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def chain_for(self, audit_id: str) -> list[AuditEvent]:
        """Return the parent event plus all events whose ``parent_audit_id`` matches."""
        rows = self._db.conn.execute(
            """
            SELECT audit_id, occurred_at, actor, action,
                   target_schema, target_table, target_id,
                   before_value, after_value, parent_audit_id, context_json
              FROM app.audit_log
             WHERE audit_id = ? OR parent_audit_id = ?
             ORDER BY occurred_at ASC, audit_id ASC
            """,
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
            context_json=json.loads(row[10]) if row[10] is not None else None,
        )
