"""Undo / history / get over audited operations (REC-PR3 Phase 6).

The undo *consumer* of Invariant 10: any audited ``app.*`` mutation is reversible
as a unit keyed on ``operation_id``. Every audit row's full before/after capture
(Req 4) lets :meth:`BaseRepo.undo_event` synthesize the inverse; this service
orchestrates loading an operation, guarding it, and dispatching each row through
:func:`undo_dispatch.repo_for` inside one transaction under a fresh operation id
(so the undo is itself queryable and undoable).

**Block, don't cascade** (spec §Block-don't-cascade): when a *later* operation
touched the same ``(target_table, target_id)``, undo refuses and returns the
blocker operation ids — the agent walks the chain explicitly rather than the
service silently reversing unrelated later work.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import cast

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import RecoveryAction, UserError
from moneybin.metrics.registry import (
    audit_undo_rows_reversed_total,
    audit_undo_total,
)
from moneybin.services.audit_service import AuditEvent, AuditService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_dispatch import is_registered, repo_for

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UndoResult:
    """Outcome of reversing one operation."""

    undo_operation_id: str
    """The fresh operation id grouping the undo's own audit rows."""
    undone_operation_id: str
    """The original operation that was reversed."""
    reversed_row_count: int
    """How many rows were inverted (markers and no-ops are skipped)."""
    tables: list[str]
    """Distinct ``app.*`` tables the undo touched."""


@dataclass(frozen=True)
class OperationSummary:
    """One operation in the audit history, grouped by ``operation_id``."""

    operation_id: str
    occurred_at: str
    actor: str
    actions: list[str]
    tables: list[str]
    row_count: int
    is_undo: bool
    undoes_operation_id: str | None
    can_undo: bool
    undo_blocked_by: list[str] | None
    recovery_actions: list[RecoveryAction]


@dataclass(frozen=True)
class OperationDetail:
    """Full before/after for every row in one operation (pre-undo inspection)."""

    operation_id: str
    events: list[AuditEvent]
    can_undo: bool
    undo_blocked_by: list[str] | None


def _undo_action(operation_id: str, *, confidence: str = "certain") -> RecoveryAction:
    return RecoveryAction(
        tool="system_audit_undo",
        arguments={"operation_id": operation_id},
        rationale=f"Reverse operation {operation_id} as a unit.",
        confidence="certain" if confidence == "certain" else "suggested",
        idempotent=False,  # a second undo of the same op raises already_undone
    )


class UndoService:
    """Reverse, list, and inspect audited operations."""

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Bind to an open Database; build an ``AuditService`` if none supplied."""
        self._db = db
        self._audit = audit if audit is not None else AuditService(db)

    def undo(self, operation_id: str, *, actor: str) -> UndoResult:
        """Reverse every row of ``operation_id`` atomically under a new operation.

        Raises ``UserError`` with a recovery action for each refusal:
        ``UNDO_OPERATION_NOT_FOUND`` (no such op), ``UNDO_ALREADY_UNDONE`` (a prior
        undo reversed it), ``RECOVERY_NO_PATH`` (the op touched a table outside the
        undoable ``app.*`` surface, e.g. ``raw.manual_transactions``), or
        ``UNDO_CASCADE_BLOCKED`` (a later op modified the same rows).
        """
        events = self._audit.events_for_operation(operation_id)
        if not events:
            audit_undo_total.labels(outcome="not_found").inc()
            raise UserError(
                f"No operation found with id {operation_id!r}.",
                code=error_codes.UNDO_OPERATION_NOT_FOUND,
                recovery_actions=[
                    RecoveryAction(
                        tool="system_audit_history",
                        arguments={},
                        rationale="List recent operations to find a valid id.",
                        confidence="certain",
                        idempotent=True,
                    )
                ],
            )
        undone_by = self._already_undone_by(operation_id)
        if undone_by is not None:
            audit_undo_total.labels(outcome="already_undone").inc()
            raise UserError(
                f"Operation {operation_id!r} was already undone by {undone_by!r}.",
                code=error_codes.UNDO_ALREADY_UNDONE,
                recovery_actions=[_undo_action(undone_by, confidence="suggested")],
            )
        unresolvable = self._unresolvable_tables(events)
        if unresolvable:
            audit_undo_total.labels(outcome="no_path").inc()
            raise UserError(
                f"Operation {operation_id!r} touched {', '.join(unresolvable)}, "
                "outside the undoable app.* surface — not reversible via undo.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        blockers = self._cascade_blockers(operation_id)
        if blockers:
            audit_undo_total.labels(outcome="cascade_blocked").inc()
            raise UserError(
                f"Operation {operation_id!r} cannot be undone: later operations "
                f"modified the same rows. Undo those first.",
                code=error_codes.UNDO_CASCADE_BLOCKED,
                recovery_actions=[_undo_action(b) for b in blockers],
            )

        # Reverse newest-first inside one transaction under a fresh operation id.
        # Marker rows (target_id is None, e.g. the tag.rename parent) carry no
        # single-row mutation, so they are skipped — only the per-row children
        # are inverted.
        row_events = [e for e in events if e.target_id is not None]
        with operation() as undo_op:
            self._db.begin()
            try:
                undone: list[AuditEvent] = []
                for event in sorted(row_events, key=lambda e: e.audit_id, reverse=True):
                    repo = repo_for(
                        event.target_schema or "", event.target_table or "", self._db
                    )
                    inverse = repo.undo_event(event, actor=actor, in_outer_txn=True)
                    if inverse is not None:
                        undone.append(inverse)
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise
        tables = sorted({e.target_table for e in undone if e.target_table})
        audit_undo_total.labels(outcome="success").inc()
        audit_undo_rows_reversed_total.inc(len(undone))
        logger.info(
            f"audit_undo undone_op={operation_id} undo_op={undo_op} "
            f"rows={len(undone)} actor={actor}"
        )
        return UndoResult(
            undo_operation_id=undo_op,
            undone_operation_id=operation_id,
            reversed_row_count=len(undone),
            tables=tables,
        )

    def history(
        self,
        *,
        domain: str | None = None,
        since: str | None = None,
        actor: str | None = None,
        limit: int = 50,
        include_undone: bool = False,
    ) -> list[OperationSummary]:
        """Return recent operations grouped by ``operation_id``, newest first.

        ``domain`` filters to operations containing an action in that family
        (e.g. ``"tag"`` → any ``tag.*`` row). ``include_undone`` controls whether
        the undo operations themselves (``is_undo=TRUE``) appear; by default they
        are hidden to reduce noise, and the originals they reversed still appear
        marked ``can_undo=False``.
        """
        clauses: list[str] = []
        params: list[object] = []
        if actor is not None:
            clauses.append("actor = ?")
            params.append(actor)
        if since is not None:
            clauses.append("occurred_at >= ?")
            params.append(since)
        if not include_undone:
            clauses.append("is_undo = FALSE")
        if domain is not None:
            clauses.append(
                "operation_id IN (SELECT operation_id FROM app.audit_log "
                "WHERE action LIKE ?)"
            )
            params.append(f"{domain}.%")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        rows = self._db.conn.execute(
            f"""
            SELECT operation_id,
                   MAX(occurred_at) AS occurred_at,
                   MAX(actor) AS actor,
                   COUNT(*) AS row_count,
                   BOOL_OR(is_undo) AS is_undo,
                   MAX(undoes_operation_id) AS undoes_operation_id,
                   list(DISTINCT action) AS actions,
                   list(DISTINCT target_table) AS tables
              FROM app.audit_log
              {where}
             GROUP BY operation_id
             ORDER BY MAX(rowid) DESC
             LIMIT ?
            """,  # noqa: S608  # WHERE built from literal clauses; values parameterized
            params,
        ).fetchall()
        return [self._summarize(r) for r in rows]

    def get(self, operation_id: str) -> OperationDetail:
        """Return full before/after for every row of one operation.

        Lets an agent pre-check what an undo would change before executing.
        Raises ``UNDO_OPERATION_NOT_FOUND`` for an unknown id.
        """
        events = self._audit.events_for_operation(operation_id)
        if not events:
            raise UserError(
                f"No operation found with id {operation_id!r}.",
                code=error_codes.UNDO_OPERATION_NOT_FOUND,
                recovery_actions=[
                    RecoveryAction(
                        tool="system_audit_history",
                        arguments={},
                        rationale="List recent operations to find a valid id.",
                        confidence="certain",
                        idempotent=True,
                    )
                ],
            )
        blockers = self._cascade_blockers(operation_id)
        can_undo = (
            self._already_undone_by(operation_id) is None
            and not blockers
            and not self._unresolvable_tables(events)
        )
        return OperationDetail(
            operation_id=operation_id,
            events=events,
            can_undo=can_undo,
            undo_blocked_by=blockers or None,
        )

    def _summarize(self, row: tuple[object, ...]) -> OperationSummary:
        operation_id = str(row[0])
        blockers = self._cascade_blockers(operation_id)
        undone_by = self._already_undone_by(operation_id)
        events = self._audit.events_for_operation(operation_id)
        unresolvable = bool(self._unresolvable_tables(events))
        can_undo = undone_by is None and not blockers and not unresolvable
        if can_undo:
            recovery = [_undo_action(operation_id)]
        elif blockers:
            recovery = [_undo_action(b) for b in blockers]
        elif undone_by is not None:
            recovery = [_undo_action(undone_by, confidence="suggested")]
        else:
            recovery = []
        actions = cast("list[str] | None", row[6]) or []
        tables = cast("list[str | None] | None", row[7]) or []
        return OperationSummary(
            operation_id=operation_id,
            occurred_at=str(row[1]),
            actor=str(row[2]),
            actions=sorted(actions),
            tables=sorted(t for t in tables if t is not None),
            row_count=int(cast("int", row[3])) if row[3] is not None else 0,
            is_undo=bool(row[4]),
            undoes_operation_id=None if row[5] is None else str(row[5]),
            can_undo=can_undo,
            undo_blocked_by=blockers or None,
            recovery_actions=recovery,
        )

    def _already_undone_by(self, operation_id: str) -> str | None:
        """Return the operation id of an existing undo of ``operation_id``, if any."""
        row = self._db.conn.execute(
            "SELECT operation_id FROM app.audit_log "
            "WHERE undoes_operation_id = ? LIMIT 1",
            [operation_id],
        ).fetchone()
        return str(row[0]) if row is not None else None

    def _unresolvable_tables(self, events: list[AuditEvent]) -> list[str]:
        """``schema.table`` of any mutated row no repo owns (sorted, deduped)."""
        return sorted({
            f"{e.target_schema}.{e.target_table}"
            for e in events
            if e.target_id is not None
            and not is_registered(e.target_schema or "", e.target_table or "")
        })

    def _cascade_blockers(self, operation_id: str) -> list[str]:
        """Operation ids that modified this op's rows *after* it, newest first.

        A later operation blocks only if it is a still-live *forward* mutation:
        undo rows (``is_undo=TRUE``) restore prior state rather than introduce a
        conflicting change, and a forward op that has itself been undone no longer
        has a live effect. Excluding both is what makes the documented walk —
        "undo the blocker, then the original undoes cleanly" — actually work.

        Ordered by ``rowid`` (monotonic, append-only audit log) rather than
        ``occurred_at`` so sub-second sequential operations order deterministically.
        """
        rows = self._db.conn.execute(
            """
            WITH op_rows AS (
                SELECT target_table, target_id, rowid
                  FROM app.audit_log
                 WHERE operation_id = ?
            ),
            boundary AS (SELECT MAX(rowid) AS r FROM op_rows)
            SELECT a.operation_id, MAX(a.rowid) AS latest
              FROM app.audit_log a
              JOIN (
                  SELECT DISTINCT target_table, target_id
                    FROM op_rows WHERE target_id IS NOT NULL
              ) t
                ON a.target_table = t.target_table
               AND a.target_id = t.target_id
             WHERE a.operation_id <> ?
               AND a.rowid > (SELECT r FROM boundary)
               AND a.is_undo = FALSE
               AND a.operation_id NOT IN (
                   SELECT undoes_operation_id FROM app.audit_log
                    WHERE undoes_operation_id IS NOT NULL
               )
             GROUP BY a.operation_id
             ORDER BY latest DESC
            """,
            [operation_id, operation_id],
        ).fetchall()
        return [str(r[0]) for r in rows]
