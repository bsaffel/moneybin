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
from typing import Literal, cast

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


@dataclass(frozen=True)
class _Undoability:
    """The three conditions that decide whether one operation can be undone."""

    can_undo: bool
    blockers: list[str]
    undone_by: str | None
    unresolvable: list[str]


def _undo_action(
    operation_id: str, *, confidence: Literal["certain", "suggested"] = "certain"
) -> RecoveryAction:
    return RecoveryAction(
        tool="system_audit_undo",
        arguments={"operation_id": operation_id},
        rationale=f"Reverse operation {operation_id} as a unit.",
        confidence=confidence,
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
            raise self._not_found_error(operation_id)
        u = self._undoability(operation_id)
        if u.undone_by is not None:
            audit_undo_total.labels(outcome="already_undone").inc()
            raise UserError(
                f"Operation {operation_id!r} was already undone by {u.undone_by!r}.",
                code=error_codes.UNDO_ALREADY_UNDONE,
                recovery_actions=[_undo_action(u.undone_by, confidence="suggested")],
            )
        if u.unresolvable:
            audit_undo_total.labels(outcome="no_path").inc()
            raise UserError(
                f"Operation {operation_id!r} touched {', '.join(u.unresolvable)}, "
                "outside the undoable app.* surface — not reversible via undo.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        if u.blockers:
            audit_undo_total.labels(outcome="cascade_blocked").inc()
            raise UserError(
                f"Operation {operation_id!r} cannot be undone: later operations "
                f"modified the same rows. Undo those first.",
                code=error_codes.UNDO_CASCADE_BLOCKED,
                recovery_actions=[_undo_action(b) for b in u.blockers],
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
            raise self._not_found_error(operation_id)
        u = self._undoability(operation_id)
        return OperationDetail(
            operation_id=operation_id,
            events=events,
            can_undo=u.can_undo,
            undo_blocked_by=u.blockers or None,
        )

    def _summarize(self, row: tuple[object, ...]) -> OperationSummary:
        (
            op_raw,
            occurred_at,
            actor,
            row_count,
            is_undo,
            undoes_op,
            actions_raw,
            tables_raw,
        ) = row
        operation_id = str(op_raw)
        u = self._undoability(operation_id)
        if u.can_undo:
            recovery = [_undo_action(operation_id)]
        elif u.blockers:
            recovery = [_undo_action(b) for b in u.blockers]
        elif u.undone_by is not None:
            recovery = [_undo_action(u.undone_by, confidence="suggested")]
        else:
            recovery = []
        actions = cast("list[str] | None", actions_raw) or []
        tables = cast("list[str | None] | None", tables_raw) or []
        return OperationSummary(
            operation_id=operation_id,
            occurred_at=str(occurred_at),
            actor=str(actor),
            actions=sorted(actions),
            tables=sorted(t for t in tables if t is not None),
            row_count=int(cast("int", row_count)) if row_count is not None else 0,
            is_undo=bool(is_undo),
            undoes_operation_id=None if undoes_op is None else str(undoes_op),
            can_undo=u.can_undo,
            undo_blocked_by=u.blockers or None,
            recovery_actions=recovery,
        )

    @staticmethod
    def _not_found_error(operation_id: str) -> UserError:
        """The shared ``UNDO_OPERATION_NOT_FOUND`` error (undo and get)."""
        return UserError(
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

    def _undoability(self, operation_id: str) -> _Undoability:
        """Compute the three undo-gating conditions once, for a single operation.

        Shared by ``undo`` (which branches on the individual fields to raise its
        specialized errors), ``get``, and ``history``'s per-operation summary — so
        "what makes an operation undoable" lives in exactly one place.
        """
        undone_by = self._already_undone_by(operation_id)
        blockers = self._cascade_blockers(operation_id)
        unresolvable = self._unresolvable_tables(operation_id)
        return _Undoability(
            can_undo=undone_by is None and not blockers and not unresolvable,
            blockers=blockers,
            undone_by=undone_by,
            unresolvable=unresolvable,
        )

    def _already_undone_by(self, operation_id: str) -> str | None:
        """Return the operation id of an existing undo of ``operation_id``, if any."""
        row = self._db.conn.execute(
            "SELECT operation_id FROM app.audit_log "
            "WHERE undoes_operation_id = ? LIMIT 1",
            [operation_id],
        ).fetchone()
        return str(row[0]) if row is not None else None

    def _unresolvable_tables(self, operation_id: str) -> list[str]:
        """``schema.table`` of any mutated row no repo owns (sorted, deduped).

        Queries the distinct targets directly rather than loading and JSON-decoding
        every row's full before/after payload just to derive this set.
        """
        rows = self._db.conn.execute(
            "SELECT DISTINCT target_schema, target_table FROM app.audit_log "
            "WHERE operation_id = ? AND target_id IS NOT NULL",
            [operation_id],
        ).fetchall()
        return sorted(
            f"{schema}.{table}"
            for schema, table in rows
            if not is_registered(str(schema or ""), str(table or ""))
        )

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
