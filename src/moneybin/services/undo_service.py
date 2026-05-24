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
from dataclasses import dataclass, field
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
    unresolvable: list[str] | None
    """``schema.table`` of any row outside the undoable surface — distinguishes a
    raw-targeted refusal from a marker-only / already-undone one for the surface
    hint. Not part of the MCP payload; consumed only when building the action."""


@dataclass(frozen=True)
class _Undoability:
    """The three conditions that decide whether one operation can be undone."""

    can_undo: bool
    blockers: list[str]
    undone_by: str | None
    unresolvable: list[str]


@dataclass
class _UndoLiveness:
    """Net liveness over the undo graph, computed once from its edges.

    An operation is *currently* undone iff it has an undo that is not itself
    currently undone — the recursive definition
    ``is_undone(op) = ∃U: U.undoes == op ∧ ¬is_undone(U)``. The graph branches
    after a re-undo (an op gains a second undo child once its first is reversed),
    so a parity-by-depth walk is wrong; this evaluates the definition directly,
    memoized over the (small) set of undo edges. ``_children`` maps an operation
    id to the operation ids that undo it.
    """

    _children: dict[str, list[str]]
    _memo: dict[str, bool] = field(default_factory=dict)

    def is_undone(self, operation_id: str) -> bool:
        """Whether ``operation_id``'s effects are currently reversed (net)."""
        cached = self._memo.get(operation_id)
        if cached is not None:
            return cached
        # Pre-seed False before recursing so a (theoretically impossible, since the
        # audit log is append-only and an undo always references an earlier op)
        # cycle can't recurse forever.
        self._memo[operation_id] = False
        result = any(
            not self.is_undone(child) for child in self._children.get(operation_id, [])
        )
        self._memo[operation_id] = result
        return result

    def live_undo_of(self, operation_id: str) -> str | None:
        """The id of a currently-live undo of ``operation_id``, or ``None``."""
        for child in self._children.get(operation_id, []):
            if not self.is_undone(child):
                return child
        return None


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
                # Surface any blockers too: the op may be partly recoverable by
                # undoing later ops first, so don't dead-end the agent.
                recovery_actions=[_undo_action(b) for b in u.blockers] or None,
            )
        if u.blockers:
            audit_undo_total.labels(outcome="cascade_blocked").inc()
            raise UserError(
                f"Operation {operation_id!r} cannot be undone: later operations "
                f"modified the same rows. Undo those first.",
                code=error_codes.UNDO_CASCADE_BLOCKED,
                recovery_actions=[_undo_action(b) for b in u.blockers],
            )

        # Reverse in the exact reverse of write order inside one transaction under
        # a fresh operation id. ``events`` is write-ordered (events_for_operation
        # tiebreaks on rowid), so reversing it undoes the last write first — the
        # order a future parent-then-child insert needs. Marker rows (target_id is
        # None, e.g. the tag.rename parent) carry no single-row mutation, so they
        # are skipped — only the per-row children are inverted.
        row_events = [e for e in events if e.target_id is not None]
        if not row_events:
            # All events are markers (target_id None) — e.g. a tag.rename that
            # matched zero rows. There is nothing to reverse; minting an undo op
            # here would return an id with no audit rows (not itself queryable or
            # undoable), so refuse instead.
            audit_undo_total.labels(outcome="no_path").inc()
            raise UserError(
                f"Operation {operation_id!r} has no reversible row mutations "
                "(only marker events) — nothing to undo.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        with operation() as undo_op:
            self._db.begin()
            try:
                undone: list[AuditEvent] = []
                for event in reversed(row_events):
                    repo = repo_for(
                        event.target_schema or "",
                        event.target_table or "",
                        self._db,
                        audit=self._audit,
                    )
                    inverse = repo.undo_event(event, actor=actor, in_outer_txn=True)
                    if inverse is not None:
                        undone.append(inverse)
                self._db.commit()
            except UserError as e:
                # _require_capture can raise RECOVERY_NO_PATH mid-loop (a legacy
                # partial-capture row). Record the outcome like the pre-loop
                # refusals do, rather than letting it vanish from metrics.
                self._db.rollback()
                if e.code == error_codes.RECOVERY_NO_PATH:
                    audit_undo_total.labels(outcome="no_path").inc()
                raise
            except BaseException:
                self._db.rollback()
                raise
        if not undone:
            # Every row event was a no-op (before == after) — e.g. legacy
            # idempotent tag.add rows. Nothing was reversed; the fresh undo_op
            # carries no audit rows, so returning it would be a phantom success.
            # (Complement of the marker-only guard above, by the other filter.)
            audit_undo_total.labels(outcome="no_path").inc()
            raise UserError(
                f"Operation {operation_id!r} has no net effect to reverse "
                "(all captured rows show before == after).",
                code=error_codes.RECOVERY_NO_PATH,
            )
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
        liveness = self._build_undo_liveness()
        return [self._summarize(r, liveness) for r in rows]

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
            unresolvable=u.unresolvable or None,
        )

    def _summarize(
        self, row: tuple[object, ...], liveness: _UndoLiveness
    ) -> OperationSummary:
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
        u = self._undoability(operation_id, liveness)
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

    def _undoability(
        self, operation_id: str, liveness: _UndoLiveness | None = None
    ) -> _Undoability:
        """Compute the three undo-gating conditions once, for a single operation.

        Shared by ``undo`` (which branches on the individual fields to raise its
        specialized errors), ``get``, and ``history``'s per-operation summary — so
        "what makes an operation undoable" lives in exactly one place. ``history``
        passes a prebuilt ``liveness`` so the undo-edge set loads once per call
        rather than once per operation.
        """
        if liveness is None:
            liveness = self._build_undo_liveness()
        undone_by = liveness.live_undo_of(operation_id)
        blockers = self._cascade_blockers(operation_id, liveness)
        targets = self._row_targets(operation_id)
        unresolvable = sorted(
            f"{schema}.{table}"
            for schema, table in targets
            if not is_registered(schema, table)
        )
        # A marker-only operation (every event target_id NULL, e.g. a tag.rename
        # that matched zero rows) has no row to reverse — undo() refuses it, so
        # can_undo must be False here too or get()/history() would disagree.
        marker_only = not targets
        return _Undoability(
            can_undo=(
                undone_by is None
                and not blockers
                and not unresolvable
                and not marker_only
            ),
            blockers=blockers,
            undone_by=undone_by,
            unresolvable=unresolvable,
        )

    def _build_undo_liveness(self) -> _UndoLiveness:
        """Load every undo edge once and index it for net-liveness queries.

        One small scan of the undo rows (those carrying ``undoes_operation_id``)
        backs both "is this op currently undone?" and "is this candidate blocker
        still live?" — see :class:`_UndoLiveness`.
        """
        rows = self._db.conn.execute(
            "SELECT DISTINCT operation_id, undoes_operation_id FROM app.audit_log "
            "WHERE undoes_operation_id IS NOT NULL"
        ).fetchall()
        children: dict[str, list[str]] = {}
        for undo_op, undone_op in rows:
            children.setdefault(str(undone_op), []).append(str(undo_op))
        return _UndoLiveness(children)

    def _row_targets(self, operation_id: str) -> list[tuple[str, str]]:
        """Distinct ``(target_schema, target_table)`` of this op's row mutations.

        Only rows with a non-null ``target_id`` — marker rows carry no row
        mutation. One query backs both the unresolvable-table check and the
        marker-only check (empty result), instead of loading and JSON-decoding
        every row's full before/after payload.
        """
        rows = self._db.conn.execute(
            "SELECT DISTINCT target_schema, target_table FROM app.audit_log "
            "WHERE operation_id = ? AND target_id IS NOT NULL",
            [operation_id],
        ).fetchall()
        return [(str(schema or ""), str(table or "")) for schema, table in rows]

    def _cascade_blockers(
        self, operation_id: str, liveness: _UndoLiveness
    ) -> list[str]:
        """Operation ids that modified this op's rows *after* it, newest first.

        A later operation blocks only if it is a still-live *forward* mutation:
        undo rows (``is_undo=TRUE``) restore prior state rather than introduce a
        conflicting change, and a forward op whose effect is *currently* undone no
        longer conflicts. Excluding both is what makes the documented walk — "undo
        the blocker, then the original undoes cleanly" — actually work.

        "Currently undone" is net liveness, not "was ever undone": after a
        blocker is round-tripped (undo then undo-the-undo) its effect is live
        again and it must block once more, so the live check is delegated to
        :meth:`_UndoLiveness.is_undone` rather than a "ever appears in
        ``undoes_operation_id``" subquery.

        Ordered by ``rowid`` (monotonic, append-only audit log) rather than
        ``occurred_at`` so sub-second sequential operations order deterministically.
        """
        rows = self._db.conn.execute(
            """
            WITH op_rows AS (
                SELECT target_schema, target_table, target_id, rowid
                  FROM app.audit_log
                 WHERE operation_id = ?
            ),
            boundary AS (SELECT MAX(rowid) AS r FROM op_rows)
            SELECT a.operation_id, MAX(a.rowid) AS latest
              FROM app.audit_log a
              JOIN (
                  SELECT DISTINCT target_schema, target_table, target_id
                    FROM op_rows WHERE target_id IS NOT NULL
              ) t
                ON a.target_schema = t.target_schema
               AND a.target_table = t.target_table
               AND a.target_id = t.target_id
             WHERE a.operation_id <> ?
               AND a.rowid > (SELECT r FROM boundary)
               AND a.is_undo = FALSE
             GROUP BY a.operation_id
             ORDER BY latest DESC
            """,
            [operation_id, operation_id],
        ).fetchall()
        return [str(r[0]) for r in rows if not liveness.is_undone(str(r[0]))]
