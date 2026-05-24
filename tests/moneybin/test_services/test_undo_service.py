"""``UndoService`` — undo / history / get over audited operations (REC-PR3 Phase 6).

Exercises the four undo outcomes the contract names (round-trip, already-undone,
cascade-blocked, not-found) plus the two read surfaces, against a real DB and
real repos (no mocks). Operations are built by wrapping repo calls in
``operation()`` so they share an ``operation_id``, exactly as the MCP/CLI seam does.
"""

from __future__ import annotations

import json

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService


def _note_and_tag_op(db: Database) -> str:
    """One operation that adds a note and a tag to txn_1; returns its op id."""
    with operation() as op:
        TransactionNotesRepo(db).add(
            transaction_id="txn_1", note_id="n1", text="hi", actor="cli"
        )
        TransactionTagsRepo(db).add(transaction_id="txn_1", tag="trip", actor="cli")
    return op


def _tag_op(db: Database, tag: str) -> str:
    with operation() as op:
        TransactionTagsRepo(db).add(transaction_id="txn_1", tag=tag, actor="cli")
    return op


def _note_op(db: Database, note_id: str = "n1", text: str = "hi") -> str:
    """Add note ``note_id`` to txn_1; returns its op id."""
    with operation() as op:
        TransactionNotesRepo(db).add(
            transaction_id="txn_1", note_id=note_id, text=text, actor="cli"
        )
    return op


def _edit_note_op(db: Database, note_id: str = "n1", text: str = "edited") -> str:
    """Edit note ``note_id`` — a second op on the same row, so it blocks the add.

    A real cascade blocker now that ``target_id`` is the entity PK (row-grain).
    """
    with operation() as op:
        TransactionNotesRepo(db).edit(note_id=note_id, text=text, actor="cli")
    return op


class TestUndo:
    """undo(operation_id) reverses every row in the operation as a unit."""

    def test_round_trip_removes_all_rows(self, db: Database) -> None:
        op = _note_and_tag_op(db)
        result = UndoService(db).undo(op, actor="cli")
        assert result.undone_operation_id == op
        assert result.reversed_row_count == 2
        assert set(result.tables) == {"transaction_notes", "transaction_tags"}
        notes = db.execute("SELECT COUNT(*) FROM app.transaction_notes").fetchone()
        tags = db.execute("SELECT COUNT(*) FROM app.transaction_tags").fetchone()
        assert notes == (0,) and tags == (0,)

    def test_undo_is_itself_undoable(self, db: Database) -> None:
        op = _note_and_tag_op(db)
        undo_result = UndoService(db).undo(op, actor="cli")
        # Undoing the undo restores the original rows.
        UndoService(db).undo(undo_result.undo_operation_id, actor="cli")
        notes = db.execute("SELECT COUNT(*) FROM app.transaction_notes").fetchone()
        tags = db.execute("SELECT COUNT(*) FROM app.transaction_tags").fetchone()
        assert notes == (1,) and tags == (1,)

    def test_not_found_raises(self, db: Database) -> None:
        with pytest.raises(UserError) as exc:
            UndoService(db).undo("op_does_not_exist", actor="cli")
        assert exc.value.code == error_codes.UNDO_OPERATION_NOT_FOUND

    def test_already_undone_raises(self, db: Database) -> None:
        op = _note_and_tag_op(db)
        UndoService(db).undo(op, actor="cli")
        with pytest.raises(UserError) as exc:
            UndoService(db).undo(op, actor="cli")
        assert exc.value.code == error_codes.UNDO_ALREADY_UNDONE

    def test_independent_children_same_txn_do_not_block(self, db: Database) -> None:
        # Two notes on the same transaction are different rows — undoing the first
        # must not be blocked by the second (row-grain cascade: target_id is the
        # entity PK, not the parent transaction_id).
        notes = TransactionNotesRepo(db)
        with operation() as op1:
            notes.add(transaction_id="txn_1", note_id="n1", text="a", actor="cli")
        with operation():
            notes.add(transaction_id="txn_1", note_id="n2", text="b", actor="cli")
        UndoService(db).undo(op1, actor="cli")  # n2 is a different row — no block
        remaining = db.execute(
            "SELECT note_id FROM app.transaction_notes ORDER BY note_id"
        ).fetchall()
        assert remaining == [("n2",)]

    def test_cascade_blocked_lists_blocker(self, db: Database) -> None:
        op1 = _note_op(db)  # add note n1
        op2 = _edit_note_op(db)  # edit the SAME row n1, later → blocks op1
        with pytest.raises(UserError) as exc:
            UndoService(db).undo(op1, actor="cli")
        assert exc.value.code == error_codes.UNDO_CASCADE_BLOCKED
        assert exc.value.recovery_actions is not None
        blockers = [a.arguments["operation_id"] for a in exc.value.recovery_actions]
        assert blockers == [op2]

    def test_cascade_resolves_after_blocker_undone(self, db: Database) -> None:
        op1 = _note_op(db)  # add note n1
        op2 = _edit_note_op(db)  # edit n1 (blocks op1)
        UndoService(db).undo(op2, actor="cli")  # clear the blocker first
        UndoService(db).undo(op1, actor="cli")  # now op1 undoes cleanly
        notes = db.execute("SELECT COUNT(*) FROM app.transaction_notes").fetchone()
        assert notes == (0,)

    def test_can_reundo_after_round_trip(self, db: Database) -> None:
        # op -> undo -> undo-the-undo restores op's effect, so op is LIVE again and
        # must be undoable a second time. "Already undone" is net liveness, not
        # "an undo of op was ever recorded".
        op = _tag_op(db, "a")
        undo1 = UndoService(db).undo(op, actor="cli")  # tag removed
        UndoService(db).undo(undo1.undo_operation_id, actor="cli")  # tag back, op live
        UndoService(db).undo(op, actor="cli")  # must succeed, not raise already_undone
        tags = db.execute("SELECT COUNT(*) FROM app.transaction_tags").fetchone()
        assert tags == (0,)

    def test_cascade_blocks_after_blocker_round_trip(self, db: Database) -> None:
        # op2 modified op1's row after it, then op2 was round-tripped
        # (undo -> undo-the-undo) so op2's effect is LIVE again. undo(op1) must
        # still be blocked by op2 — net liveness, not "op2 was ever undone".
        # Without this, undo(op1) would silently clobber op2's live row.
        op1 = _note_op(db)  # add note n1
        op2 = _edit_note_op(db)  # edit the SAME row n1, later → blocks op1
        undo2 = UndoService(db).undo(op2, actor="cli")  # op2 effect removed
        UndoService(db).undo(undo2.undo_operation_id, actor="cli")  # op2 effect live
        with pytest.raises(UserError) as exc:
            UndoService(db).undo(op1, actor="cli")
        assert exc.value.code == error_codes.UNDO_CASCADE_BLOCKED
        assert exc.value.recovery_actions is not None
        blockers = [a.arguments["operation_id"] for a in exc.value.recovery_actions]
        assert op2 in blockers

    def test_undo_replays_in_reverse_write_order(self, db: Database) -> None:
        # Undo reverses rows in the reverse of their write order (so a future
        # parent-then-child insert undoes child-first). All rows in one operation
        # share occurred_at, so order hinges on the tiebreaker; the old code sorted
        # by the random audit_id. Force audit_id order opposite to write (rowid)
        # order and assert undo still replays newest-first.
        repo = TransactionTagsRepo(db)
        with operation() as op:
            for tag in ("a1", "a2", "a3"):
                repo.add(transaction_id="txn_1", tag=tag, actor="cli")
        rowids = [
            r[0]
            for r in db.execute(
                "SELECT rowid FROM app.audit_log WHERE operation_id = ? ORDER BY rowid",
                [op],
            ).fetchall()
        ]
        for rowid, audit_id in zip(rowids, ("z3", "z2", "z1"), strict=True):
            db.execute(
                "UPDATE app.audit_log SET audit_id = ? WHERE rowid = ?",
                [audit_id, rowid],
            )
        undo = UndoService(db).undo(op, actor="cli")
        undo_rows = db.execute(
            "SELECT before_value FROM app.audit_log "
            "WHERE operation_id = ? AND is_undo = TRUE ORDER BY rowid",
            [undo.undo_operation_id],
        ).fetchall()
        replayed = [json.loads(r[0])["tag"] for r in undo_rows]
        assert replayed == ["a3", "a2", "a1"]

    def test_marker_only_operation_refuses(self, db: Database) -> None:
        # A tag.rename matching zero transactions audits only the parent marker
        # (target_id=None) — no row mutations. Undo must refuse, not return a
        # phantom undo_operation_id that has no audit rows and can't be queried.
        with operation() as op:
            AuditService(db).record_audit_event(
                action="tag.rename",
                target=("app", "transaction_tags", None),
                before={"old_tag": "ghost"},
                after={"new_tag": "x", "row_count": 0},
                actor="cli",
            )
        with pytest.raises(UserError) as exc:
            UndoService(db).undo(op, actor="cli")
        assert exc.value.code == error_codes.RECOVERY_NO_PATH

    def test_cascade_ignores_same_table_different_schema(self, db: Database) -> None:
        # A later forward op on a same-named table in a DIFFERENT schema must not
        # block: the blocker join keys on (schema, table, id), like
        # _unresolvable_tables — not table+id alone.
        op = _tag_op(db, "a")  # app.transaction_tags, target_id "txn_1:a"
        with operation():
            AuditService(db).record_audit_event(
                action="manual.create",
                target=(
                    "raw",
                    "transaction_tags",
                    "txn_1:a",
                ),  # same table+id, diff schema
                before=None,
                after={"x": 1},
                actor="cli",
            )
        UndoService(db).undo(op, actor="cli")  # raw-schema row is not a blocker
        tags = db.execute("SELECT COUNT(*) FROM app.transaction_tags").fetchone()
        assert tags == (0,)

    def test_all_noop_rows_refuse(self, db: Database) -> None:
        # An operation whose every row is a no-op (before == after, e.g. a legacy
        # idempotent tag.add) reverses nothing. Refuse rather than mint a phantom
        # undo_operation_id that carries no audit rows.
        same = json.dumps({"transaction_id": "txn_1", "tag": "trip"})
        db.execute(
            "INSERT INTO app.audit_log "
            "(audit_id, actor, action, target_schema, target_table, target_id, "
            " before_value, after_value, operation_id) "
            "VALUES ('noop1','cli','tag.add','app','transaction_tags','txn_1', "
            " ?, ?, 'op_noop')",
            [same, same],
        )
        with pytest.raises(UserError) as exc:
            UndoService(db).undo("op_noop", actor="cli")
        assert exc.value.code == error_codes.RECOVERY_NO_PATH
        undo_rows = db.execute(
            "SELECT COUNT(*) FROM app.audit_log WHERE is_undo = TRUE"
        ).fetchone()
        assert undo_rows == (0,)  # no phantom undo operation written

    def test_partial_update_before_refuses(self, db: Database) -> None:
        # A legacy note.edit captured a partial before_value (note_id/text only).
        # The UPDATE-restore branch would silently partial-restore (leaving
        # transaction_id/author/created_at at current values); it must refuse.
        full_after = {
            "note_id": "n1",
            "transaction_id": "txn_1",
            "text": "new",
            "author": "cli",
            "created_at": "2026-05-24 00:00:00",
        }
        db.execute(
            "INSERT INTO app.audit_log "
            "(audit_id, actor, action, target_schema, target_table, target_id, "
            " before_value, after_value, operation_id) "
            "VALUES ('legacy_upd','cli','note.edit','app','transaction_notes','n1', "
            " ?, ?, 'op_legacy_upd')",
            [json.dumps({"note_id": "n1", "text": "old"}), json.dumps(full_after)],
        )
        with pytest.raises(UserError) as exc:
            UndoService(db).undo("op_legacy_upd", actor="cli")
        assert exc.value.code == error_codes.RECOVERY_NO_PATH

    def test_partial_legacy_row_refuses_with_recovery_no_path(
        self, db: Database
    ) -> None:
        # A pre-PR note.delete captured only a partial before_value (no
        # transaction_id / created_at). undo_event's re-INSERT branch would hit a
        # raw DuckDB NOT NULL; instead it must refuse cleanly with RECOVERY_NO_PATH.
        db.execute(
            "INSERT INTO app.audit_log "
            "(audit_id, actor, action, target_schema, target_table, target_id, "
            " before_value, after_value, operation_id) "
            "VALUES ('legacy1','cli','note.delete','app','transaction_notes','n1', "
            " ?, NULL, 'op_legacy')",
            [json.dumps({"note_id": "n1", "text": "hi", "author": "cli"})],
        )
        with pytest.raises(UserError) as exc:
            UndoService(db).undo("op_legacy", actor="cli")
        assert exc.value.code == error_codes.RECOVERY_NO_PATH

    def test_unresolvable_with_blocker_still_lists_blocker(self, db: Database) -> None:
        # An op that touches BOTH a raw table (unresolvable) AND an app row a later
        # op modified (blocker) refuses with RECOVERY_NO_PATH but must still surface
        # the blocker in recovery_actions, not dead-end the agent.
        with operation() as op1:
            TransactionNotesRepo(db).add(
                transaction_id="txn_1", note_id="n1", text="a", actor="cli"
            )
            AuditService(db).record_audit_event(
                action="manual.create",
                target=("raw", "manual_transactions", "imp_1"),
                before=None,
                after={"row_count": 1},
                actor="cli",
            )
        op2 = _edit_note_op(db)  # edits n1 → blocker on op1's note row
        with pytest.raises(UserError) as exc:
            UndoService(db).undo(op1, actor="cli")
        assert exc.value.code == error_codes.RECOVERY_NO_PATH
        assert exc.value.recovery_actions is not None
        blockers = [a.arguments["operation_id"] for a in exc.value.recovery_actions]
        assert op2 in blockers

    def test_raw_target_is_not_undoable(self, db: Database) -> None:
        # A manual.create writes an audit row targeting raw.manual_transactions,
        # which no repo owns — undo must report recovery_no_path, not crash.
        with operation() as op:
            AuditService(db).record_audit_event(
                action="manual.create",
                target=("raw", "manual_transactions", "imp_1"),
                before=None,
                after={"row_count": 2},
                actor="cli",
            )
        with pytest.raises(UserError) as exc:
            UndoService(db).undo(op, actor="cli")
        assert exc.value.code == error_codes.RECOVERY_NO_PATH

    def test_rename_parent_marker_is_skipped(self, db: Database) -> None:
        # A cross-row tag.rename parent (target_id=None) is a marker, not a row
        # mutation — undo reverses only the per-row children.
        tags = TransactionTagsRepo(db)
        with operation():
            tags.add(transaction_id="txn_1", tag="old", actor="cli")
        with operation() as rename_op:
            parent = AuditService(db).record_audit_event(
                action="tag.rename",
                target=("app", "transaction_tags", None),
                before={"old_tag": "old"},
                after={"new_tag": "new", "row_count": 1},
                actor="cli",
            )
            tags.rename_row(
                transaction_id="txn_1",
                old_tag="old",
                new_tag="new",
                actor="cli",
                parent_audit_id=parent.audit_id,
                in_outer_txn=False,
            )
        result = UndoService(db).undo(rename_op, actor="cli")
        assert result.reversed_row_count == 1  # parent skipped, child reversed
        rows = db.execute(
            "SELECT tag FROM app.transaction_tags WHERE transaction_id = ?", ["txn_1"]
        ).fetchall()
        assert rows == [("old",)]


class TestHistory:
    """history() groups audit rows by operation, newest first, with undoability."""

    def test_groups_operations_newest_first(self, db: Database) -> None:
        op1 = _tag_op(db, "a")
        op2 = _note_and_tag_op(db)
        ops = UndoService(db).history()
        ids = [o.operation_id for o in ops]
        assert ids == [op2, op1]
        op2_summary = ops[0]
        assert op2_summary.row_count == 2
        assert set(op2_summary.tables) == {"transaction_notes", "transaction_tags"}
        assert op2_summary.can_undo is True

    def test_undone_operation_marked_not_undoable(self, db: Database) -> None:
        op = _tag_op(db, "a")
        UndoService(db).undo(op, actor="cli")
        summary = next(o for o in UndoService(db).history() if o.operation_id == op)
        assert summary.can_undo is False

    def test_excludes_undo_operations_by_default(self, db: Database) -> None:
        op = _tag_op(db, "a")
        undo = UndoService(db).undo(op, actor="cli")
        default_ids = [o.operation_id for o in UndoService(db).history()]
        assert undo.undo_operation_id not in default_ids
        all_ids = [o.operation_id for o in UndoService(db).history(include_undone=True)]
        assert undo.undo_operation_id in all_ids

    def test_filters_by_actor(self, db: Database) -> None:
        _tag_op(db, "a")  # actor="cli"
        with operation():
            TransactionTagsRepo(db).add(transaction_id="txn_1", tag="b", actor="mcp")
        cli_ops = UndoService(db).history(actor="cli")
        mcp_ops = UndoService(db).history(actor="mcp")
        assert cli_ops and all(o.actor == "cli" for o in cli_ops)
        assert mcp_ops and all(o.actor == "mcp" for o in mcp_ops)

    def test_filters_by_since(self, db: Database) -> None:
        _tag_op(db, "a")
        assert UndoService(db).history(since="2999-01-01") == []  # future → none
        assert UndoService(db).history(since="2000-01-01")  # past → all

    def test_blocked_operation_carries_blockers(self, db: Database) -> None:
        op1 = _note_op(db)  # add note n1
        op2 = _edit_note_op(db)  # edit the same row n1 → blocks op1
        summary = next(o for o in UndoService(db).history() if o.operation_id == op1)
        assert summary.can_undo is False
        assert summary.undo_blocked_by == [op2]


class TestMetrics:
    """undo() records its outcome and reversed-row count to Prometheus."""

    def _outcome(self, outcome: str) -> float:
        from prometheus_client import REGISTRY

        return (
            REGISTRY.get_sample_value("moneybin_audit_undo_total", {"outcome": outcome})
            or 0.0
        )

    def _rows(self) -> float:
        from prometheus_client import REGISTRY

        return (
            REGISTRY.get_sample_value("moneybin_audit_undo_rows_reversed_total") or 0.0
        )

    def test_success_increments_outcome_and_rows(self, db: Database) -> None:
        op = _note_and_tag_op(db)
        before_ok, before_rows = self._outcome("success"), self._rows()
        UndoService(db).undo(op, actor="cli")
        assert self._outcome("success") - before_ok == 1.0
        assert self._rows() - before_rows == 2.0

    def test_refusal_increments_its_outcome(self, db: Database) -> None:
        before = self._outcome("not_found")
        with pytest.raises(UserError):
            UndoService(db).undo("op_missing", actor="cli")
        assert self._outcome("not_found") - before == 1.0

    def test_in_loop_recovery_no_path_increments_outcome(self, db: Database) -> None:
        # A partial-capture row trips _require_capture INSIDE the reversal loop;
        # that failure path must still record an outcome, not vanish from metrics.
        import json

        db.execute(
            "INSERT INTO app.audit_log "
            "(audit_id, actor, action, target_schema, target_table, target_id, "
            " before_value, after_value, operation_id) "
            "VALUES ('legacy_m','cli','note.delete','app','transaction_notes','n9', "
            " ?, NULL, 'op_legacy_metric')",
            [json.dumps({"note_id": "n9", "text": "hi", "author": "cli"})],
        )
        before = self._outcome("no_path")
        with pytest.raises(UserError):
            UndoService(db).undo("op_legacy_metric", actor="cli")
        assert self._outcome("no_path") - before == 1.0


class TestGet:
    """get() returns full before/after for each row, with undoability flags."""

    def test_returns_events_and_can_undo(self, db: Database) -> None:
        op = _note_and_tag_op(db)
        detail = UndoService(db).get(op)
        assert detail.operation_id == op
        assert len(detail.events) == 2
        assert detail.can_undo is True
        actions = {e.action for e in detail.events}
        assert actions == {"note.add", "tag.add"}

    def test_not_found_raises(self, db: Database) -> None:
        with pytest.raises(UserError) as exc:
            UndoService(db).get("op_missing")
        assert exc.value.code == error_codes.UNDO_OPERATION_NOT_FOUND

    def test_all_noop_not_undoable(self, db: Database) -> None:
        # An all-noop operation (every row before==after) is refused by undo();
        # get() must agree (can_undo=False), not advertise an undo that fails.
        same = json.dumps({"transaction_id": "txn_1", "tag": "trip"})
        db.execute(
            "INSERT INTO app.audit_log "
            "(audit_id, actor, action, target_schema, target_table, target_id, "
            " before_value, after_value, operation_id) "
            "VALUES ('noopg','cli','tag.add','app','transaction_tags','txn_1:trip', "
            " ?, ?, 'op_noopg')",
            [same, same],
        )
        assert UndoService(db).get("op_noopg").can_undo is False

    def test_marker_only_not_undoable(self, db: Database) -> None:
        # A marker-only operation (tag.rename matching zero rows) is refused by
        # undo(); get() must agree (can_undo=False), not advertise an undo that
        # would immediately fail.
        with operation() as op:
            AuditService(db).record_audit_event(
                action="tag.rename",
                target=("app", "transaction_tags", None),
                before={"old_tag": "ghost"},
                after={"new_tag": "x", "row_count": 0},
                actor="cli",
            )
        detail = UndoService(db).get(op)
        assert detail.can_undo is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
