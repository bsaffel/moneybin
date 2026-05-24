"""Tests for AuditService — unified audit log emission and query."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.services.audit_service import AuditEvent, AuditService


@pytest.fixture()
def audit_service(db: Database) -> AuditService:
    return AuditService(db)


class TestRecordAuditEvent:
    """Insert path: row content, generated id, context, parent linkage."""

    def test_writes_row_with_actor_action_target(
        self, audit_service: AuditService
    ) -> None:
        event = audit_service.record_audit_event(
            action="note.add",
            target=("app", "transaction_notes", "txn_abc"),
            before=None,
            after={"note_id": "n1", "text": "hello"},
            actor="cli",
        )
        assert isinstance(event, AuditEvent)
        assert event.actor == "cli"
        assert event.action == "note.add"
        assert event.target_table == "transaction_notes"
        assert event.target_id == "txn_abc"
        assert event.before_value is None
        assert event.after_value == {"note_id": "n1", "text": "hello"}

    def test_returns_event_with_generated_audit_id(
        self, audit_service: AuditService
    ) -> None:
        event = audit_service.record_audit_event(
            action="tag.add",
            target=("app", "transaction_tags", "txn_x"),
            before=None,
            after={"tag": "tax:business"},
            actor="mcp",
        )
        assert event.audit_id and len(event.audit_id) == 32

    def test_idempotent_op_marks_context_noop(
        self, audit_service: AuditService
    ) -> None:
        event = audit_service.record_audit_event(
            action="tag.add",
            target=("app", "transaction_tags", "txn_x"),
            before={"tag": "foo"},
            after={"tag": "foo"},
            actor="cli",
            context={"noop": True},
        )
        assert (event.context_json or {}).get("noop") is True

    def test_parent_audit_id_chain(self, audit_service: AuditService) -> None:
        parent = audit_service.record_audit_event(
            action="tag.rename",
            target=("app", "transaction_tags", None),
            before={"old_tag": "foo"},
            after={"new_tag": "bar", "row_count": 3},
            actor="cli",
        )
        child = audit_service.record_audit_event(
            action="tag.rename_row",
            target=("app", "transaction_tags", "txn_x"),
            before={"tag": "foo"},
            after={"tag": "bar"},
            actor="cli",
            parent_audit_id=parent.audit_id,
        )
        assert child.parent_audit_id == parent.audit_id


class TestQueryHelpers:
    """Read path: filtered list_events and chain_for parent/children."""

    def test_list_events_filters_by_actor(self, audit_service: AuditService) -> None:
        audit_service.record_audit_event(
            action="note.add",
            target=("app", "transaction_notes", "t1"),
            before=None,
            after={"x": 1},
            actor="cli",
        )
        audit_service.record_audit_event(
            action="note.add",
            target=("app", "transaction_notes", "t2"),
            before=None,
            after={"x": 2},
            actor="mcp",
        )
        cli_events = audit_service.list_events(actor="cli")
        assert len(cli_events) == 1
        assert cli_events[0].actor == "cli"

    def test_list_events_filters_by_target(self, audit_service: AuditService) -> None:
        audit_service.record_audit_event(
            action="note.add",
            target=("app", "transaction_notes", "t1"),
            before=None,
            after={},
            actor="cli",
        )
        audit_service.record_audit_event(
            action="tag.add",
            target=("app", "transaction_tags", "t1"),
            before=None,
            after={},
            actor="cli",
        )
        events = audit_service.list_events(target_id="t1")
        assert len(events) == 2

    def test_chain_for_returns_parent_and_children(
        self, audit_service: AuditService
    ) -> None:
        parent = audit_service.record_audit_event(
            action="tag.rename",
            target=("app", "transaction_tags", None),
            before={"old": "a"},
            after={"new": "b"},
            actor="cli",
        )
        for txn in ("t1", "t2"):
            audit_service.record_audit_event(
                action="tag.rename_row",
                target=("app", "transaction_tags", txn),
                before={"tag": "a"},
                after={"tag": "b"},
                actor="cli",
                parent_audit_id=parent.audit_id,
            )
        chain = audit_service.chain_for(parent.audit_id)
        assert len(chain) == 3
        assert chain[0].audit_id == parent.audit_id


class TestEventsForTransaction:
    """events_for_transaction finds a transaction's events despite row-grain ids."""

    def test_includes_child_rows_by_captured_transaction_id(self, db: Database) -> None:
        # Row-grain target_id keys a note/tag row by its own PK, not the parent
        # transaction — so the per-transaction view must match the transaction_id
        # captured in the row image, not just target_id.
        from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
        from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
        from moneybin.services.mutation_context import operation

        with operation():
            TransactionNotesRepo(db).add(
                transaction_id="txnA", note_id="nA", text="x", actor="cli"
            )
            TransactionTagsRepo(db).add(transaction_id="txnA", tag="food", actor="cli")
        with operation():
            TransactionNotesRepo(db).add(
                transaction_id="txnB", note_id="nB", text="y", actor="cli"
            )
        events = AuditService(db).events_for_transaction("txnA")
        assert {e.action for e in events} == {"note.add", "tag.add"}  # txnB excluded


class TestEventOrdering:
    """events_for_operation replays in write (rowid) order, not random audit_id."""

    def test_orders_by_rowid_not_random_audit_id(self, db: Database) -> None:
        # Every row in one operation shares occurred_at (DuckDB CURRENT_TIMESTAMP is
        # transaction-stable), so the ORDER BY tiebreaker alone decides replay order.
        # audit_id is a random uuid4 — using it as the tiebreaker scrambles the
        # order. Insert with audit_ids that sort opposite to write order and assert
        # we get write (rowid) order back, which the undo consumer reverses.
        ts = "2026-05-24 00:00:00"
        for audit_id, target in (("zc", "t1"), ("zb", "t2"), ("za", "t3")):
            db.execute(
                "INSERT INTO app.audit_log "
                "(audit_id, occurred_at, actor, action, target_schema, "
                " target_table, target_id, operation_id) "
                "VALUES (?, ?, 'cli', 'tag.add', 'app', 'transaction_tags', ?, ?)",
                [audit_id, ts, target, "op_order"],
            )
        events = AuditService(db).events_for_operation("op_order")
        assert [e.target_id for e in events] == ["t1", "t2", "t3"]
