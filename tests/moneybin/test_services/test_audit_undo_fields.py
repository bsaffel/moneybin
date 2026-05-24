"""is_undo + undoes_operation_id threading on app.audit_log (REC-PR3 Phase 1).

The undo consumer (REC-PR3) writes audit rows that mark themselves as undos.
This phase only threads the two columns through AuditService: a normal mutation
records ``is_undo=False`` / ``undoes_operation_id=None``; an undo emission sets
both. Round-trips through ``list_events`` so the read path carries them too.

Runs against the ``db`` fixture (fresh-install schema path); the existing-DB
upgrade is covered by ``test_migration_v024``.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.services.audit_service import AuditService


@pytest.fixture()
def audit(db: Database) -> AuditService:
    return AuditService(db)


def _undone_op() -> str:
    return "op_" + "a" * 32


class TestDefaultMutation:
    """A normal mutation is not an undo."""

    def test_returned_event_defaults_to_not_undo(self, audit: AuditService) -> None:
        event = audit.record_audit_event(
            action="tag.add",
            target=("app", "transaction_tags", "txn_1"),
            before=None,
            after={"tag": "x"},
            actor="cli",
        )
        assert event.is_undo is False
        assert event.undoes_operation_id is None

    def test_persisted_row_defaults_to_not_undo(
        self, db: Database, audit: AuditService
    ) -> None:
        audit.record_audit_event(
            action="tag.add",
            target=("app", "transaction_tags", "txn_1"),
            before=None,
            after={"tag": "x"},
            actor="cli",
        )
        row = db.execute(
            "SELECT is_undo, undoes_operation_id FROM app.audit_log"
        ).fetchone()
        assert row == (False, None)


class TestUndoEmission:
    """An undo emission sets is_undo + undoes_operation_id."""

    def test_returned_event_carries_undo_marker(self, audit: AuditService) -> None:
        undone = _undone_op()
        event = audit.record_audit_event(
            action="tag.remove",
            target=("app", "transaction_tags", "txn_1"),
            before={"tag": "x"},
            after=None,
            actor="cli",
            is_undo=True,
            undoes_operation_id=undone,
        )
        assert event.is_undo is True
        assert event.undoes_operation_id == undone

    def test_round_trips_through_list_events(self, audit: AuditService) -> None:
        undone = _undone_op()
        audit.record_audit_event(
            action="tag.remove",
            target=("app", "transaction_tags", "txn_1"),
            before={"tag": "x"},
            after=None,
            actor="cli",
            is_undo=True,
            undoes_operation_id=undone,
        )
        (event,) = audit.list_events(target_id="txn_1")
        assert event.is_undo is True
        assert event.undoes_operation_id == undone

    def test_to_dict_includes_undo_fields(self, audit: AuditService) -> None:
        undone = _undone_op()
        event = audit.record_audit_event(
            action="tag.remove",
            target=("app", "transaction_tags", "txn_1"),
            before={"tag": "x"},
            after=None,
            actor="cli",
            is_undo=True,
            undoes_operation_id=undone,
        )
        d = event.to_dict()
        assert d["is_undo"] is True
        assert d["undoes_operation_id"] == undone


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
