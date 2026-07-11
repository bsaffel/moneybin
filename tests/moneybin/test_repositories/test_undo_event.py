"""Generic inverse synthesis: ``BaseRepo.undo_event`` (REC-PR3 Phase 4).

The undo consumer reverses any audited ``app.*`` mutation from its full-row
``before``/``after`` capture alone (Req 4/5): INSERT→delete, DELETE→reinsert,
UPDATE→restore-before. One generic implementation on ``BaseRepo`` serves every
repo — including ``MatchDecisionsRepo``, whose ``match_signals`` JSON column the
reverser binds natively (no per-repo override). The undo is itself audited
(``is_undo``/``undoes_operation_id``) so it is undoable in turn.
"""

from __future__ import annotations

import json

import pytest

from moneybin.database import Database
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
from moneybin.services.audit_service import AuditEvent


def _seed_match(repo: MatchDecisionsRepo, match_id: str = "m1") -> AuditEvent:
    return repo.insert(
        match_id=match_id,
        source_transaction_id_a="txn_a",
        source_type_a="csv",
        source_origin_a="o_a",
        source_transaction_id_b="txn_b",
        source_type_b="csv",
        source_origin_b="o_b",
        account_id="acct_1",
        confidence_score=0.87,
        match_signals={"date_distance": 0, "description_similarity": 0.87},
        match_status="pending",
        decided_by="auto",
        actor="cli",
    )


class TestUndoOfInsert:
    """Undoing an INSERT deletes the row and audits the reversal."""

    def test_deletes_inserted_row(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        add = repo.add(transaction_id="txn_1", tag="trip", actor="cli")
        repo.undo_event(add, actor="cli")
        row = db.execute(
            "SELECT 1 FROM app.transaction_tags WHERE transaction_id = ? AND tag = ?",
            ["txn_1", "trip"],
        ).fetchone()
        assert row is None

    def test_undo_audit_shape(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        add = repo.add(transaction_id="txn_1", tag="trip", actor="cli")
        undo = repo.undo_event(add, actor="cli")
        assert undo is not None
        assert undo.is_undo is True
        assert undo.undoes_operation_id == add.operation_id
        assert undo.action == "tag.add.undo"
        # The undo's before/after mirror the original (before=after, after=before).
        assert undo.before_value is not None
        assert undo.before_value["tag"] == "trip"
        assert undo.after_value is None
        assert undo.target_id == add.target_id


class TestUndoOfDelete:
    """Undoing a DELETE reinserts the removed row with its original fields."""

    def test_reinserts_removed_row(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        repo.add(transaction_id="txn_1", note_id="n1", text="hi", actor="cli")
        delete = repo.delete(note_id="n1", actor="cli")
        repo.undo_event(delete, actor="cli")
        row = db.execute(
            "SELECT transaction_id, text, author FROM app.transaction_notes "
            "WHERE note_id = ?",
            ["n1"],
        ).fetchone()
        assert row == ("txn_1", "hi", "cli")

    def test_undo_audit_shape(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        repo.add(transaction_id="txn_1", note_id="n1", text="hi", actor="cli")
        delete = repo.delete(note_id="n1", actor="cli")
        undo = repo.undo_event(delete, actor="cli")
        assert undo is not None
        assert undo.is_undo is True
        assert undo.undoes_operation_id == delete.operation_id
        assert undo.action == "note.delete.undo"
        assert undo.after_value is not None and undo.after_value["text"] == "hi"
        assert undo.before_value is None


class TestUndoOfUpdate:
    """Undoing an UPDATE restores every column to its before-image."""

    def test_restores_prior_text(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        repo.add(transaction_id="txn_1", note_id="n1", text="hi", actor="cli")
        edit = repo.edit(note_id="n1", text="bye", actor="cli")
        repo.undo_event(edit, actor="cli")
        row = db.execute(
            "SELECT text FROM app.transaction_notes WHERE note_id = ?", ["n1"]
        ).fetchone()
        assert row == ("hi",)

    def test_undo_audit_shape(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        repo.add(transaction_id="txn_1", note_id="n1", text="hi", actor="cli")
        edit = repo.edit(note_id="n1", text="bye", actor="cli")
        undo = repo.undo_event(edit, actor="cli")
        assert undo is not None
        assert undo.action == "note.edit.undo"
        # before mirrors the original after (text=bye); after mirrors before (hi).
        assert undo.before_value is not None and undo.before_value["text"] == "bye"
        assert undo.after_value is not None and undo.after_value["text"] == "hi"


class TestUndoOfPkChangingUpdate:
    """A pk-changing UPDATE (tag rename) is located by the after-image's pk."""

    def test_restores_old_tag(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        repo.add(transaction_id="txn_1", tag="old", actor="cli")
        rename = repo.rename_row(
            transaction_id="txn_1", old_tag="old", new_tag="new", actor="cli"
        )
        repo.undo_event(rename, actor="cli")
        tags = [
            r[0]
            for r in db.execute(
                "SELECT tag FROM app.transaction_tags WHERE transaction_id = ?",
                ["txn_1"],
            ).fetchall()
        ]
        assert tags == ["old"]


class TestUndoOfNoop:
    """A no-op event (before == after, or both None) writes nothing."""

    def test_returns_none_no_audit(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        before_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
        assert before_count is not None
        same = {"note_id": "n1", "transaction_id": "t", "text": "x"}
        noop = AuditEvent(
            audit_id="a1",
            occurred_at="",
            actor="cli",
            action="note.edit",
            target_schema="app",
            target_table="transaction_notes",
            target_id="t",
            before_value=dict(same),
            after_value=dict(same),
            parent_audit_id=None,
            operation_id="op_x",
        )
        result = repo.undo_event(noop, actor="cli")
        assert result is None
        after_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
        assert after_count == before_count  # no audit row written


class TestUndoOfMatchDecisionGeneric:
    """Generic reverser handles ``match_decisions`` (JSON column) with no override."""

    def test_undo_insert_deletes(self, db: Database) -> None:
        repo = MatchDecisionsRepo(db)
        insert = _seed_match(repo)
        repo.undo_event(insert, actor="cli")
        row = db.execute(
            "SELECT 1 FROM app.match_decisions WHERE match_id = ?", ["m1"]
        ).fetchone()
        assert row is None

    def test_undo_reverse_restores_status_and_signals(self, db: Database) -> None:
        repo = MatchDecisionsRepo(db)
        _seed_match(repo)
        # reverse() only accepts accepted/rejected rows; accept before reversing.
        repo.update_status("m1", status="accepted", decided_by="user", actor="cli")
        reverse = repo.reverse("m1", reversed_by="user", actor="cli")
        repo.undo_event(reverse, actor="cli")
        row = db.execute(
            "SELECT match_status, reversed_at, match_signals "
            "FROM app.match_decisions WHERE match_id = ?",
            ["m1"],
        ).fetchone()
        assert row is not None
        assert row[0] == "accepted"  # restored from 'reversed'
        assert row[1] is None  # reversed_at cleared back to before-image
        assert json.loads(row[2])["description_similarity"] == 0.87


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
