"""Audited writes to app.transaction_notes via TransactionNotesRepo (REC-PR3).

Repo-ifies the notes mutations that transaction_service previously did with raw
SQL (Invariant 10), capturing FULL rows in before/after (DN1) so the undo
consumer can reconstruct them. Missing-note edit/delete raise LookupError to
preserve transaction_service's existing contract.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo


def _seed_note(repo: TransactionNotesRepo, note_id: str = "n1") -> None:
    repo.add(transaction_id="txn_1", note_id=note_id, text="hi", actor="cli")


class TestAdd:
    """Adding a note inserts the row and audits the full new row."""

    def test_emits_full_row_audit(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        event = repo.add(transaction_id="txn_1", note_id="n1", text="hi", actor="cli")
        assert event.action == "note.add"
        assert event.before_value is None
        after = event.after_value
        assert after is not None
        assert after["note_id"] == "n1"
        assert after["transaction_id"] == "txn_1"
        assert after["text"] == "hi"
        assert after["author"] == "cli"
        assert "created_at" in after  # full row, not partial

    def test_row_persisted(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        repo.add(transaction_id="txn_1", note_id="n1", text="hi", actor="cli")
        row = db.execute(
            "SELECT text, author FROM app.transaction_notes WHERE note_id = ?", ["n1"]
        ).fetchone()
        assert row == ("hi", "cli")


class TestEdit:
    """Editing a note audits full prior/new rows; missing note raises."""

    def test_emits_full_before_and_after(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        _seed_note(repo)
        event = repo.edit(note_id="n1", text="bye", actor="cli")
        assert event.action == "note.edit"
        assert event.before_value is not None and event.before_value["text"] == "hi"
        assert event.after_value is not None and event.after_value["text"] == "bye"
        # full row both sides (DN1/DN3) — not just {text}
        assert event.before_value["note_id"] == "n1"
        assert event.after_value["author"] == "cli"

    def test_missing_raises_lookup_error(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        with pytest.raises(LookupError):
            repo.edit(note_id="missing", text="x", actor="cli")


class TestDelete:
    """Deleting a note audits the full prior row; missing note raises."""

    def test_emits_full_before_after_none(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        _seed_note(repo)
        event = repo.delete(note_id="n1", actor="cli")
        assert event.action == "note.delete"
        assert event.after_value is None
        assert event.before_value is not None
        assert event.before_value["note_id"] == "n1"
        assert event.before_value["text"] == "hi"
        assert event.before_value["author"] == "cli"

    def test_row_removed(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        _seed_note(repo)
        repo.delete(note_id="n1", actor="cli")
        row = db.execute(
            "SELECT 1 FROM app.transaction_notes WHERE note_id = ?", ["n1"]
        ).fetchone()
        assert row is None

    def test_missing_raises_lookup_error(self, db: Database) -> None:
        repo = TransactionNotesRepo(db)
        with pytest.raises(LookupError):
            repo.delete(note_id="missing", actor="cli")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
