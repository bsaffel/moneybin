"""Audited writes to app.transaction_tags via TransactionTagsRepo (REC-PR3).

Composite PK (transaction_id, tag). Repo primitives audit only REAL mutations
(DN2 — the service keeps idempotency orchestration); full-row before/after (DN1)
so undo can reconstruct. remove on an absent tag raises LookupError.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo


class TestAdd:
    """Adding a tag inserts the row and audits the full new row."""

    def test_emits_full_row_audit(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        event = repo.add(transaction_id="txn_1", tag="trip", actor="cli")
        assert event.action == "tag.add"
        assert event.before_value is None
        after = event.after_value
        assert after is not None
        assert after["transaction_id"] == "txn_1"
        assert after["tag"] == "trip"
        assert after["applied_by"] == "cli"
        assert "applied_at" in after

    def test_row_persisted(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        repo.add(transaction_id="txn_1", tag="trip", actor="cli")
        row = db.execute(
            "SELECT applied_by FROM app.transaction_tags "
            "WHERE transaction_id = ? AND tag = ?",
            ["txn_1", "trip"],
        ).fetchone()
        assert row == ("cli",)


class TestRemove:
    """Removing a tag audits the full prior row; absent tag raises."""

    def test_emits_full_before_after_none(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        repo.add(transaction_id="txn_1", tag="trip", actor="cli")
        event = repo.remove(transaction_id="txn_1", tag="trip", actor="cli")
        assert event.action == "tag.remove"
        assert event.after_value is None
        assert event.before_value is not None
        assert event.before_value["tag"] == "trip"
        assert event.before_value["transaction_id"] == "txn_1"

    def test_row_removed(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        repo.add(transaction_id="txn_1", tag="trip", actor="cli")
        repo.remove(transaction_id="txn_1", tag="trip", actor="cli")
        row = db.execute(
            "SELECT 1 FROM app.transaction_tags WHERE transaction_id = ? AND tag = ?",
            ["txn_1", "trip"],
        ).fetchone()
        assert row is None

    def test_missing_raises_lookup_error(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        with pytest.raises(LookupError):
            repo.remove(transaction_id="txn_1", tag="absent", actor="cli")


class TestRenameRow:
    """Renaming one row's tag audits full rows and chains the parent id."""

    def test_emits_full_before_after_with_parent(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        repo.add(transaction_id="txn_1", tag="old", actor="cli")
        event = repo.rename_row(
            transaction_id="txn_1",
            old_tag="old",
            new_tag="new",
            actor="cli",
            parent_audit_id="parent123",
        )
        assert event.action == "tag.rename_row"
        assert event.before_value is not None and event.before_value["tag"] == "old"
        assert event.after_value is not None and event.after_value["tag"] == "new"
        assert event.parent_audit_id == "parent123"

    def test_tag_changed(self, db: Database) -> None:
        repo = TransactionTagsRepo(db)
        repo.add(transaction_id="txn_1", tag="old", actor="cli")
        repo.rename_row(
            transaction_id="txn_1", old_tag="old", new_tag="new", actor="cli"
        )
        rows = db.execute(
            "SELECT tag FROM app.transaction_tags WHERE transaction_id = ?", ["txn_1"]
        ).fetchall()
        assert rows == [("new",)]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
