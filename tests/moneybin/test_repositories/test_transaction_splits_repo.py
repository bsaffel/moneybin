"""Audited writes to app.transaction_splits via TransactionSplitsRepo (REC-PR3).

PK split_id. Full-row audit (DN1); ``clear`` emits one ``split.remove`` per
deleted row (DN3) so undo can reinsert each split. ``delete`` on a missing
split raises LookupError. category_id resolution stays in the service.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.repositories.transaction_splits_repo import TransactionSplitsRepo


def _insert(
    repo: TransactionSplitsRepo,
    *,
    split_id: str,
    ord: int = 0,
    amount: str = "10.00",
    category: str | None = "Food",
) -> None:
    repo.insert(
        split_id=split_id,
        transaction_id="txn_1",
        amount=Decimal(amount),
        category=category,
        subcategory=None,
        category_id=None,
        note=None,
        ord=ord,
        actor="cli",
    )


class TestInsert:
    """Inserting a split audits the full new row (amount as str)."""

    def test_emits_full_row_audit(self, db: Database) -> None:
        repo = TransactionSplitsRepo(db)
        event = repo.insert(
            split_id="s1",
            transaction_id="txn_1",
            amount=Decimal("10.00"),
            category="Food",
            subcategory=None,
            category_id=None,
            note="lunch",
            ord=0,
            actor="cli",
        )
        assert event.action == "split.add"
        assert event.before_value is None
        after = event.after_value
        assert after is not None
        assert after["split_id"] == "s1"
        assert after["transaction_id"] == "txn_1"
        assert after["amount"] == "10.00"  # Decimal serialized to str
        assert after["category"] == "Food"
        assert after["ord"] == 0
        assert after["created_by"] == "cli"
        assert "created_at" in after

    def test_row_persisted(self, db: Database) -> None:
        repo = TransactionSplitsRepo(db)
        _insert(repo, split_id="s1")
        row = db.execute(
            "SELECT amount FROM app.transaction_splits WHERE split_id = ?", ["s1"]
        ).fetchone()
        assert row == (Decimal("10.00"),)


class TestDelete:
    """Deleting a split audits the full prior row; missing split raises."""

    def test_emits_full_before_after_none(self, db: Database) -> None:
        repo = TransactionSplitsRepo(db)
        _insert(repo, split_id="s1")
        event = repo.delete(split_id="s1", actor="cli")
        assert event.action == "split.remove"
        assert event.after_value is None
        assert event.before_value is not None
        assert event.before_value["split_id"] == "s1"
        assert event.before_value["amount"] == "10.00"

    def test_missing_raises_lookup_error(self, db: Database) -> None:
        repo = TransactionSplitsRepo(db)
        with pytest.raises(LookupError):
            repo.delete(split_id="missing", actor="cli")


class TestClear:
    """Clearing a transaction's splits emits one split.remove per row (DN3)."""

    def test_emits_one_remove_per_row(self, db: Database) -> None:
        repo = TransactionSplitsRepo(db)
        _insert(repo, split_id="s1", ord=0, amount="10.00")
        _insert(repo, split_id="s2", ord=1, amount="20.00")
        events = repo.clear(transaction_id="txn_1", actor="cli")
        assert len(events) == 2
        assert all(e.action == "split.remove" for e in events)
        assert all(e.after_value is None for e in events)
        removed_ids = {e.before_value["split_id"] for e in events if e.before_value}
        assert removed_ids == {"s1", "s2"}

    def test_removes_all_rows(self, db: Database) -> None:
        repo = TransactionSplitsRepo(db)
        _insert(repo, split_id="s1", ord=0)
        _insert(repo, split_id="s2", ord=1)
        repo.clear(transaction_id="txn_1", actor="cli")
        rows = db.execute(
            "SELECT 1 FROM app.transaction_splits WHERE transaction_id = ?", ["txn_1"]
        ).fetchall()
        assert rows == []

    def test_empty_returns_no_events(self, db: Database) -> None:
        repo = TransactionSplitsRepo(db)
        assert repo.clear(transaction_id="txn_empty", actor="cli") == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
