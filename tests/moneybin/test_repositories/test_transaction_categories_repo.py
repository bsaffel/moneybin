"""Tests for ``TransactionCategoriesRepo``.

Covers the user upsert (``set``), maintenance link update (``update_links``),
the precedence-guarded engine upsert (``upsert_guarded``), single-row ``clear``,
and the multi-row ``delete_by_rule``; each pairs its write with a full
before/after audit row (Req 4).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.repositories.transaction_categories_repo import (
    TransactionCategoriesRepo,
)
from tests.moneybin.test_repositories.conftest import audit_rows_for as _audit_rows_for
from tests.moneybin.test_repositories.conftest import metric_for

_metric = metric_for("transaction_categories")


# ---------------------------------------------------------------------------
# set — user upsert (partial column set)
# ---------------------------------------------------------------------------


def test_set_upserts_and_audits(db: Database) -> None:
    repo = TransactionCategoriesRepo(db)
    before_metric = _metric("category.set")

    event = repo.set(
        "txn1",
        category="Dining",
        subcategory="Coffee",
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    assert event.target_id == "txn1"

    row = db.conn.execute(
        "SELECT category, subcategory, categorized_by "
        "FROM app.transaction_categories WHERE transaction_id = ?",
        ["txn1"],
    ).fetchone()
    assert row == ("Dining", "Coffee", "user")

    audit = _audit_rows_for(db, "txn1")
    assert len(audit) == 1
    assert audit[0][0] == "category.set"
    assert audit[0][4] is None  # before
    after = json.loads(audit[0][5])
    assert after["category"] == "Dining"
    assert after["categorized_by"] == "user"
    assert audit[0][6] == "cli"
    assert _metric("category.set") - before_metric == 1.0


def test_set_overwrite_captures_full_before_row(db: Database) -> None:
    repo = TransactionCategoriesRepo(db)
    repo.set(
        "txn2",
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    repo.set(
        "txn2",
        category="Groceries",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    second = _audit_rows_for(db, "txn2")[1]
    assert json.loads(second[4])["category"] == "Dining"  # full prior row
    assert json.loads(second[5])["category"] == "Groceries"


def test_set_preserves_merchant_and_rule_on_user_overwrite(db: Database) -> None:
    # An engine write sets merchant_id/rule_id; a later user `set` (partial
    # columns) must leave those provenance fields intact (existing behavior).
    repo = TransactionCategoriesRepo(db)
    repo.upsert_guarded(
        "txn3",
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="rule",
        merchant_id="m1",
        rule_id="r1",
        confidence=None,
        actor="system",
    )
    repo.set(
        "txn3",
        category="Groceries",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    row = db.conn.execute(
        "SELECT category, categorized_by, merchant_id, rule_id "
        "FROM app.transaction_categories WHERE transaction_id = ?",
        ["txn3"],
    ).fetchone()
    assert row == ("Groceries", "user", "m1", "r1")


def test_update_links_preserves_category_metadata_and_audits(db: Database) -> None:
    repo = TransactionCategoriesRepo(db)
    repo.set(
        "txn-links",
        category="Dining",
        subcategory="Coffee",
        category_id=None,
        categorized_by="user",
        actor="cli",
    )

    event = repo.update_links(
        "txn-links",
        merchant_id="merchant-1",
        rule_id="rule-1",
        actor="script",
    )

    assert event.target_id == "txn-links"
    row = db.conn.execute(
        "SELECT category, subcategory, categorized_by, merchant_id, rule_id "
        "FROM app.transaction_categories WHERE transaction_id = ?",
        ["txn-links"],
    ).fetchone()
    assert row == ("Dining", "Coffee", "user", "merchant-1", "rule-1")
    audit = _audit_rows_for(db, "txn-links")[-1]
    assert audit[0] == "category.set"
    assert json.loads(audit[4])["merchant_id"] is None
    assert json.loads(audit[5])["merchant_id"] == "merchant-1"
    assert audit[6] == "script"


# ---------------------------------------------------------------------------
# upsert_guarded — precedence-guarded engine upsert
# ---------------------------------------------------------------------------


def test_upsert_guarded_writes_when_no_existing_row(db: Database) -> None:
    repo = TransactionCategoriesRepo(db)
    event = repo.upsert_guarded(
        "txn4",
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="ai",
        merchant_id="m9",
        rule_id=None,
        confidence=0.9,
        actor="system",
    )
    assert event is not None
    after = json.loads(_audit_rows_for(db, "txn4")[0][5])
    assert after["merchant_id"] == "m9"
    assert after["categorized_by"] == "ai"


def test_upsert_guarded_skipped_by_lower_priority(db: Database) -> None:
    repo = TransactionCategoriesRepo(db)
    repo.set(
        "txn5",
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    # ai (priority 7) cannot overwrite user (priority 1).
    event = repo.upsert_guarded(
        "txn5",
        category="Groceries",
        subcategory=None,
        category_id=None,
        categorized_by="ai",
        merchant_id=None,
        rule_id=None,
        confidence=None,
        actor="system",
    )
    assert event is None  # precedence-skipped → no mutation, no audit
    row = db.conn.execute(
        "SELECT category, categorized_by FROM app.transaction_categories "
        "WHERE transaction_id = ?",
        ["txn5"],
    ).fetchone()
    assert row == ("Dining", "user")
    # Only the original user set emitted audit; the skipped write did not.
    assert [r[0] for r in _audit_rows_for(db, "txn5")] == ["category.set"]


def test_upsert_guarded_overwrites_lower_priority_existing(db: Database) -> None:
    repo = TransactionCategoriesRepo(db)
    repo.upsert_guarded(
        "txn6",
        category="A",
        subcategory=None,
        category_id=None,
        categorized_by="ai",  # priority 7
        merchant_id=None,
        rule_id=None,
        confidence=None,
        actor="system",
    )
    event = repo.upsert_guarded(
        "txn6",
        category="B",
        subcategory=None,
        category_id=None,
        categorized_by="rule",  # priority 2 — higher authority, overwrites
        merchant_id=None,
        rule_id="r2",
        confidence=None,
        actor="system",
    )
    assert event is not None
    row = db.conn.execute(
        "SELECT category, categorized_by FROM app.transaction_categories "
        "WHERE transaction_id = ?",
        ["txn6"],
    ).fetchone()
    assert row == ("B", "rule")


def test_upsert_guarded_many_preserves_precedence_and_row_grain_audits(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One batched engine write retains each changed row's undo image."""
    repo = TransactionCategoriesRepo(db)
    repo.set(
        "protected",
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    repo.upsert_guarded(
        "replaceable",
        category="Old",
        subcategory=None,
        category_id=None,
        categorized_by="ai",
        merchant_id=None,
        rule_id=None,
        confidence=None,
        actor="system",
    )

    database_calls = 0
    execute = db.execute

    def count_execute(query: str, params: list[Any] | None = None) -> Any:
        nonlocal database_calls
        database_calls += 1
        return execute(query, params)

    monkeypatch.setattr(db, "execute", count_execute)
    written = repo.upsert_guarded_many(
        [
            {
                "transaction_id": "protected",
                "category": "Blocked",
                "subcategory": None,
                "category_id": None,
                "categorized_by": "rule",
                "merchant_id": None,
                "rule_id": "r1",
                "confidence": 1.0,
                "source_type": "internal",
            },
            {
                "transaction_id": "replaceable",
                "category": "New",
                "subcategory": None,
                "category_id": None,
                "categorized_by": "rule",
                "merchant_id": None,
                "rule_id": "r1",
                "confidence": 1.0,
                "source_type": "internal",
            },
            {
                "transaction_id": "new",
                "category": "Fresh",
                "subcategory": None,
                "category_id": None,
                "categorized_by": "rule",
                "merchant_id": "m1",
                "rule_id": "r1",
                "confidence": 1.0,
                "source_type": "internal",
            },
        ],
        actor="system",
    )

    assert written == {"replaceable", "new"}
    # One read of the before image, one guarded multi-row upsert, and one read
    # of after images; the matching row-grain audit insert is one direct
    # multi-row connection write. Per-record persistence would make nine
    # wrapper calls for this three-row batch.
    assert database_calls == 3
    assert db.execute(
        "SELECT category FROM app.transaction_categories WHERE transaction_id = 'protected'"
    ).fetchone() == ("Dining",)
    for transaction_id in written:
        audit = _audit_rows_for(db, transaction_id)[-1]
        assert audit[0] == "category.set"
        assert json.loads(audit[5])["transaction_id"] == transaction_id


def test_upsert_guarded_many_rejects_duplicate_transaction_ids(db: Database) -> None:
    repo = TransactionCategoriesRepo(db)
    categorization = {
        "transaction_id": "duplicate",
        "category": "Dining",
        "subcategory": None,
        "category_id": None,
        "categorized_by": "rule",
        "merchant_id": None,
        "rule_id": "r1",
        "confidence": 1.0,
        "source_type": "internal",
    }

    with pytest.raises(ValueError, match="unique transaction_ids"):
        repo.upsert_guarded_many([categorization, categorization], actor="system")


# ---------------------------------------------------------------------------
# clear + delete_by_rule
# ---------------------------------------------------------------------------


def test_clear_deletes_and_audits(db: Database) -> None:
    repo = TransactionCategoriesRepo(db)
    repo.set(
        "txn7",
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    event = repo.clear("txn7", actor="cli")
    assert event is not None

    gone = db.conn.execute(
        "SELECT 1 FROM app.transaction_categories WHERE transaction_id = ?", ["txn7"]
    ).fetchone()
    assert gone is None
    clear_audit = next(
        r for r in _audit_rows_for(db, "txn7") if r[0] == "category.clear"
    )
    assert json.loads(clear_audit[4])["category"] == "Dining"  # full before
    assert clear_audit[5] is None  # after


def test_clear_noop_returns_none(db: Database) -> None:
    repo = TransactionCategoriesRepo(db)
    assert repo.clear("missing", actor="cli") is None
    assert _audit_rows_for(db, "missing") == []


def test_delete_by_rule_audits_each_deleted_row(db: Database) -> None:
    repo = TransactionCategoriesRepo(db)
    for tid in ("d1", "d2"):
        repo.upsert_guarded(
            tid,
            category="Dining",
            subcategory=None,
            category_id=None,
            categorized_by="rule",
            merchant_id=None,
            rule_id="rX",
            confidence=None,
            actor="system",
        )
    # A user row for a different rule must survive.
    repo.set(
        "d3",
        category="Dining",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )

    events = repo.delete_by_rule("rX", actor="system")
    assert len(events) == 2

    remaining = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_categories"
    ).fetchone()
    assert remaining == (1,)  # only the user row d3 survives
    for tid in ("d1", "d2"):
        clear = next(r for r in _audit_rows_for(db, tid) if r[0] == "category.clear")
        assert clear[5] is None  # after_value


def test_set_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = TransactionCategoriesRepo(db, audit=audit)
    try:
        repo.set(
            "ghost",
            category="X",
            subcategory=None,
            category_id=None,
            categorized_by="user",
            actor="cli",
        )
    except RuntimeError:
        pass
    rows = db.conn.execute(
        "SELECT 1 FROM app.transaction_categories WHERE transaction_id = 'ghost'"
    ).fetchall()
    assert rows == []


# ---------------------------------------------------------------------------
# repoint_transaction — the audit shape a dedup re-key leaves behind
# ---------------------------------------------------------------------------


def test_repoint_audits_the_departure_on_the_old_id(db: Database) -> None:
    """Both keys are named, so the move is visible from the superseded id.

    ``UndoService._cascade_blockers`` joins on exact ``target_id``: an event
    naming only the survivor would let an undo of the original ``category.set``
    delete by the old id, match nothing, and report success.
    """
    repo = TransactionCategoriesRepo(db)
    repo.set(
        "txn_old",
        category="Dining",
        subcategory="Coffee",
        category_id=None,
        categorized_by="user",
        actor="cli",
    )

    events = repo.repoint_transaction(
        old_transaction_id="txn_old",
        new_transaction_id="txn_new",
        actor="system",
    )

    assert [
        (e.target_id, e.before_value is None, e.after_value is None) for e in events
    ] == [
        ("txn_old", False, True),
        ("txn_new", True, False),
    ]


def test_repoint_is_reversible_event_by_event(db: Database) -> None:
    """Replaying the events in reverse write order restores the original row."""
    repo = TransactionCategoriesRepo(db)
    repo.set(
        "txn_old",
        category="Dining",
        subcategory="Coffee",
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    events = repo.repoint_transaction(
        old_transaction_id="txn_old",
        new_transaction_id="txn_new",
        actor="system",
    )

    for event in reversed(events):
        repo.undo_event(event, actor="cli")

    assert db.execute(
        "SELECT transaction_id, category, subcategory, categorized_by "
        "FROM app.transaction_categories"
    ).fetchall() == [("txn_old", "Dining", "Coffee", "user")]


def test_repoint_onto_a_categorized_destination_is_reversible(db: Database) -> None:
    """The tie-moves branch drops the destination row; undo restores both."""
    repo = TransactionCategoriesRepo(db)
    repo.set(
        "txn_old",
        category="Dining",
        subcategory="Coffee",
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    repo.set(
        "txn_new",
        category="Shopping",
        subcategory=None,
        category_id=None,
        categorized_by="user",
        actor="cli",
    )
    events = repo.repoint_transaction(
        old_transaction_id="txn_old",
        new_transaction_id="txn_new",
        actor="system",
    )
    assert db.execute(
        "SELECT transaction_id, category FROM app.transaction_categories"
    ).fetchall() == [("txn_new", "Dining")]

    for event in reversed(events):
        repo.undo_event(event, actor="cli")

    assert db.execute(
        "SELECT transaction_id, category FROM app.transaction_categories "
        "ORDER BY transaction_id"
    ).fetchall() == [("txn_new", "Shopping"), ("txn_old", "Dining")]
