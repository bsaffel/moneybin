"""Tests for ``TransactionIdAliasesRepo``.

The alias map is append-only old_id -> new_id forwarding (M1S Decision 4 /
ADR-015). Each insert pairs with an ``app.audit_log`` row, and the append-only
guard rejects re-aliasing an ``old_transaction_id`` that already forwards.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.transaction_id_aliases_repo import (
    TransactionIdAliasesRepo,
)


def _audit_rows_for(db: Database, target_id: str) -> list[tuple[Any, ...]]:
    return db.conn.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [target_id],
    ).fetchall()


def _metric(action: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_app_mutation_audit_emitted_total",
            {"repository": "transaction_id_aliases", "action": action},
        )
        or 0.0
    )


def _insert(repo: TransactionIdAliasesRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "old_transaction_id": "oldtxn0001",
        "new_transaction_id": "newtxn0001",
        "actor": "system",
    }
    kwargs.update(overrides)
    return repo.insert(**kwargs)


def test_insert_writes_row_and_audit_row(db: Database) -> None:
    repo = TransactionIdAliasesRepo(db)
    before_metric = _metric("transaction_id_alias.insert")

    event = _insert(repo)
    assert event.target_id == "oldtxn0001"

    row = db.conn.execute(
        "SELECT new_transaction_id FROM app.transaction_id_aliases "
        "WHERE old_transaction_id = ?",
        ["oldtxn0001"],
    ).fetchone()
    assert row == ("newtxn0001",)

    audit = _audit_rows_for(db, "oldtxn0001")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor = audit[0]
    assert action == "transaction_id_alias.insert"
    assert (schema, table, target_id) == (
        "app",
        "transaction_id_aliases",
        "oldtxn0001",
    )
    assert before is None
    assert json.loads(after)["new_transaction_id"] == "newtxn0001"
    assert actor == "system"

    assert _metric("transaction_id_alias.insert") - before_metric == 1.0


def test_insert_rejects_duplicate_old_id(db: Database) -> None:
    """Append-only: an old_transaction_id forwards to exactly one new id."""
    repo = TransactionIdAliasesRepo(db)
    _insert(repo)
    with pytest.raises(ValueError, match="already aliased"):
        _insert(repo, new_transaction_id="newtxn0002")


def test_alias_insert_is_not_undoable(db: Database) -> None:
    """Append-only: undoing an alias insert orphans its forward pointer.

    BaseRepo.undo_event reverses an INSERT with a DELETE, so the repo refuses the
    undo instead of silently orphaning old_transaction_id.
    """
    repo = TransactionIdAliasesRepo(db)
    event = _insert(repo)
    with pytest.raises(ValueError, match="append-only"):
        repo.undo_event(event, actor="system")
    row = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_id_aliases WHERE old_transaction_id = ?",
        ["oldtxn0001"],
    ).fetchone()
    assert row is not None and row[0] == 1  # the alias survives the refused undo


def test_insert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = TransactionIdAliasesRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _insert(repo, old_transaction_id="ghost_old")

    rows = db.conn.execute(
        "SELECT 1 FROM app.transaction_id_aliases WHERE old_transaction_id = ?",
        ["ghost_old"],
    ).fetchall()
    assert rows == []
