"""Tests for ``LotSelectionsRepo``.

``set_for_disposal`` is a declarative replace (Shape 1a): the whole selection
set for one disposal is captured as ``before``, deleted, re-inserted from
``selections``, captured as ``after``, and audited as ONE
``lot_selections.set`` row — never a row per (investment_transaction_id,
lot_id). An empty ``selections`` list clears all overrides for that disposal.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.services.audit_service import AuditEvent


def _audit_rows_for(db: Database, target_id: str) -> list[tuple[Any, ...]]:
    return db.conn.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor, parent_audit_id
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [target_id],
    ).fetchall()


def test_set_for_disposal_replaces_prior_selections(db: Database) -> None:
    repo = LotSelectionsRepo(db)
    repo.set_for_disposal(
        investment_transaction_id="txn_1",
        selections=[("lot_a", Decimal("5")), ("lot_b", Decimal("5"))],
        actor="cli",
    )

    repo.set_for_disposal(
        investment_transaction_id="txn_1",
        selections=[("lot_b", Decimal("3")), ("lot_c", Decimal("7"))],
        actor="cli",
    )

    result = repo.list_for_disposal("txn_1")
    assert result == [("lot_b", Decimal("3")), ("lot_c", Decimal("7"))]


def test_set_for_disposal_with_empty_list_clears_selections(db: Database) -> None:
    repo = LotSelectionsRepo(db)
    repo.set_for_disposal(
        investment_transaction_id="txn_1",
        selections=[("lot_a", Decimal("5"))],
        actor="cli",
    )

    repo.set_for_disposal(
        investment_transaction_id="txn_1",
        selections=[],
        actor="cli",
    )

    assert repo.list_for_disposal("txn_1") == []


def test_set_for_disposal_emits_one_audit_row_for_whole_set(db: Database) -> None:
    repo = LotSelectionsRepo(db)
    repo.set_for_disposal(
        investment_transaction_id="txn_1",
        selections=[("lot_a", Decimal("5")), ("lot_b", Decimal("5"))],
        actor="cli",
    )

    audit = _audit_rows_for(db, "txn_1")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "lot_selections.set"
    assert (schema, table, target_id) == ("app", "lot_selections", "txn_1")
    assert actor == "cli"

    before_payload = json.loads(before)
    assert before_payload == {
        "investment_transaction_id": "txn_1",
        "selections": [],
    }
    after_payload = json.loads(after)
    assert after_payload["investment_transaction_id"] == "txn_1"
    lot_ids = {s["lot_id"] for s in after_payload["selections"]}
    assert lot_ids == {"lot_a", "lot_b"}


def test_set_for_disposal_before_value_reflects_prior_set(db: Database) -> None:
    repo = LotSelectionsRepo(db)
    repo.set_for_disposal(
        investment_transaction_id="txn_1",
        selections=[("lot_a", Decimal("5"))],
        actor="cli",
    )
    repo.set_for_disposal(
        investment_transaction_id="txn_1",
        selections=[("lot_b", Decimal("3"))],
        actor="cli",
    )

    audit = _audit_rows_for(db, "txn_1")
    assert len(audit) == 2
    second_before = json.loads(audit[1][4])
    # DECIMAL(28,10) round-trips Decimal("5") as Decimal("5.0000000000").
    assert second_before == {
        "investment_transaction_id": "txn_1",
        "selections": [{"lot_id": "lot_a", "quantity": "5.0000000000"}],
    }


def test_list_for_disposal_returns_decimal_quantities(db: Database) -> None:
    repo = LotSelectionsRepo(db)
    repo.set_for_disposal(
        investment_transaction_id="txn_1",
        selections=[("lot_a", Decimal("12.3456789012"))],
        actor="cli",
    )

    result = repo.list_for_disposal("txn_1")
    assert len(result) == 1
    _lot_id, quantity = result[0]
    assert isinstance(quantity, Decimal)
    assert quantity == Decimal("12.3456789012")


def test_set_for_disposal_records_parent_audit_id_when_supplied(db: Database) -> None:
    repo = LotSelectionsRepo(db)
    parent = repo.set_for_disposal(
        investment_transaction_id="txn_parent",
        selections=[("lot_a", Decimal("1"))],
        actor="cli",
    )
    event = repo.set_for_disposal(
        investment_transaction_id="txn_child",
        selections=[("lot_b", Decimal("2"))],
        actor="cli",
        parent_audit_id=parent.audit_id,
    )
    audit = _audit_rows_for(db, event.target_id or "")
    assert audit[0][7] == parent.audit_id  # parent_audit_id column


def test_set_for_disposal_returns_audit_event(db: Database) -> None:
    repo = LotSelectionsRepo(db)
    event = repo.set_for_disposal(
        investment_transaction_id="txn_1",
        selections=[("lot_a", Decimal("5"))],
        actor="cli",
    )
    assert isinstance(event, AuditEvent)
    assert event.target_id == "txn_1"


def test_undo_of_set_event_raises_user_error(db: Database) -> None:
    """The generic reverser can't reverse a collection-shaped audit row.

    ``before``/``after`` here are ``{"investment_transaction_id", "selections"}``
    dicts, not row-shaped by ``pk_columns`` — so ``_require_capture`` correctly
    refuses to synthesize an inverse (no ``lot_id`` key to locate a row by).
    """
    repo = LotSelectionsRepo(db)
    event = repo.set_for_disposal(
        investment_transaction_id="txn_1",
        selections=[("lot_a", Decimal("5"))],
        actor="cli",
    )
    with pytest.raises(UserError):
        repo.undo_event(event, actor="cli")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
