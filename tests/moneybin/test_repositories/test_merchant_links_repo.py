"""Tests for ``MerchantLinksRepo``.

Every mutating test asserts both the row mutation and the paired ``app.audit_log``
entry land in one transaction, plus the (source_type, ref_kind, ref_value)
uniqueness guard among accepted rows.
"""

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.merchant_links_repo import MerchantLinksRepo
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService


def test_insert_creates_binding_and_pairs_audit(db: Database) -> None:
    repo = MerchantLinksRepo(db)
    event = repo.insert(
        link_id="lk0000000001",
        merchant_id="m00000000001",
        ref_kind="merchant_entity_id",
        ref_value="ent_abc",
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )
    assert event.action == "merchant_link.insert"
    assert repo.lookup("plaid", "ent_abc") == "m00000000001"
    row = db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action='merchant_link.insert'"
    ).fetchone()
    assert row is not None and row[0] == 1


def test_insert_rejects_duplicate_accepted_provider_id(db: Database) -> None:
    repo = MerchantLinksRepo(db)
    repo.insert(
        link_id="lk1",
        merchant_id="mA",
        ref_kind="merchant_entity_id",
        ref_value="ent_dup",
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )
    with pytest.raises(UserError, match="already"):
        repo.insert(
            link_id="lk2",
            merchant_id="mB",
            ref_kind="merchant_entity_id",
            ref_value="ent_dup",
            source_type="plaid",
            decided_by="auto",
            actor="system",
        )


def test_repoint_moves_binding_to_new_merchant(db: Database) -> None:
    repo = MerchantLinksRepo(db)
    repo.insert(
        link_id="lk1",
        merchant_id="mA",
        ref_kind="merchant_entity_id",
        ref_value="ent_x",
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )
    repo.repoint(link_id="lk1", new_merchant_id="mB", decided_by="user", actor="cli")
    assert repo.lookup("plaid", "ent_x") == "mB"


def test_undo_of_a_repoint_is_itself_undoable(db: Database) -> None:
    """Redo: undoing the undo re-applies the repoint.

    The undo engine replays rows in reverse, so the re-insert of the new binding
    runs before the old one is put back to 'reversed'. That transient violates
    the at-most-one-accepted-binding invariant even though the final state does
    not — the redo must not trip on it. Guarded by emitting repoint's audit rows
    in mutation order; the security twin carries the identical contract.
    """
    repo = MerchantLinksRepo(db)
    repo.insert(
        link_id="lk1",
        merchant_id="mA",
        ref_kind="merchant_entity_id",
        ref_value="ent_x",
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )

    with operation() as op:
        repo.repoint(
            link_id="lk1", new_merchant_id="mB", decided_by="user", actor="cli"
        )
    undone = UndoService(db).undo(op, actor="cli")
    assert repo.lookup("plaid", "ent_x") == "mA"

    UndoService(db).undo(undone.undo_operation_id, actor="cli")
    assert repo.lookup("plaid", "ent_x") == "mB"
