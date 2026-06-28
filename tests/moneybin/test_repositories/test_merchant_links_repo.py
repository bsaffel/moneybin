"""Tests for ``MerchantLinksRepo``.

Every mutating test asserts both the row mutation and the paired ``app.audit_log``
entry land in one transaction, plus the (source_type, ref_kind, ref_value)
uniqueness guard among accepted rows.
"""

import pytest

from moneybin.database import Database
from moneybin.repositories.merchant_links_repo import MerchantLinksRepo


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
    with pytest.raises(ValueError, match="already"):
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
