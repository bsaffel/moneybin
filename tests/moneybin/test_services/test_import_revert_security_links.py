"""Import reversal retires manual identity routes atomically and permanently."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.import_service import ImportService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService


def _batch(db: Database, suffix: str) -> tuple[str, str]:
    import_id = ImportService(db).allocate_import_log(
        source_type="manual", format_name="manual_investment_entry", actor="cli"
    )
    db.execute(
        """INSERT INTO raw.manual_investment_transactions
        (source_transaction_id, import_id, account_id, security_id, security_ref,
         type, trade_date, quantity, price, amount, created_by,
         investment_transaction_id)
        VALUES (?, ?, 'account', 'original', 'TEST', 'buy', DATE '2024-05-01',
                10, 100, -1000, 'cli', ?)""",
        [suffix, import_id, suffix],
    )
    event = SecurityLinksRepo(db).insert(
        security_id="survivor",
        ref_kind="manual_investment_transaction_id",
        ref_value=suffix,
        source_type="manual",
        decided_by="user",
        actor="cli",
    )
    assert event.target_id is not None
    return import_id, event.target_id


def _status(db: Database, link_id: str) -> str:
    row = db.execute(
        "SELECT status FROM app.security_links WHERE link_id = ?", [link_id]
    ).fetchone()
    assert row is not None
    return str(row[0])


@pytest.mark.parametrize("actor", ["system", "cli", "mcp"])
def test_revert_retires_only_batch_routes_with_audit(db: Database, actor: str) -> None:
    batch, link = _batch(db, "removed")
    _, retained = _batch(db, "retained")
    with operation() as op:
        result = ImportService(db).revert_confirmed(
            batch, verify=lambda _: None, actor=actor
        )
    assert result == {"status": "reverted", "rows_deleted": 1}
    assert _status(db, link) == "reversed"
    assert _status(db, retained) == "accepted"
    row = db.execute(
        "SELECT action, actor FROM app.audit_log WHERE operation_id = ? AND target_id = ?",
        [op, link],
    ).fetchone()
    assert row == ("security_link.reverse", actor)


def test_plan_binds_routes_even_when_raw_counts_stay_equal(db: Database) -> None:
    batch, link = _batch(db, "changed")
    service = ImportService(db)
    before = service.plan_revert(batch)
    SecurityLinksRepo(db).repoint(
        link_id=link, new_security_id="other", decided_by="user", actor="cli"
    )
    after = service.plan_revert(batch)
    assert before.table_counts == after.table_counts
    assert before != after
    assert before.blast_radius["security_links_reversed"] == 1


def test_revert_route_cleanup_rolls_back_on_later_failure(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    batch, link = _batch(db, "rollback")
    before_audit = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()

    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("alias repair failed")

    monkeypatch.setattr(
        "moneybin.matching.aliasing.forward_rekeyed_transaction_ids", fail
    )
    with pytest.raises(RuntimeError, match="alias repair failed"):
        ImportService(db).revert_confirmed(batch, verify=lambda _: None)
    assert _status(db, link) == "accepted"
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == before_audit
    assert db.execute(
        "SELECT COUNT(*) FROM raw.manual_investment_transactions WHERE import_id = ?",
        [batch],
    ).fetchone() == (1,)


def test_undo_cannot_restore_route_after_irreversible_import_revert(
    db: Database,
) -> None:
    batch, link = _batch(db, "undo")
    _, retained = _batch(db, "undo_retained")
    with operation() as op:
        ImportService(db).revert_confirmed(batch, verify=lambda _: None)
        SecurityLinksRepo(db).reverse(link_id=retained, reversed_by="user", actor="cli")
    before_audit = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="observation"):
        UndoService(db).undo(op, actor="cli")
    assert _status(db, link) == "reversed"
    assert _status(db, retained) == "reversed"
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == before_audit
    assert db.execute(
        "SELECT COUNT(*) FROM raw.manual_investment_transactions WHERE import_id = ?",
        [batch],
    ).fetchone() == (0,)


def test_mcp_confirmation_changes_when_route_is_replaced(db: Database) -> None:
    from moneybin.mcp.tools.import_tools import (
        _import_revert_binding,  # pyright: ignore[reportPrivateUsage]  # test live confirmation binding
    )

    batch, link = _batch(db, "confirmation")
    service = ImportService(db)
    before = _import_revert_binding(service.plan_revert(batch))
    SecurityLinksRepo(db).repoint(
        link_id=link, new_security_id="other", decided_by="user", actor="cli"
    )
    after = _import_revert_binding(service.plan_revert(batch))
    assert before.canonical_bytes() != after.canonical_bytes()


def test_redo_cannot_reinsert_route_after_import_revert(db: Database) -> None:
    batch, link = _batch(db, "redo")
    row = db.execute(
        "SELECT operation_id FROM app.audit_log WHERE target_id = ?", [link]
    ).fetchone()
    assert row is not None
    undone = UndoService(db).undo(str(row[0]), actor="cli")
    ImportService(db).revert_confirmed(batch, verify=lambda _: None)
    with pytest.raises(UserError, match="observation"):
        UndoService(db).undo(undone.undo_operation_id, actor="cli")
    assert db.execute(
        "SELECT COUNT(*) FROM app.security_links WHERE link_id = ?", [link]
    ).fetchone() == (0,)
