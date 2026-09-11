"""Recovery cannot strand later elections across identity mutation targets."""

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_services.test_investment_identity import (
    add_security_route,
    legacy_repoint,
    raw_observations,
)
from tests.moneybin.test_services.test_security_links_service import (
    add_disposal,
    add_lot,
    create_core_tables,
)


def test_legacy_undo_refuses_later_selection_of_affected_manual_acquisition(
    db: Database,
) -> None:
    op = legacy_repoint(db)
    create_core_tables(db)
    lot = add_lot(db, security_id="b", source_transaction_id="manual_buy")
    add_disposal(db, "sell", "b")
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="sell",
        selections=[(lot, Decimal("2"))],
        actor="cli",
    )
    frozen = raw_observations(db)
    audits = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="lot selection"):
        UndoService(db).undo(op, actor="cli")
    assert raw_observations(db) == frozen
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audits


def test_legacy_recovery_counts_both_actual_link_inverses(db: Database) -> None:
    add_security_route(db, "b")
    op = legacy_repoint(db)
    frozen = raw_observations(db)
    undo = UndoService(db).undo(op, actor="cli")
    events = AuditService(db).events_for_operation(undo.undo_operation_id)
    assert undo.reversed_row_count == len(events) == 2
    assert all(event.is_undo and event.undoes_operation_id == op for event in events)
    assert raw_observations(db) == frozen
    UndoService(db).undo(undo.undo_operation_id, actor="cli")
    assert raw_observations(db) == frozen
    assert (
        SecurityLinksRepo(db).lookup(
            ref_kind="manual_investment_transaction_id",
            ref_value="manual_buy",
            source_type="manual",
        )
        == "b"
    )
