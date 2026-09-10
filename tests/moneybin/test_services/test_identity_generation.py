"""Undo and redo advance identity evidence while preserving Raw revisions."""

from moneybin.database import Database
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_services.test_investment_identity import add_security_route
from tests.moneybin.test_services.test_manual_identity_projection import (
    add_account_edge,
    add_account_terminal,
    add_manual_observation,
    create_identity_views,
)


def test_identity_generations_change_on_round_trip_without_raw_changes(
    db: Database,
) -> None:
    add_manual_observation(db)
    add_account_terminal(db, "b")
    with operation() as op:
        add_account_edge(db, "a", "b")
        add_security_route(db, "survivor", "m")
    create_identity_views(db)
    frozen = db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall()
    before = db.execute(
        "SELECT account_id, security_id, account_identity_generation, security_identity_generation FROM prep.int_manual__investment_identity"
    ).fetchone()
    assert before is not None
    undo = UndoService(db).undo(op, actor="cli")
    undone = db.execute(
        "SELECT account_identity_generation, security_identity_generation FROM prep.int_manual__investment_identity"
    ).fetchone()
    UndoService(db).undo(undo.undo_operation_id, actor="cli")
    after = db.execute(
        "SELECT account_id, security_id, account_identity_generation, security_identity_generation FROM prep.int_manual__investment_identity"
    ).fetchone()
    assert after is not None and after[:2] == before[:2]
    assert after[2] != before[2] and after[3] != before[3]
    assert undone is not None and undone != before[2:] and undone != after[2:]
    assert (
        db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall()
        == frozen
    )
