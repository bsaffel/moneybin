"""Account merges preserve complete elections and expose their mutation impact."""

from datetime import date
from decimal import Decimal

import pytest
from pytest_mock import MockerFixture

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.db_helpers import create_core_dim_stub_views, create_core_tables
from tests.moneybin.test_services.test_manual_identity_projection import (
    add_account_edge,
    add_account_terminal,
)


def seed_account_selections(db: Database) -> str:
    create_core_tables(db)
    create_core_dim_stub_views(db)
    db.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name, currency_code) VALUES ('a', 'First', 'USD'), ('b', 'Second', 'USD')"
    )
    add_account_terminal(db, "a")
    add_account_terminal(db, "b")
    add_account_edge(db, "a", "b", status="pending")
    db.execute("""INSERT INTO core.fct_investment_transactions
        (investment_transaction_id, account_id, security_id, type, trade_date, quantity, currency_code)
        VALUES ('buy', 'a', 'security', 'buy', DATE '2026-01-01', 10, 'USD'),
               ('sell', 'a', 'security', 'sell', DATE '2026-02-01', -2, 'USD')""")
    lot = compute_lot_id("a", "security", date(2026, 1, 1), "buy")
    db.execute(
        """INSERT INTO core.fct_investment_lots
        (lot_id, account_id, security_id, acquisition_date, acquisition_type,
         source_transaction_id, currency_code, original_quantity, remaining_quantity)
        VALUES (?, 'a', 'security', DATE '2026-01-01', 'buy', 'buy', 'USD', 10, 8)""",
        [lot],
    )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="sell",
        selections=[(lot, Decimal("2"))],
        actor="cli",
    )
    return lot


def test_account_preview_captures_the_complete_selection_impact(db: Database) -> None:
    seed_account_selections(db)
    impact = AccountLinksService(db).accept_impact("a_b_", target_account_id="b")
    assert impact.blast_radius.get("lot_selections") == 1
    assert impact.lot_selection_disposal_ids == ("sell",)


def test_account_selection_remap_and_undo_are_one_operation(
    db: Database, mocker: MockerFixture
) -> None:
    old = seed_account_selections(db)
    mocker.patch.object(AccountLinksService, "rematch_after_merge", return_value=None)
    with operation() as op:
        AccountLinksService(db).set("a_b_", target_account_id="b")
    new = compute_lot_id("b", "security", date(2026, 1, 1), "buy")
    assert LotSelectionsRepo(db).list_for_disposal("sell") == [(new, Decimal("2"))]
    undo = UndoService(db).undo(op, actor="cli")
    assert LotSelectionsRepo(db).list_for_disposal("sell") == [(old, Decimal("2"))]
    UndoService(db).undo(undo.undo_operation_id, actor="cli")
    assert LotSelectionsRepo(db).list_for_disposal("sell") == [(new, Decimal("2"))]


def test_account_currency_change_refuses_atomically(db: Database) -> None:
    old = seed_account_selections(db)
    db.execute(
        "UPDATE core.dim_accounts SET currency_code = 'EUR' WHERE account_id = 'b'"
    )
    count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="lot selection"):
        AccountLinksService(db).set("a_b_", target_account_id="b")
    assert LotSelectionsRepo(db).list_for_disposal("sell") == [(old, Decimal("2"))]
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == count
