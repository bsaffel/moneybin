"""Exact selected-lot preservation across a source-date correction and refusal.

Two units cost $200 each in the first lot and $100 each in the second.
Selling two for $300 with one unit from each realizes $0. FIFO instead
realizes a $100 loss, so these expectations detect a discarded election.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.exports.service import ExportService
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.investments.freshness import projection_pending
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.investment_service import InvestmentService
from moneybin.services.mutation_context import operation
from moneybin.services.transform_service import TransformService
from tests.moneybin.test_investments.comparison_helpers import (
    seed_manual_event,
    seed_plaid_event,
)

pytestmark = pytest.mark.integration


def _correct_date(db: Database, day: int) -> None:
    transaction = SyncInvestmentTransaction(
        investment_transaction_id="buy",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, day),
        name="Revised",
        quantity=Decimal(2),
        amount=Decimal(200),
        price=Decimal(100),
        fees=Decimal(0),
        type="buy",
        subtype="buy",
    )
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # retain business time while recording the new post-build landing
        [transaction],
        "date_correction",
        datetime(2026, 2, 2, tzinfo=UTC),
        datetime.now(UTC),
    )


def _selected_position(db: Database) -> dict[str, dict[str, Any]]:
    """Publish two acquisitions, then make a complete specific-ID election."""
    SecuritiesRepo(db).upsert(
        security_id="security",
        name="Synthetic selected security",
        security_type="equity",
        cost_basis_method="specific",
        actor="test",
    )
    seed_manual_event(db, "expensive", quantity="2", price="200", amount="-400", day=1)
    seed_plaid_event(db, "buy", quantity="2", amount="200", day=10)
    seed_manual_event(
        db,
        "disposal",
        type_="sell",
        quantity="-2",
        price="150",
        amount="300",
        day=20,
    )
    built = TransformService(db).apply()
    assert built.applied, built.error
    rows = {
        row["native_reference"]: row
        for row in InvestmentMembershipService(db).project()
    }
    assert set(rows) == {"expensive", "buy", "disposal"}
    expensive = compute_lot_id(
        "account",
        "security",
        date(2026, 1, 1),
        rows["expensive"]["investment_transaction_id"],
    )
    cheap = compute_lot_id(
        "account",
        "security",
        date(2026, 1, 10),
        rows["buy"]["investment_transaction_id"],
    )
    InvestmentService(db).select_lots(
        rows["disposal"]["investment_transaction_id"],
        [(expensive, Decimal(1)), (cheap, Decimal(1))],
        actor="test",
    )
    _assert_selected_finances(db, rows, date(2026, 1, 10))
    return rows


def _assert_selected_finances(
    db: Database, rows: dict[str, dict[str, Any]], cheap_date: date
) -> None:
    expensive = compute_lot_id(
        "account",
        "security",
        date(2026, 1, 1),
        rows["expensive"]["investment_transaction_id"],
    )
    cheap = compute_lot_id(
        "account", "security", cheap_date, rows["buy"]["investment_transaction_id"]
    )
    disposal = rows["disposal"]["investment_transaction_id"]
    assert db.execute("SELECT COUNT(*) FROM core.fct_investment_lots").fetchone() == (
        2,
    )
    assert db.execute("SELECT COUNT(*) FROM core.fct_realized_gains").fetchone() == (2,)
    assert LotSelectionsRepo(db).list_for_disposal(disposal) == sorted([
        (expensive, Decimal(1)),
        (cheap, Decimal(1)),
    ])
    assert {
        tuple(row)
        for row in db.execute("""SELECT lot_id, acquisition_date,
            remaining_quantity, cost_basis_total, cost_basis_remaining,
            cost_basis_method, basis_incomplete
            FROM core.fct_investment_lots""").fetchall()
    } == {
        (
            expensive,
            date(2026, 1, 1),
            Decimal(1),
            Decimal(400),
            Decimal(200),
            "specific",
            False,
        ),
        (cheap, cheap_date, Decimal(1), Decimal(200), Decimal(100), "specific", False),
    }
    assert {
        tuple(row)
        for row in db.execute("""SELECT disposal_txn_id, lot_id,
            acquisition_date, disposal_date, quantity, proceeds, cost_basis,
            gain_loss, cost_basis_method, basis_incomplete
            FROM core.fct_realized_gains""").fetchall()
    } == {
        (
            disposal,
            expensive,
            date(2026, 1, 1),
            date(2026, 1, 20),
            Decimal(1),
            Decimal(150),
            Decimal(200),
            Decimal(-50),
            "specific",
            False,
        ),
        (
            disposal,
            cheap,
            cheap_date,
            date(2026, 1, 20),
            Decimal(1),
            Decimal(150),
            Decimal(100),
            Decimal(50),
            "specific",
            False,
        ),
    }
    assert db.execute(
        "SELECT quantity, cost_basis FROM core.dim_holdings"
    ).fetchall() == [(Decimal(2), Decimal(300))]


def test_source_date_correction_remaps_complete_selection_and_rebuilds_exact_gains(
    comparison_db: Database,
) -> None:
    db = comparison_db
    before = _selected_position(db)
    old_lot = compute_lot_id(
        "account",
        "security",
        date(2026, 1, 10),
        before["buy"]["investment_transaction_id"],
    )
    _correct_date(db, 11)
    with operation() as corrected:
        built = TransformService(db).apply()
    assert built.applied, built.error
    after = {
        row["native_reference"]: row
        for row in InvestmentMembershipService(db).project()
    }
    assert after["buy"]["trade_date"] == date(2026, 1, 11)
    assert after["buy"]["observation_version"] != before["buy"]["observation_version"]
    assert {
        native: (row["event_group_id"], row["investment_transaction_id"])
        for native, row in after.items()
    } == {
        native: (row["event_group_id"], row["investment_transaction_id"])
        for native, row in before.items()
    }
    _assert_selected_finances(db, after, date(2026, 1, 11))
    assert db.execute(
        "SELECT COUNT(*) FROM app.lot_selections WHERE lot_id = ?", [old_lot]
    ).fetchone() == (0,)
    events = AuditService(db).events_for_operation(corrected)
    assert sum(event.action == "lot_selections.set" for event in events) == 1
    selections = LotSelectionsRepo(db).list_for_disposal(
        after["disposal"]["investment_transaction_id"]
    )
    history = InvestmentEventMembersRepo(db).history()
    built = TransformService(db).apply()
    assert built.applied, built.error
    _assert_selected_finances(db, after, date(2026, 1, 11))
    assert InvestmentEventMembersRepo(db).history() == history
    assert (
        LotSelectionsRepo(db).list_for_disposal(
            after["disposal"]["investment_transaction_id"]
        )
        == selections
    )


def test_source_date_after_unchanged_disposal_refuses_before_fifo_can_replace_election(
    comparison_db: Database,
) -> None:
    db = comparison_db
    before = _selected_position(db)
    membership = InvestmentMembershipService(db)
    projected = membership.project()
    history = InvestmentEventMembersRepo(db).history()
    selections = LotSelectionsRepo(db).list_for_disposal(
        before["disposal"]["investment_transaction_id"]
    )
    _correct_date(db, 21)
    # Only the acquisition changed. The January 20 disposal and its complete
    # two-lot election must still be checked at the earliest changed ordering key.
    assert membership.project() == projected
    with operation() as refused:
        built = TransformService(db).apply()
    assert not built.applied
    assert built.error is not None and "selection" in built.error
    assert built.investment_match is not None
    assert built.investment_match.error is not None
    assert InvestmentEventMembersRepo(db).history() == history
    assert membership.project() == projected
    assert (
        LotSelectionsRepo(db).list_for_disposal(
            before["disposal"]["investment_transaction_id"]
        )
        == selections
    )
    assert AuditService(db).events_for_operation(refused) == []
    _assert_selected_finances(db, before, date(2026, 1, 10))
    assert projection_pending(db)
    service = InvestmentService(db)
    for read in (service.holdings, service.lots, service.gains):
        with pytest.raises(UserError) as caught:
            read()
        assert caught.value.code == "refresh_stale"
    with pytest.raises(UserError) as caught:
        ExportService(db).prepare_bundle(profile="test", redaction_mode="unredacted")
    assert caught.value.code == "refresh_stale"
