"""Mixed-source lifecycle restates independently calculated financial results.

Each buy is 10 units/$1,000; the one sale is 4 units/$600. With two
unconsolidated buys, 16 units/$1,600 remain; with one, 6 units/$600 remain.
Every stage realizes $600-$400=$200, retains $12 income and $3 fees.
The collision guard still withholds valuation even with a usable $150 close.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.exports.service import ExportService
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.investments.freshness import projection_pending
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.investment_service import InvestmentService
from moneybin.services.mutation_context import operation
from moneybin.services.transform_service import TransformService
from moneybin.services.undo_service import UndoService
from moneybin.services.user_reports_service import UserReportsService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_account_metadata,
    seed_manual_event,
    seed_plaid_event,
)

pytestmark = pytest.mark.integration


def _bundle_rows(db: Database, name: str) -> list[dict[str, Any]]:
    snapshot = ExportService(db).prepare_bundle(
        profile="test", redaction_mode="unredacted"
    )
    table = next(table for table in snapshot.tables if table.name == name)
    return [
        dict(zip((column.name for column in table.columns), row, strict=True))
        for row in table.rows
    ]


def _assert_financial_state(db: Database, *, buys: int, report_id: str) -> None:
    remaining = Decimal(10 * buys - 4)
    basis = remaining * 100
    assert not projection_pending(db)
    assert db.execute(
        """SELECT SUM(remaining_quantity), SUM(cost_basis_remaining),
                  SUM(cost_basis_total), COUNT(*)
           FROM core.fct_investment_lots"""
    ).fetchone() == (remaining, basis, Decimal(1000 * buys), buys)
    assert db.execute(
        """SELECT SUM(quantity), SUM(proceeds), SUM(cost_basis), SUM(gain_loss)
           FROM core.fct_realized_gains"""
    ).fetchone() == (Decimal(4), Decimal(600), Decimal(400), Decimal(200))
    assert db.execute(
        """SELECT quantity, cost_basis, average_cost, valuation_status,
                  market_value, unrealized_gain, price_date, price_source
           FROM core.dim_holdings"""
    ).fetchone() == (
        remaining,
        basis,
        Decimal(100),
        "source_overlap",
        None,
        None,
        None,
        None,
    )
    assert db.execute(
        """SELECT SUM(amount) FILTER (WHERE type = 'dividend'),
                  -SUM(amount) FILTER (WHERE type = 'fee'), SUM(fees)
           FROM core.fct_investment_transactions"""
    ).fetchone() == (Decimal(12), Decimal(3), Decimal(0))
    assert db.execute(
        """SELECT close FROM core.fct_security_prices
           WHERE price_date = DATE '2026-01-21' AND source_type = 'plaid'"""
    ).fetchall() == [(Decimal(150),)]
    holdings = InvestmentService(db).holdings()
    assert len(holdings.rows) == 1
    assert holdings.rows[0].quantity == remaining
    assert holdings.rows[0].cost_basis == basis
    assert holdings.rows[0].market_value is None
    assert holdings.degraded_reason is not None
    assert "source_overlap" in holdings.degraded_reason
    assert holdings.warnings

    transactions = _bundle_rows(db, "investment_transactions")
    assert len(transactions) == buys + 3
    assert sum(
        row["amount"] for row in transactions if row["type"] == "buy"
    ) == Decimal(-1000 * buys)
    assert [
        (r["quantity"], r["cost_basis"], r["market_value"], r["valuation_status"])
        for r in _bundle_rows(db, "holdings")
    ] == [(remaining, basis, None, "source_overlap")]
    gains = _bundle_rows(db, "realized_gains")
    assert sum(row["gain_loss"] for row in gains) == Decimal(200)
    assert sum(row["cost_basis"] for row in gains) == Decimal(400)
    report = ExportService(db).prepare_report(
        profile="test", report_id=report_id, redaction_mode="unredacted"
    )
    assert report.tables[0].rows == ((remaining, basis, None, "source_overlap"),)


def test_real_transform_accept_replace_and_audit_undo_restate_exact_finances(
    comparison_db: Database,
) -> None:
    db = comparison_db
    seed_account_metadata(
        db, "account", native="native_account", origin="third", currency="USD"
    )
    seed_manual_event(db, "acquisition", quantity="10", amount="-1000")
    seed_manual_event(
        db, "disposal", type_="sell", quantity="-4", price="150", amount="600", day=20
    )
    seed_manual_event(
        db, "income", type_="dividend", quantity=None, price=None, amount="12", day=15
    )
    seed_manual_event(
        db, "commission", type_="fee", quantity=None, price=None, amount="-3", day=16
    )
    install_comparison_models(db)
    InvestmentMembershipService(db).reconcile(actor="test")
    seed_plaid_event(db, "broker_buy", quantity="10", amount="1000")
    db.execute(
        """INSERT INTO raw.security_prices (
            provider_security_key, price_date, quote_currency, source_type,
            source_origin, close, price_basis, extracted_at, loaded_at
        ) VALUES ('native_security', DATE '2026-01-21', 'USD', 'plaid',
                  'origin', 150, 'raw', ?, ?)""",
        [datetime(2026, 2, 1, tzinfo=UTC)] * 2,
    )
    built = TransformService(db).apply()
    assert built.applied, built.error
    report = UserReportsService(db).create(
        name="lifecycle_position",
        query_sql="""SELECT quantity, cost_basis, market_value, valuation_status
                     FROM core.dim_holdings""",
        actor="test",
    )
    _assert_financial_state(db, buys=2, report_id=report.report_id)
    repo = InvestmentEventMembersRepo(db)
    initial = repo.active_members()
    manual = next(r for r in initial if r["native_reference"] == "acquisition")
    plaid = next(r for r in initial if r["native_reference"] == "broker_buy")
    disposal = next(r for r in initial if r["native_reference"] == "disposal")
    old_lot = compute_lot_id(
        "account", "security", date(2026, 1, 10), plaid["investment_transaction_id"]
    )
    surviving_lot = compute_lot_id(
        "account", "security", date(2026, 1, 10), manual["investment_transaction_id"]
    )
    selections = LotSelectionsRepo(db)
    selections.set_for_disposal(
        investment_transaction_id=disposal["investment_transaction_id"],
        selections=[(old_lot, Decimal(4))],
        actor="test",
    )
    matching = InvestmentMatchingService(db)
    first = matching.pending()
    assert len(first) == 1 and first[0]["status"] == "pending"
    with operation():
        matching.decide(first[0]["proposal_id"], decision="accept", actor="test")
    _assert_financial_state(db, buys=1, report_id=report.report_id)
    assert selections.list_for_disposal(disposal["investment_transaction_id"]) == [
        (surviving_lot, Decimal(4))
    ]

    seed_plaid_event(
        db, "third_buy", source_origin="third", quantity="10", amount="1000"
    )
    built = TransformService(db).apply()
    assert built.applied, built.error
    _assert_financial_state(db, buys=2, report_id=report.report_id)
    third = next(
        r for r in repo.active_members() if r["native_reference"] == "third_buy"
    )
    third_lot = compute_lot_id(
        "account", "security", date(2026, 1, 10), third["investment_transaction_id"]
    )
    selections.set_for_disposal(
        investment_transaction_id=disposal["investment_transaction_id"],
        selections=[(third_lot, Decimal(4))],
        actor="test",
    )
    matching.run()
    replacement = matching.pending()
    assert len(replacement) == 1 and not replacement[0]["auto_eligible"]
    with operation() as accepted:
        matching.decide(replacement[0]["proposal_id"], decision="accept", actor="test")
    _assert_financial_state(db, buys=1, report_id=report.report_id)
    assert selections.list_for_disposal(disposal["investment_transaction_id"]) == [
        (surviving_lot, Decimal(4))
    ]
    assert {
        r["investment_transaction_id"]
        for r in repo.active_members()
        if r["native_reference"] in {"acquisition", "broker_buy", "third_buy"}
    } == {manual["investment_transaction_id"]}

    undone = UndoService(db).undo(accepted, actor="test")
    _assert_financial_state(db, buys=2, report_id=report.report_id)
    assert selections.list_for_disposal(disposal["investment_transaction_id"]) == [
        (third_lot, Decimal(4))
    ]
    active = repo.active_members()
    assert {
        r["decision_id"]
        for r in active
        if r["native_reference"] in {"acquisition", "broker_buy"}
    } == {first[0]["proposal_id"]}
    assert (
        next(r for r in active if r["native_reference"] == "third_buy")["decision_id"]
        is None
    )
    assert any(
        e.is_undo and e.undoes_operation_id == accepted
        for e in AuditService(db).events_for_operation(undone.undo_operation_id)
    )
    stable = repo.history()
    built = TransformService(db).apply()
    assert built.applied, built.error
    _assert_financial_state(db, buys=2, report_id=report.report_id)
    assert repo.history() == stable
