"""Opening membership is reconstructed from retained, selected inputs."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from moneybin.connectors.sync_models import (
    InstitutionResult,
    SyncHolding,
    SyncInvestmentTransaction,
)
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.investment_membership_service import InvestmentMembershipService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_plaid_event,
)


def seed_snapshot(
    db: Database, file: str, quantity: str = "5", *, account: str = "native_account"
) -> None:
    holding = SyncHolding(
        account_id=account,
        provider_item_id="origin",
        security_id="native_security",
        quantity=Decimal(quantity),
        cost_basis=Decimal("50"),
        iso_currency_code="USD",
    )
    at = datetime(2026, 2, 1, tzinfo=UTC)
    extractor = PlaidExtractor(db)
    extractor._load_investment_holdings(  # pyright: ignore[reportPrivateUsage]  # retained synthetic snapshot
        [holding], {"origin": date(2026, 1, 1)}, {"origin"}, file, at, at
    )
    extractor._load_holdings_snapshots(  # pyright: ignore[reportPrivateUsage]  # retained synthetic receipt
        [holding],
        [
            InstitutionResult(
                provider_item_id="origin",
                status="completed",
                transactions_window_start=date(2026, 1, 1),
            )
        ],
        {"origin"},
        file,
        at,
        at,
    )


def revise(
    db: Database, quantity: str, *, trade_date: date = date(2026, 1, 10)
) -> None:
    transaction = SyncInvestmentTransaction(
        investment_transaction_id="buy",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=trade_date,
        name="Revised",
        quantity=Decimal(quantity),
        amount=Decimal(quantity) * 100,
        price=Decimal(100),
        fees=Decimal(0),
        type="buy",
        subtype="buy",
    )
    at = datetime(2026, 2, 2, tzinfo=UTC)
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # immutable synthetic revision
        [transaction], "revision_" + quantity, at, at
    )


def opening(service: InvestmentMembershipService) -> list[dict[str, Any]]:
    return [row for row in service.project() if row["subtype"] == "opening_bootstrap"]


def test_selected_revision_changes_gap_without_rotating_surviving_key(
    comparison_db: Database,
) -> None:
    db = comparison_db
    seed_plaid_event(db, "buy")
    seed_snapshot(db, "first")
    install_comparison_models(db)
    service = InvestmentMembershipService(db)
    outcome = service.reconcile(actor="test")
    assert outcome.registered == 2
    initial = opening(service)
    assert len(initial) == 1
    assert initial[0]["quantity"] == Decimal(4)
    member = next(
        row
        for row in InvestmentEventMembersRepo(db).active_members()
        if row["member_kind"] == "opening_lot_reconstruction"
    )
    evidence = json.loads(member["reconstruction_evidence"])
    assert evidence["snapshot"]["source_file"] == "first"
    assert evidence["source_member_ids"]
    assert "quantity" not in member["reconstruction_evidence"]
    revise(db, "2")
    assert opening(service)[0]["quantity"] == Decimal(4)
    service.reconcile(actor="test")
    after = opening(service)[0]
    assert after["quantity"] == Decimal(3)
    assert after["investment_transaction_id"] == initial[0]["investment_transaction_id"]
    assert after["event_group_id"] == initial[0]["event_group_id"]
    revise(db, "5")
    service.reconcile(actor="test")
    assert opening(service) == []


def test_equal_time_later_receipt_and_replay_preserve_first_snapshot(
    comparison_db: Database,
) -> None:
    db = comparison_db
    seed_plaid_event(db, "buy")
    seed_snapshot(db, "z_first")
    install_comparison_models(db)
    service = InvestmentMembershipService(db)
    service.reconcile(actor="test")
    initial = opening(service)
    assert len(initial) == 1
    history = InvestmentEventMembersRepo(db).history()
    seed_snapshot(db, "a_later", "50")
    seed_snapshot(db, "z_first")
    service.reconcile(actor="test")
    assert opening(service) == initial
    assert InvestmentEventMembersRepo(db).history() == history


def test_earlier_opening_gap_preflights_intervening_complete_election(
    comparison_db: Database,
) -> None:
    import pytest

    from moneybin.errors import UserError
    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from tests.moneybin.test_investments.comparison_helpers import seed_manual_event

    db = comparison_db
    seed_plaid_event(db, "buy")
    seed_manual_event(
        db, "sell", type_="sell", quantity="-4", amount="40", price="10", day=5
    )
    seed_snapshot(db, "first")
    install_comparison_models(db)
    service = InvestmentMembershipService(db)
    service.reconcile(actor="test")
    old = service.project()
    opening_row = opening(service)[0]
    sell = next(row for row in old if row["type"] == "sell")
    lot = compute_lot_id(
        "account",
        "security",
        date(2025, 12, 31),
        opening_row["investment_transaction_id"],
    )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id=sell["investment_transaction_id"],
        selections=[(lot, Decimal(4))],
        actor="test",
    )
    before = InvestmentEventMembersRepo(db).history()
    audit_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    revise(db, "2")
    with pytest.raises(UserError, match="selection"):
        service.reconcile(actor="test")
    assert InvestmentEventMembersRepo(db).history() == before
    assert service.project() == old
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit_count


def test_out_of_window_correction_does_not_advance_opening_membership(
    comparison_db: Database,
) -> None:
    db = comparison_db
    revise(db, "1", trade_date=date(2026, 3, 1))
    seed_snapshot(db, "first")
    install_comparison_models(db)
    service = InvestmentMembershipService(db)
    service.reconcile(actor="test")
    before = [
        row
        for row in InvestmentEventMembersRepo(db).history()
        if row["member_kind"] == "opening_lot_reconstruction"
    ]
    assert len(before) == 1
    revise(db, "2", trade_date=date(2026, 3, 1))
    service.reconcile(actor="test")
    assert [
        row
        for row in InvestmentEventMembersRepo(db).history()
        if row["member_kind"] == "opening_lot_reconstruction"
    ] == before


def test_later_account_in_same_origin_gets_its_own_first_receipt(
    comparison_db: Database,
) -> None:
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    db = comparison_db
    db.execute("INSERT INTO core.dim_accounts VALUES ('second_account', 'USD')")
    AccountLinksRepo(db).insert(
        link_id="second_link",
        account_id="second_account",
        ref_kind="source_native",
        ref_value="second_native",
        source_type="plaid",
        source_origin="origin",
        decided_by="user",
        actor="test",
    )
    seed_snapshot(db, "first")
    install_comparison_models(db)
    service = InvestmentMembershipService(db)
    service.reconcile(actor="test")
    initial = opening(service)
    assert len(initial) == 1
    seed_snapshot(db, "later", "7", account="second_native")
    service.reconcile(actor="test")
    rows = opening(service)
    assert {r["account_id"]: r["quantity"] for r in rows} == {
        "account": Decimal(5),
        "second_account": Decimal(7),
    }
    assert next(r for r in rows if r["account_id"] == "account") == initial[0]


def test_unresolved_opening_account_is_not_published(comparison_db: Database) -> None:
    db = comparison_db
    seed_snapshot(db, "unresolved", account="unmapped_account")
    install_comparison_models(db)
    result = InvestmentMembershipService(db).reconcile(actor="test")
    assert result.registered == 0
    assert InvestmentEventMembersRepo(db).history() == []
