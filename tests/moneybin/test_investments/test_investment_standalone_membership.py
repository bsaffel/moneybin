"""Standalone registration preserves exact source evidence and Golden identity."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


def test_registration_keeps_both_sources_separate_and_ids_stable(
    comparison_db: Database,
) -> None:
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )

    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    service = InvestmentMembershipService(comparison_db)
    first = service.reconcile(actor="test")
    assert first.registered == 2
    rows = service.project()
    assert len(rows) == 2
    assert len({r["event_group_id"] for r in rows}) == 2
    assert all(
        len(r["event_group_id"]) == len(r["investment_transaction_id"]) == 12
        for r in rows
    )
    assert {r["amount"] for r in rows} == {Decimal("-100")}
    ids = {
        (r["source_type"], r["source_origin"]): r["investment_transaction_id"]
        for r in rows
    }
    second = service.reconcile(actor="test")
    assert second.registered == second.advanced == second.retired == 0
    assert ids == {
        (r["source_type"], r["source_origin"]): r["investment_transaction_id"]
        for r in service.project()
    }


def test_unpaired_and_ineligible_observations_still_register(
    comparison_db: Database,
) -> None:
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )

    seed_manual_event(
        comparison_db, "other", type_="other", quantity=None, amount="-5", price=None
    )
    seed_manual_event(comparison_db, "unpaired", type_="reinvest", subtype="dividend")
    install_comparison_models(comparison_db)
    service = InvestmentMembershipService(comparison_db)
    service.reconcile(actor="test")
    assert {r["type"] for r in service.project()} == {"other", "reinvest"}


def test_revision_advancement_preserves_identity_and_historical_evidence(
    comparison_db: Database,
) -> None:
    from moneybin.repositories.investment_event_members_repo import (
        InvestmentEventMembersRepo,
    )
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )

    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    service = InvestmentMembershipService(comparison_db)
    service.reconcile(actor="test")
    previous = service.project()[0]
    old_member = InvestmentEventMembersRepo(comparison_db).active_members()[0]
    revised = SyncInvestmentTransaction(
        investment_transaction_id="native",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, 10),
        name="Revised observation",
        quantity=Decimal("1"),
        amount=Decimal("150"),
        price=Decimal("150"),
        fees=Decimal("0"),
        type="buy",
        subtype="buy",
    )
    PlaidExtractor(comparison_db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # exercise immutable revision ingestion
        [revised],
        "sync_revision",
        datetime(2026, 2, 2, tzinfo=UTC),
        datetime(2026, 2, 2, tzinfo=UTC),
    )
    # Current source evidence changes before membership reconciliation; published
    # membership must still reproduce the last-bound immutable revision.
    assert service.project()[0]["amount"] == Decimal("-100")
    outcome = service.reconcile(actor="test")
    assert outcome.advanced == 1
    after = service.project()[0]
    assert after["amount"] == Decimal("-150")
    assert after["investment_transaction_id"] == previous["investment_transaction_id"]
    assert after["event_group_id"] == previous["event_group_id"]
    history = InvestmentEventMembersRepo(comparison_db).history()
    assert len(history) == 2
    assert (
        next(r for r in history if r["membership_id"] == old_member["membership_id"])[
            "observation_version"
        ]
        == old_member["observation_version"]
    )
    assert comparison_db.execute(
        "SELECT COUNT(*) FROM raw.plaid_investment_transactions"
    ).fetchone() == (2,)


def test_comparison_planning_alone_does_not_register_membership(
    comparison_db: Database,
) -> None:
    from moneybin.repositories.investment_event_members_repo import (
        InvestmentEventMembersRepo,
    )
    from moneybin.services.investment_matching_service import InvestmentMatchingService

    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    InvestmentMatchingService(comparison_db).run()
    assert InvestmentEventMembersRepo(comparison_db).history() == []


def test_pre_cut_election_without_prior_ledger_refuses_atomically(
    comparison_db: Database,
) -> None:
    from moneybin.errors import UserError
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )

    db = comparison_db
    seed_manual_event(db, "buy", quantity="2", amount="-20", price="10", day=1)
    install_comparison_models(db)
    repo = LotSelectionsRepo(db)
    repo.set_for_disposal(
        investment_transaction_id="sell",
        selections=[("unknown_lot", Decimal(1))],
        actor="test",
    )
    audit = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="prior ledger"):
        InvestmentMembershipService(db).reconcile(actor="test")
    assert repo.list_for_disposal("sell") == [("unknown_lot", Decimal(1))]
    assert db.execute(
        "SELECT COUNT(*) FROM app.investment_event_members"
    ).fetchone() == (0,)
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit


def test_populated_cutover_migrates_complete_source_id_elections(
    comparison_db: Database,
) -> None:
    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )

    db = comparison_db
    seed_manual_event(db, "buy", quantity="2", amount="-20", price="10", day=1)
    seed_manual_event(
        db, "sell", type_="sell", quantity="-1", amount="10", price="10", day=2
    )
    install_comparison_models(db)
    db.execute("""CREATE VIEW core.fct_investment_transactions AS
        SELECT *, original_investment_transaction_id AS investment_transaction_id
        FROM prep.int_investment_events__legs""")
    lot = compute_lot_id("account", "security", date(2026, 1, 1), "buy")
    repo = LotSelectionsRepo(db)
    repo.set_for_disposal(
        investment_transaction_id="sell", selections=[(lot, Decimal(1))], actor="test"
    )
    service = InvestmentMembershipService(db)
    service.reconcile(actor="test")
    buy, sell = sorted(service.project(), key=lambda row: row["trade_date"])
    expected = compute_lot_id(
        "account", "security", date(2026, 1, 1), buy["investment_transaction_id"]
    )
    assert repo.list_for_disposal("sell") == []
    assert repo.list_for_disposal(sell["investment_transaction_id"]) == [
        (expected, Decimal(1))
    ]


def test_ambiguous_pre_cut_election_blocks_all_membership_publication(
    comparison_db: Database,
) -> None:
    import pytest

    from moneybin.errors import UserError
    from moneybin.repositories.investment_event_members_repo import (
        InvestmentEventMembersRepo,
    )
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )

    db = comparison_db
    seed_manual_event(db, "buy", quantity="2", amount="-20", price="10", day=1)
    seed_manual_event(
        db, "sell", type_="sell", quantity="-1", amount="10", price="10", day=2
    )
    install_comparison_models(db)
    db.execute("""CREATE VIEW core.fct_investment_transactions AS
        SELECT *, original_investment_transaction_id AS investment_transaction_id
        FROM prep.int_investment_events__legs""")
    repo = LotSelectionsRepo(db)
    repo.set_for_disposal(
        investment_transaction_id="sell",
        selections=[("unknown_lot", Decimal(1))],
        actor="test",
    )
    audits = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="selection"):
        InvestmentMembershipService(db).reconcile(actor="test")
    assert InvestmentEventMembersRepo(db).history() == []
    assert repo.list_for_disposal("sell") == [("unknown_lot", Decimal(1))]
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audits


def test_changed_reinvest_shape_retires_all_former_semantic_ids(
    comparison_db: Database,
) -> None:
    from moneybin.repositories.investment_event_members_repo import (
        InvestmentEventMembersRepo,
    )
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )

    db = comparison_db
    seed_manual_event(
        db, "acquire", type_="reinvest", subtype="dividend", group="group"
    )
    seed_manual_event(
        db,
        "income",
        type_="dividend",
        group="group",
        quantity=None,
        amount="100",
        price=None,
    )
    install_comparison_models(db)
    service = InvestmentMembershipService(db)
    service.reconcile(actor="test")
    before = service.project()
    assert len({r["event_group_id"] for r in before}) == 1
    seed_manual_event(
        db,
        "ambiguous_income",
        type_="dividend",
        group="group",
        quantity=None,
        amount="100",
        price=None,
    )
    result = service.reconcile(actor="test")
    after = service.project()
    assert result.retired == 2 and result.registered == 3
    assert len({r["event_group_id"] for r in after}) == 3
    assert not {r["investment_transaction_id"] for r in before} & {
        r["investment_transaction_id"] for r in after
    }
    assert len(InvestmentEventMembersRepo(db).active_members()) == 3


def test_reconciliation_stales_pending_proposal_after_revision_change(
    comparison_db: Database,
) -> None:
    from moneybin.repositories.investment_match_decisions_repo import (
        InvestmentMatchDecisionsRepo,
    )
    from moneybin.services.investment_matching_service import InvestmentMatchingService
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )
    from tests.moneybin.test_investments.test_investment_opening_membership import (
        revise,
    )

    db = comparison_db
    seed_manual_event(db, "manual")
    seed_plaid_event(db, "buy")
    install_comparison_models(db)
    InvestmentMatchingService(db).run()
    pending = InvestmentMatchDecisionsRepo(db).all_rows()
    assert len(pending) == 1 and pending[0]["status"] == "pending"
    service = InvestmentMembershipService(db)
    service.reconcile(actor="test")
    revise(db, "2")
    service.reconcile(actor="test")
    assert InvestmentMatchDecisionsRepo(db).all_rows()[0]["status"] == "stale"


def test_unresolved_native_security_stays_out_of_golden_membership(
    comparison_db: Database,
) -> None:
    from moneybin.repositories.investment_event_members_repo import (
        InvestmentEventMembersRepo,
    )
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )

    seed_plaid_event(comparison_db, "unresolved", security_id="unmapped_security")
    install_comparison_models(comparison_db)
    result = InvestmentMembershipService(comparison_db).reconcile(actor="test")
    assert result.registered == 0
    assert InvestmentEventMembersRepo(comparison_db).active_members() == []


def test_failed_publication_rolls_back_every_member_and_audit(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pytest

    from moneybin.repositories.investment_event_members_repo import (
        InvestmentEventMembersRepo,
    )
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )

    db = comparison_db
    seed_manual_event(db, "first")
    seed_manual_event(db, "second")
    install_comparison_models(db)
    before = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    register = InvestmentEventMembersRepo.register
    calls = 0

    def fail_second(self: InvestmentEventMembersRepo, *args: Any, **kwargs: Any) -> str:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected publication failure")
        return register(self, *args, **kwargs)

    monkeypatch.setattr(InvestmentEventMembersRepo, "register", fail_second)
    with pytest.raises(RuntimeError, match="injected"):
        InvestmentMembershipService(db).reconcile(actor="test")
    assert calls == 2
    assert InvestmentEventMembersRepo(db).history() == []
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == before


def test_plaid_reinvest_revision_keeps_complete_shape_but_partner_switch_mints_ids(
    comparison_db: Database,
) -> None:
    from moneybin.services.investment_membership_service import (
        InvestmentMembershipService,
    )

    db = comparison_db
    seed_plaid_event(db, "acquire", type_="buy", subtype="dividend reinvestment")
    seed_plaid_event(
        db, "income_a", type_="cash", subtype="dividend", quantity=None, amount="-100"
    )
    seed_plaid_event(
        db,
        "income_b",
        type_="cash",
        subtype="dividend",
        quantity=None,
        amount="-100",
        day=20,
    )
    install_comparison_models(db)
    service = InvestmentMembershipService(db)
    service.reconcile(actor="test")
    before = {r["native_reference"]: r for r in service.project()}
    assert before["acquire"]["event_group_id"] == before["income_a"]["event_group_id"]
    extractor = PlaidExtractor(db)
    at = datetime(2026, 2, 2, tzinfo=UTC)
    changed = SyncInvestmentTransaction(
        investment_transaction_id="acquire",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, 10),
        name="Corrected description",
        quantity=Decimal(1),
        amount=Decimal(100),
        price=Decimal(100),
        fees=Decimal(0),
        type="buy",
        subtype="dividend reinvestment",
    )
    extractor._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # immutable synthetic revision
        [changed], "changed_acquire", at, at
    )
    service.reconcile(actor="test")
    assert {
        r["native_reference"]: r["investment_transaction_id"] for r in service.project()
    } == {native: r["investment_transaction_id"] for native, r in before.items()}
    incomes = [
        SyncInvestmentTransaction(
            investment_transaction_id=native,
            account_id="native_account",
            provider_item_id="origin",
            security_id="native_security",
            date=date(2026, 1, day),
            name="Corrected income",
            quantity=None,
            amount=Decimal(-100),
            fees=Decimal(0),
            type="cash",
            subtype="dividend",
        )
        for native, day in (("income_a", 20), ("income_b", 10))
    ]
    extractor._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # immutable synthetic revisions
        incomes, "changed_incomes", at, at
    )
    service.reconcile(actor="test")
    after = {r["native_reference"]: r for r in service.project()}
    assert after["acquire"]["event_group_id"] == after["income_b"]["event_group_id"]
    assert after["income_a"]["event_group_id"] != after["acquire"]["event_group_id"]
    assert not {r["investment_transaction_id"] for r in before.values()} & {
        r["investment_transaction_id"] for r in after.values()
    }
