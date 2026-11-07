"""Dependency publication and Golden membership change inside one transaction."""

from datetime import UTC, datetime
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.account_settings_repo import AccountSettingsRepo
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.mutation_context import operation
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)
from tests.moneybin.test_investments.test_investment_match_acceptance import (
    registered_proposal,
)


def settings(
    db: Database, *, currency: str | None = None, source: str | None = None
) -> None:
    AccountSettingsRepo(db).set(
        account_id="account",
        display_name=None,
        official_name=None,
        last_four=None,
        account_subtype=None,
        holder_category=None,
        currency_code=currency,
        credit_limit=None,
        archived=False,
        archived_at=None,
        include_in_net_worth=True,
        default_cost_basis_method=None,
        investment_source_type=source,
        investment_source_type_changed_at=datetime.now(UTC),
        archived_changed_at=None,
        currency_code_changed_at=datetime.now(UTC),
        default_cost_basis_method_changed_at=None,
        actor="test",
    )


def standalone(db: Database, source: str = "plaid") -> dict[str, Any]:
    (seed_plaid_event if source == "plaid" else seed_manual_event)(db, "native")
    install_comparison_models(db)
    InvestmentMembershipService(db).reconcile(actor="test")
    return InvestmentEventMembersRepo(db).active_members()[0]


def target_security(db: Database) -> str:
    SecuritiesRepo(db).upsert(
        security_id="other_security",
        name="Other Security",
        security_type="equity",
        actor="test",
    )
    row = db.execute(
        "SELECT link_id FROM app.security_links WHERE status = 'accepted' AND ref_kind = 'plaid_security_id'"
    ).fetchone()
    assert row is not None
    return str(row[0])


def test_security_repoint_advances_standalone_before_route_commit(
    comparison_db: Database,
) -> None:
    before = standalone(comparison_db)
    link = target_security(comparison_db)
    with operation():
        comparison_db.begin()
        try:
            SecurityLinksRepo(comparison_db).repoint(
                link_id=link,
                new_security_id="other_security",
                decided_by="user",
                actor="test",
                in_outer_txn=True,
            )
            after = InvestmentEventMembersRepo(comparison_db).active_members()[0]
            assert after["bound_security_id"] == "other_security"
            assert after["membership_id"] != before["membership_id"]
            assert (
                after["investment_transaction_id"]
                == before["investment_transaction_id"]
            )
            comparison_db.commit()
        except BaseException:
            comparison_db.rollback()
            raise


def test_non_equivalent_repoint_stales_match_and_preserves_reviewed_binding(
    comparison_db: Database,
) -> None:
    matching, proposal, _ = registered_proposal(comparison_db)
    with operation():
        matching.decide(proposal, decision="accept", actor="test")
    before = InvestmentEventMembersRepo(comparison_db).history()
    SecurityLinksRepo(comparison_db).repoint(
        link_id=target_security(comparison_db),
        new_security_id="other_security",
        decided_by="user",
        actor="test",
    )
    assert matching.history()[0]["status"] == "stale"
    assert InvestmentEventMembersRepo(comparison_db).history() == before
    assert (
        InvestmentMembershipService(comparison_db).project()[0]["security_id"]
        == "security"
    )


def test_reversing_account_route_retires_unresolved_standalone(
    comparison_db: Database,
) -> None:
    standalone(comparison_db)
    raw = comparison_db.execute(
        "SELECT COUNT(*) FROM raw.plaid_investment_transactions"
    ).fetchone()
    AccountLinksRepo(comparison_db).reverse(
        link_id="account_link", reversed_by="user", actor="test"
    )
    assert InvestmentEventMembersRepo(comparison_db).active_members() == []
    assert (
        comparison_db.execute(
            "SELECT COUNT(*) FROM raw.plaid_investment_transactions"
        ).fetchone()
        == raw
    )


def test_inherited_currency_advances_standalone_atomically(
    comparison_db: Database,
) -> None:
    before = standalone(comparison_db, "manual")
    assert before["currency_source"] == "account"
    settings(comparison_db, currency="EUR")
    after = InvestmentEventMembersRepo(comparison_db).active_members()[0]
    assert after["bound_effective_currency"] == "EUR"
    assert after["investment_transaction_id"] == before["investment_transaction_id"]
    assert after["projection_changed_at"] > before["projection_changed_at"]


def test_inherited_currency_stales_reviewed_match_without_projection_transition(
    comparison_db: Database,
) -> None:
    matching, proposal, _ = registered_proposal(comparison_db)
    matching.decide(proposal, decision="accept", actor="test")
    before = InvestmentEventMembersRepo(comparison_db).history()
    settings(comparison_db, currency="EUR")
    assert matching.history()[0]["status"] == "stale"
    assert InvestmentEventMembersRepo(comparison_db).history() == before
    assert (
        InvestmentMembershipService(comparison_db).project()[0]["currency_code"]
        == "USD"
    )


def test_ratified_equivalence_scope_preserves_match_status_ids_and_reviewed_evidence(
    comparison_db: Database,
) -> None:
    from moneybin.investments.dependency_publication import (
        dependency_publication,
        record_equivalence,
    )

    matching, proposal, _ = registered_proposal(comparison_db)
    matching.decide(proposal, decision="accept", actor="test")
    before = InvestmentEventMembersRepo(comparison_db).active_members()
    link = target_security(comparison_db)
    comparison_db.begin()
    try:
        with dependency_publication(comparison_db):
            record_equivalence(
                comparison_db,
                kind="security",
                previous="security",
                proposed="other_security",
            )
            SecurityLinksRepo(comparison_db).repoint(
                link_id=link,
                new_security_id="other_security",
                decided_by="user",
                actor="test",
                in_outer_txn=True,
            )
            SecurityLinksRepo(comparison_db).insert(
                security_id="other_security",
                ref_kind="manual_investment_transaction_id",
                ref_value="manual",
                source_type="manual",
                decided_by="user",
                actor="test",
                in_outer_txn=True,
            )
        comparison_db.commit()
    except BaseException:
        comparison_db.rollback()
        raise
    after = InvestmentEventMembersRepo(comparison_db).active_members()
    assert matching.history()[0]["status"] == "accepted"
    assert {row["bound_security_id"] for row in after} == {"other_security"}
    assert {
        (
            row["event_group_id"],
            row["investment_transaction_id"],
            row["observation_version"],
        )
        for row in after
    } == {
        (
            row["event_group_id"],
            row["investment_transaction_id"],
            row["observation_version"],
        )
        for row in before
    }
    assert {row["membership_id"] for row in after}.isdisjoint(
        row["membership_id"] for row in before
    )


def test_explicit_source_currency_does_not_advance_membership(
    comparison_db: Database,
) -> None:
    seed_plaid_event(comparison_db, "native", currency="USD")
    install_comparison_models(comparison_db)
    InvestmentMembershipService(comparison_db).reconcile(actor="test")
    before = InvestmentEventMembersRepo(comparison_db).history()
    settings(comparison_db, currency="EUR")
    assert InvestmentEventMembersRepo(comparison_db).history() == before


def test_inherited_opening_currency_advances_with_account_setting(
    comparison_db: Database,
) -> None:
    from tests.moneybin.test_investments.test_investment_opening_membership import (
        seed_snapshot,
    )

    seed_snapshot(comparison_db, "snapshot", currency=None)
    install_comparison_models(comparison_db)
    InvestmentMembershipService(comparison_db).reconcile(actor="test")
    before = InvestmentEventMembersRepo(comparison_db).active_members()[0]
    settings(comparison_db, currency="EUR")
    after = InvestmentEventMembersRepo(comparison_db).active_members()[0]
    assert after["bound_effective_currency"] == "EUR"
    assert after["investment_transaction_id"] == before["investment_transaction_id"]


def test_source_choice_cannot_hide_a_selected_acquisition(
    comparison_db: Database,
) -> None:
    from datetime import date
    from decimal import Decimal

    import pytest

    from moneybin.errors import UserError
    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo

    seed_manual_event(comparison_db, "buy")
    seed_manual_event(
        comparison_db, "sell", type_="sell", quantity="-1", amount="100", day=20
    )
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    membership = InvestmentMembershipService(comparison_db)
    membership.reconcile(actor="test")
    rows = membership.project()
    acquisition = next(row for row in rows if row["source_type"] == "plaid")
    disposal = next(row for row in rows if row["type"] == "sell")
    lot = compute_lot_id(
        "account",
        "security",
        date(2026, 1, 10),
        acquisition["investment_transaction_id"],
    )
    LotSelectionsRepo(comparison_db).set_for_disposal(
        investment_transaction_id=disposal["investment_transaction_id"],
        selections=[(lot, Decimal(1))],
        actor="test",
    )
    before = InvestmentEventMembersRepo(comparison_db).history()
    with pytest.raises(UserError, match="selection"):
        settings(comparison_db, source="manual")
    assert InvestmentEventMembersRepo(comparison_db).history() == before
    assert (
        comparison_db.execute(
            "SELECT investment_source_type FROM app.account_settings"
        ).fetchall()
        == []
    )


def reviewed_profile(
    db: Database, with_selection: bool = False
) -> tuple[InvestmentMatchingService, list[dict[str, Any]]]:
    from moneybin.repositories.security_link_decisions_repo import (
        SecurityLinkDecisionsRepo,
    )
    from tests.moneybin.test_stg_plaid_investments import (
        _link_security,  # pyright: ignore[reportPrivateUsage]  # shared synthetic financial fixture
        _manual_investment_txn,  # pyright: ignore[reportPrivateUsage]  # shared synthetic financial fixture
        _publish_investment_fixture,  # pyright: ignore[reportPrivateUsage]  # shared synthetic financial fixture
        _raw_investment_txn,  # pyright: ignore[reportPrivateUsage]  # shared synthetic financial fixture
    )

    SecuritiesRepo(db).upsert(
        security_id="security",
        name="Synthetic provisional",
        security_type="equity",
        created_by="plaid",
        actor="test",
    )
    _raw_investment_txn(db)
    _link_security(db, "sec_1", "security")
    _manual_investment_txn(
        db,
        account_id="acc_1",
        security_id="security",
        trade_date="2026-07-06",
        quantity="10",
        price="214.55",
        amount="-2145.50",
        fees="0",
    )
    if with_selection:
        _manual_investment_txn(
            db,
            source_transaction_id="sell",
            investment_transaction_id="sell",
            account_id="acc_1",
            security_id="security",
            type="sell",
            trade_date="2026-07-07",
            quantity="-1",
            price="214.55",
            amount="214.55",
            fees="0",
        )
    _publish_investment_fixture(db)
    matching = InvestmentMatchingService(db)
    proposal = matching.pending()[0]["proposal_id"]
    with operation():
        matching.decide(proposal, decision="accept", actor="test")
    before = InvestmentEventMembersRepo(db).active_members()
    if with_selection:
        from datetime import date
        from decimal import Decimal

        from moneybin.investments.cost_basis import compute_lot_id
        from moneybin.repositories.lot_selections_repo import LotSelectionsRepo

        ledger = InvestmentMembershipService(db).project()
        buy = next(row for row in ledger if row["type"] == "buy")
        sell = next(row for row in ledger if row["type"] == "sell")
        lot = compute_lot_id(
            "acc_1", "security", date(2026, 7, 6), buy["investment_transaction_id"]
        )
        LotSelectionsRepo(db).set_for_disposal(
            investment_transaction_id=sell["investment_transaction_id"],
            selections=[(lot, Decimal(1))],
            actor="test",
        )
    SecuritiesRepo(db).upsert(
        security_id="survivor",
        name="Synthetic survivor",
        security_type="equity",
        actor="test",
    )
    SecurityLinkDecisionsRepo(db).insert(
        decision_id="merge",
        ref_kind="plaid_security_id",
        ref_value="sec_1",
        source_type="plaid",
        candidate_security_id="survivor",
        actor="test",
    )
    return matching, before


@pytest.mark.parametrize("with_selection", [False, True])
def test_public_security_merge_advances_complete_reviewed_match(
    db: Database, with_selection: bool
) -> None:
    from moneybin.services.security_links_service import SecurityLinksService

    matching, before = reviewed_profile(db, with_selection)
    with operation():
        SecurityLinksService(db, actor="test").accept_merge("merge", into="survivor")
    after = InvestmentEventMembersRepo(db).active_members()
    assert matching.history()[0]["status"] == "accepted"
    assert {row["bound_security_id"] for row in after} == {"survivor"}
    assert {row["investment_transaction_id"] for row in after} == {
        row["investment_transaction_id"] for row in before
    }
    assert db.execute(
        "SELECT DISTINCT security_id FROM core.fct_investment_transactions"
    ).fetchall() == [("survivor",)]


def test_identity_confirmation_binds_unprocessed_source_revision(db: Database) -> None:
    from datetime import date
    from decimal import Decimal

    from moneybin.connectors.sync_models import SyncInvestmentTransaction
    from moneybin.extractors.plaid.extractor import PlaidExtractor
    from moneybin.protocol.write_contracts import SecurityLinkDecisionRequest
    from moneybin.services.review_decisions_service import ReviewDecisionsService

    reviewed_profile(db)
    request = SecurityLinkDecisionRequest(
        kind="security_link",
        decision_id="merge",
        decision="accept",
        target_id="survivor",
    )
    service = ReviewDecisionsService(db, actor="test")
    before = service.plan_identity([request])
    tx = SyncInvestmentTransaction(
        investment_transaction_id="itx_1",
        account_id="acc_1",
        provider_item_id="item_1",
        security_id="sec_1",
        date=date(2026, 7, 6),
        name="Corrected purchase",
        quantity=Decimal(11),
        amount=Decimal("2360.05"),
        price=Decimal("214.55"),
        fees=Decimal(0),
        iso_currency_code="USD",
        type="buy",
        subtype="buy",
    )
    at = datetime(2026, 7, 9, tzinfo=UTC)
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # immutable synthetic correction
        [tx], "correction", at, at
    )
    after = service.plan_identity([request])
    assert before.items[0].before_state != after.items[0].before_state


def test_dependency_change_stales_planner_only_proposal_without_registering_membership(
    comparison_db: Database,
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    matching = InvestmentMatchingService(comparison_db)
    matching.run()
    assert len(matching.pending()) == 1
    assert not InvestmentEventMembersRepo(comparison_db).has_history()
    SecurityLinksRepo(comparison_db).repoint(
        link_id=target_security(comparison_db),
        new_security_id="other_security",
        decided_by="user",
        actor="test",
    )
    assert matching.pending() == []
    assert not InvestmentEventMembersRepo(comparison_db).has_history()


def test_settings_undo_rebinds_current_currency_without_restoring_member_images(
    db: Database,
) -> None:
    from moneybin.services.transform_service import TransformService
    from moneybin.services.undo_service import UndoService
    from tests.moneybin.test_stg_plaid_investments import (
        _manual_investment_txn,  # pyright: ignore[reportPrivateUsage]  # synthetic inherited-currency fixture
        _publish_investment_fixture,  # pyright: ignore[reportPrivateUsage]  # production synthetic refresh
    )

    _manual_investment_txn(
        db, account_id="account", security_id="security", currency_code=None
    )
    db.execute("""INSERT INTO raw.plaid_balances(account_id, balance_date, current_balance, available_balance,
        iso_currency_code, source_file, source_origin, extracted_at)
        VALUES ('account', DATE '2026-07-08', 100, 100, 'USD', 'balance', 'fixture_metadata', CURRENT_TIMESTAMP)""")
    _publish_investment_fixture(db)
    assert (
        InvestmentEventMembersRepo(db).active_members()[0]["bound_effective_currency"]
        == "USD"
    )
    with operation() as changed:
        settings(db, currency="EUR")
    before = InvestmentEventMembersRepo(db).history()
    assert TransformService(db).apply().applied
    assert db.execute(
        "SELECT currency_code FROM core.dim_accounts WHERE account_id = 'account'"
    ).fetchone() == ("EUR",)
    UndoService(db).undo(changed, actor="test")
    after = InvestmentEventMembersRepo(db).history()
    assert (
        InvestmentEventMembersRepo(db).active_members()[0]["bound_effective_currency"]
        == "USD"
    )
    assert {row["membership_id"] for row in before} <= {
        row["membership_id"] for row in after
    }
    retired = {
        row["membership_id"]: row for row in before if row["retired_at"] is not None
    }
    assert all(
        row == retired[row["membership_id"]]
        for row in after
        if row["membership_id"] in retired
    )
