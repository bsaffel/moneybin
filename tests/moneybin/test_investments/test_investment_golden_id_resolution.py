"""Core identity resolvers derive self-maps, forwards and retirement from history."""

from moneybin.database import Database
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.mutation_context import operation
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_plaid_event,
)
from tests.moneybin.test_investments.test_investment_golden_projection import (
    install_model,
)
from tests.moneybin.test_investments.test_investment_match_acceptance import (
    registered_proposal,
)
from tests.moneybin.test_investments.test_investment_opening_membership import (
    revise,
    seed_snapshot,
)


def install_resolvers(db: Database) -> None:
    install_model(db, "core.alias_investment_event_ids")
    install_model(db, "core.alias_investment_transaction_ids")


def test_consolidation_forwards_only_to_active_membership_and_excludes_source_ids(
    comparison_db: Database,
) -> None:
    db = comparison_db
    matching, proposal_id, oldest = registered_proposal(db)
    before = InvestmentMembershipService(db).project()
    install_resolvers(db)
    for row in before:
        assert db.execute(
            "SELECT active_investment_transaction_id, resolution_status FROM core.alias_investment_transaction_ids WHERE requested_investment_transaction_id = ?",
            [row["investment_transaction_id"]],
        ).fetchone() == (row["investment_transaction_id"], "active")
    with operation():
        matching.decide(proposal_id, decision="accept", actor="test")
    for row in before:
        expected = (
            "active"
            if row["investment_transaction_id"] == oldest["investment_transaction_id"]
            else "forwarded"
        )
        assert db.execute(
            "SELECT active_investment_transaction_id, resolution_status FROM core.alias_investment_transaction_ids WHERE requested_investment_transaction_id = ?",
            [row["investment_transaction_id"]],
        ).fetchone() == (oldest["investment_transaction_id"], expected)
        assert db.execute(
            "SELECT active_event_group_id, resolution_status FROM core.alias_investment_event_ids WHERE requested_event_group_id = ?",
            [row["event_group_id"]],
        ).fetchone() == (oldest["event_group_id"], expected)
    assert db.execute(
        "SELECT COUNT(*) FROM core.alias_investment_transaction_ids WHERE requested_investment_transaction_id IN ('manual', 'native')"
    ).fetchone() == (0,)
    assert db.execute(
        "SELECT COUNT(*) FROM core.alias_investment_transaction_ids WHERE resolution_status = 'forwarded' AND active_investment_transaction_id NOT IN (SELECT investment_transaction_id FROM app.investment_event_members WHERE retired_at IS NULL)"
    ).fetchone() == (0,)


def test_vanished_reconstruction_is_terminally_retired(comparison_db: Database) -> None:
    db = comparison_db
    seed_plaid_event(db, "buy")
    seed_snapshot(db, "first")
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    opening = next(
        r for r in membership.project() if r["subtype"] == "opening_bootstrap"
    )
    install_resolvers(db)
    revise(db, "5")
    membership.reconcile(actor="test")
    assert db.execute(
        "SELECT active_investment_transaction_id, resolution_status FROM core.alias_investment_transaction_ids WHERE requested_investment_transaction_id = ?",
        [opening["investment_transaction_id"]],
    ).fetchone() == (None, "retired")
    assert db.execute(
        "SELECT active_event_group_id, resolution_status FROM core.alias_investment_event_ids WHERE requested_event_group_id = ?",
        [opening["event_group_id"]],
    ).fetchone() == (None, "retired")


def test_decomposed_match_history_never_forwards_to_one_of_multiple_successors(
    comparison_db: Database,
) -> None:
    from uuid import uuid4

    from moneybin.investments.event_membership import EventMember, MembershipBinding
    from moneybin.repositories.investment_event_members_repo import (
        InvestmentEventMembersRepo,
    )

    db = comparison_db
    matching, proposal_id, oldest = registered_proposal(db)
    with operation():
        matching.decide(proposal_id, decision="accept", actor="test")
    repo = InvestmentEventMembersRepo(db)
    prior = repo.active_members()
    # Construct the membership-history shape independently of the reversal owner.
    db.begin()
    try:
        for row in prior:
            repo.retire(row["membership_id"], actor="test", in_outer_txn=True)
        for row in prior:
            binding = MembershipBinding(
                account_id=row["bound_account_id"],
                security_id=row["bound_security_id"],
                effective_currency=row["bound_effective_currency"],
                currency_source=row["currency_source"],
                account_generation=row["account_generation"],
                security_generation=row["security_generation"],
                currency_generation=row["currency_generation"],
            )
            repo.register(
                EventMember(
                    event_group_id=uuid4().hex[:12],
                    investment_transaction_id=uuid4().hex[:12],
                    source_event_key=row["source_event_key"],
                    source_type=row["source_type"],
                    source_origin=row["source_origin"],
                    native_reference=row["native_reference"],
                    observation_version=row["observation_version"],
                    semantic_leg_role=row["semantic_leg_role"],
                    binding=binding,
                ),
                actor="test",
                in_outer_txn=True,
            )
        db.commit()
    except BaseException:
        db.rollback()
        raise
    install_resolvers(db)
    assert db.execute(
        "SELECT active_event_group_id, resolution_status FROM core.alias_investment_event_ids WHERE requested_event_group_id = ?",
        [oldest["event_group_id"]],
    ).fetchone() == (None, "retired")
    assert db.execute(
        "SELECT active_investment_transaction_id, resolution_status FROM core.alias_investment_transaction_ids WHERE requested_investment_transaction_id = ?",
        [oldest["investment_transaction_id"]],
    ).fetchone() == (None, "retired")
    assert db.execute(
        "SELECT COUNT(*) FROM core.alias_investment_event_ids WHERE requested_event_group_id = active_event_group_id AND resolution_status != 'active'"
    ).fetchone() == (0,)
