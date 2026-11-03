"""Synthetic adapter-contract and real-SQL repeated-trade lifecycle proof.

The third type supplies normalized observations and retained evidence at the
adapter boundary. This does not prove ingestion or a production evidence loader
for that type. No accepted membership or decision is inserted by the fixture.
"""

from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.investments.event_projection import (
    SourceRevision,
    load_member_evidence,
    read_rows,
    source_revision,
)
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.audit_service import AuditService
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)
from tests.moneybin.test_investments.test_investment_match_acceptance import (
    registered_proposal,
)


def _install_synthetic_adapter_contract(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Supply one normalized immutable observation, not an importer."""
    db.execute("""CREATE TABLE prep.synthetic_adapter_base AS
        SELECT * FROM prep.int_investment_events__observations""")
    db.execute("""CREATE TABLE prep.synthetic_adapter_observations AS
        SELECT * REPLACE (
            'synthetic_broker' AS source_type,
            'synthetic_origin' AS source_origin,
            'synthetic_trade' AS native_reference,
            'synthetic_broker_revision_1' AS observation_version,
            'synthetic_trade' AS original_investment_transaction_id
        ) FROM prep.synthetic_adapter_base WHERE source_type = 'manual'""")
    assert db.execute(
        "SELECT COUNT(*) FROM prep.synthetic_adapter_observations"
    ).fetchone() == (1,)
    db.execute("""CREATE OR REPLACE VIEW prep.int_investment_events__observations AS
        SELECT * FROM prep.synthetic_adapter_base
        UNION ALL SELECT * FROM prep.synthetic_adapter_observations""")
    db.execute("""CREATE TABLE prep.synthetic_adapter_revisions AS
        SELECT l.*, NULL::VARCHAR AS provider_type,
            NULL::VARCHAR AS provider_subtype
        FROM prep.int_investment_events__legs AS l
        WHERE source_type = 'synthetic_broker'""")

    # The shipped loader knows manual and Plaid only. Supply retained normalized
    # evidence explicitly; the SQL comparison and every membership write stay real.
    def normalized_evidence(database: Database) -> dict[SourceRevision, dict[str, Any]]:
        evidence = load_member_evidence(database)
        rows = read_rows(
            database,
            "SELECT * FROM prep.synthetic_adapter_revisions",
        )
        evidence.update({source_revision(row): row for row in rows})
        return evidence

    monkeypatch.setattr(
        "moneybin.services.investment_membership_service.load_member_evidence",
        normalized_evidence,
    )


def _native_pairs(proposals: list[dict[str, Any]]) -> set[frozenset[str]]:
    return {
        frozenset(leg["native_reference"] for leg in proposal["legs"])
        for proposal in proposals
    }


def test_third_source_type_contract_preserves_golden_ids_through_accept_and_undo(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = comparison_db
    matching, original_id, oldest = registered_proposal(db)
    with operation():
        matching.decide(original_id, decision="accept", actor="test")
    membership = InvestmentMembershipService(db)
    original = membership.project()[0]
    expected_ids = (oldest["event_group_id"], oldest["investment_transaction_id"])
    assert (original["event_group_id"], original["investment_transaction_id"]) == (
        expected_ids
    )
    _install_synthetic_adapter_contract(db, monkeypatch)
    assert (
        db.execute("""SELECT DISTINCT source_type
        FROM prep.int_investment_events__headers ORDER BY source_type""").fetchall()
        == [
            ("manual",),
            ("plaid",),
            ("synthetic_broker",),
        ]
    )
    assert db.execute("""SELECT COUNT(*) FROM prep.int_investment_events__evidence
        WHERE is_candidate""").fetchone() == (3,)
    membership.reconcile(actor="test")
    repo = InvestmentEventMembersRepo(db)
    third = next(
        row for row in repo.active_members() if row["source_type"] == "synthetic_broker"
    )
    assert third["decision_id"] is None
    matching.run()
    proposals = matching.pending()
    assert len(proposals) == 1
    replacement = proposals[0]
    assert _native_pairs(proposals) == {
        frozenset({"manual", "native", "synthetic_trade"})
    }
    assert replacement["supersedes_decision_ids"] == [original_id]
    assert replacement["auto_eligible"] is False
    with operation() as accepted:
        matching.decide(replacement["proposal_id"], decision="accept", actor="test")
    active = repo.active_members()
    assert {
        (row["source_type"], row["native_reference"], row["observation_version"])
        for row in active
        if row["source_type"] == "synthetic_broker"
    } == {("synthetic_broker", "synthetic_trade", "synthetic_broker_revision_1")}
    assert len(active) == 3
    assert {row["decision_id"] for row in active} == {replacement["proposal_id"]}
    assert {
        (row["event_group_id"], row["investment_transaction_id"]) for row in active
    } == {expected_ids}
    projected = membership.project()
    assert len(projected) == 1
    assert (projected[0]["quantity"], projected[0]["amount"]) == (
        Decimal("1"),
        Decimal("-100"),
    )
    assert {row["proposal_id"]: row["status"] for row in matching.history()} == {
        original_id: "reversed",
        replacement["proposal_id"]: "accepted",
    }
    assert any(
        event.action == "investment_match.supersede"
        for event in AuditService(db).events_for_operation(accepted)
    )

    UndoService(db).undo(accepted, actor="test")
    restored = repo.active_members()
    assert len(restored) == 3
    assert {
        row["decision_id"]
        for row in restored
        if row["source_type"] in {"manual", "plaid"}
    } == {original_id}
    restored_third = next(
        row for row in restored if row["source_type"] == "synthetic_broker"
    )
    assert restored_third["decision_id"] is None
    assert (
        restored_third["event_group_id"],
        restored_third["investment_transaction_id"],
    ) == (third["event_group_id"], third["investment_transaction_id"])
    restored_original = next(row for row in restored if row["source_type"] == "manual")
    assert (
        restored_original["event_group_id"],
        restored_original["investment_transaction_id"],
    ) == expected_ids
    assert len(membership.project()) == 2
    assert {row["proposal_id"]: row["status"] for row in matching.history()} == {
        original_id: "accepted",
        replacement["proposal_id"]: "reversed",
    }


@pytest.mark.parametrize("ambiguous", [False, True], ids=["unique", "ambiguous"])
def test_real_sql_repeated_trades_keep_exact_assignment_and_ambiguity_visible(
    comparison_db: Database, ambiguous: bool
) -> None:
    db = comparison_db
    # Both histories repeat the same security, quantity and cash. Distinct dates
    # make only the diagonal exact; equal dates leave both assignments optimal.
    seed_manual_event(db, "manual_1", day=10)
    seed_manual_event(db, "manual_2", day=10 if ambiguous else 11)
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    oldest_ids = {
        row["native_reference"]: (
            row["event_group_id"],
            row["investment_transaction_id"],
        )
        for row in membership.project()
    }
    seed_plaid_event(db, "native_1", day=10)
    seed_plaid_event(db, "native_2", day=10 if ambiguous else 11)
    membership.reconcile(actor="test")
    before = InvestmentEventMembersRepo(db).history()
    evidence = db.execute("""SELECT left_native_reference, right_native_reference,
        is_candidate, is_exact_economic_identity
        FROM prep.int_investment_events__evidence
        ORDER BY left_native_reference, right_native_reference""").fetchall()
    assert evidence == [
        ("manual_1", "native_1", True, True),
        ("manual_1", "native_2", True, ambiguous),
        ("manual_2", "native_1", True, ambiguous),
        ("manual_2", "native_2", True, True),
    ]
    matching = InvestmentMatchingService(db)
    matching.run()
    pending = matching.pending()
    if ambiguous:
        assert _native_pairs(pending) == {
            frozenset({"manual_1", "native_1"}),
            frozenset({"manual_1", "native_2"}),
            frozenset({"manual_2", "native_1"}),
            frozenset({"manual_2", "native_2"}),
        }
        assert all(row["is_competing"] and not row["auto_eligible"] for row in pending)
        assert {row["status"] for row in pending} == {"pending"}
        assert InvestmentEventMembersRepo(db).history() == before
        assert len(membership.project()) == 4
        return

    assert _native_pairs(pending) == {
        frozenset({"manual_1", "native_1"}),
        frozenset({"manual_2", "native_2"}),
    }
    assert all(not row["is_competing"] and row["auto_eligible"] for row in pending)
    assert InvestmentEventMembersRepo(db).history() == before
    for index in (1, 2):
        matching.run()
        pair = {f"manual_{index}", f"native_{index}"}
        proposal = next(
            row
            for row in matching.pending()
            if {leg["native_reference"] for leg in row["legs"]} == pair
        )
        with operation():
            matching.decide(proposal["proposal_id"], decision="accept", actor="test")
    projected = membership.project()
    assert len(projected) == 2
    assert {
        (row["event_group_id"], row["investment_transaction_id"]) for row in projected
    } == set(oldest_ids.values())
    assert sorted((row["quantity"], row["amount"]) for row in projected) == [
        (Decimal("1"), Decimal("-100")),
        (Decimal("1"), Decimal("-100")),
    ]
    assert {
        frozenset(
            row["native_reference"]
            for row in InvestmentEventMembersRepo(db).active_members()
            if row["decision_id"] == decision["proposal_id"]
        )
        for decision in matching.history()
        if decision["status"] == "accepted"
    } == {
        frozenset({"manual_1", "native_1"}),
        frozenset({"manual_2", "native_2"}),
    }
