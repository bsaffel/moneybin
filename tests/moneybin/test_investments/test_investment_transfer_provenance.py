"""Reviewed transfers retain manual basis and date through real Core rebuilds."""

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.investments.event_projection import read_rows
from moneybin.investments.freshness import projection_pending
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.repositories.investment_match_field_resolutions_repo import (
    InvestmentMatchFieldResolutionsRepo,
)
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.mutation_context import operation
from moneybin.services.transform_service import TransformService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_plaid_event,
)
from tests.moneybin.test_investments.test_investment_golden_projection import ledger

pytestmark = pytest.mark.integration


def _raw(db: Database) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        read_rows(
            db,
            """SELECT * FROM raw.manual_investment_transactions
               ORDER BY source_transaction_id""",
        ),
        read_rows(
            db,
            """SELECT * FROM raw.plaid_investment_transactions
               ORDER BY investment_transaction_id, observation_version""",
        ),
    )


def _assert_finances(db: Database, transaction_id: str) -> None:
    assert not projection_pending(db)
    assert db.execute(
        """SELECT account_id, security_id, source_transaction_id,
                  acquisition_date, acquisition_type, original_quantity,
                  remaining_quantity, cost_basis_total, cost_basis_remaining,
                  basis_incomplete, currency_code
           FROM core.fct_investment_lots"""
    ).fetchall() == [
        (
            "account",
            "security",
            transaction_id,
            date(2020, 3, 4),
            "transfer_in",
            Decimal("4"),
            Decimal("4"),
            Decimal("400"),
            Decimal("400"),
            False,
            "USD",
        )
    ]
    assert db.execute("SELECT * FROM core.fct_realized_gains").fetchall() == []
    assert db.execute(
        "SELECT quantity, cost_basis, average_cost FROM core.dim_holdings"
    ).fetchall() == [(Decimal("4"), Decimal("400"), Decimal("100"))]


def _assert_provenance(
    db: Database,
    *,
    transaction_id: str,
    group_id: str,
    proposal_id: str,
    revisions: set[tuple[Any, ...]],
    manual_version: str,
) -> None:
    members = InvestmentEventMembersRepo(db).active_members()
    assert len(members) == 2
    assert {
        (
            m["source_type"],
            m["source_origin"],
            m["native_reference"],
            m["observation_version"],
        )
        for m in members
    } == revisions
    assert {
        (
            m["investment_transaction_id"],
            m["event_group_id"],
            m["member_kind"],
            m["semantic_leg_role"],
            m["bound_account_id"],
            m["bound_security_id"],
            m["bound_effective_currency"],
            m["decision_id"],
        )
        for m in members
    } == {
        (
            transaction_id,
            group_id,
            "source_event",
            "acquisition",
            "account",
            "security",
            "USD",
            proposal_id,
        )
    }
    # Defaults have no curation row: recover the exact supplier from membership.
    assert db.execute(
        """SELECT l.source_type, l.source_origin, l.native_reference,
                  l.observation_version, l.original_acquisition_date, l.amount
           FROM app.investment_event_members AS m
           JOIN prep.int_investment_events__legs AS l
             ON l.source_type = m.source_type AND l.source_origin = m.source_origin
            AND l.native_reference = m.native_reference
            AND l.observation_version = m.observation_version
           WHERE m.retired_at IS NULL AND m.investment_transaction_id = ?
             AND (l.original_acquisition_date IS NOT NULL OR l.amount IS NOT NULL)""",
        [transaction_id],
    ).fetchall() == [
        (
            "manual",
            "user",
            "manual_transfer",
            manual_version,
            date(2020, 3, 4),
            Decimal("-400"),
        )
    ]
    assert InvestmentMatchFieldResolutionsRepo(db).active_resolutions() == []


def test_reviewed_transfer_preserves_present_manual_fields_and_exact_revisions(
    comparison_db: Database,
) -> None:
    db = comparison_db
    # The shared manual factory has no original-acquisition-date argument.
    db.execute(
        """INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id, type,
            trade_date, original_acquisition_date, quantity, price, amount,
            fees, description, created_by, investment_transaction_id
        ) VALUES ('manual_transfer', 'import', 'account', 'security', 'transfer_in',
                  DATE '2026-01-10', DATE '2020-03-04', 4, 100, -400, 0,
                  'Manual basis and holding period', 'cli', 'manual_transfer')"""
    )
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    (oldest,) = membership.project()
    seed_plaid_event(
        db,
        "plaid_transfer",
        type_="transfer",
        subtype="transfer",
        quantity="4",
        amount="0",
    )
    built = TransformService(db).apply()
    assert built.applied, built.error
    before = ledger(db)
    assert len(before) == 2
    assert {row["type"] for row in before} == {"transfer_in"}
    assert {row["reconciliation_status"] for row in before} == {"unreconciled"}
    source_rows = db.execute(
        """SELECT source_type, source_origin, native_reference, observation_version,
                  original_acquisition_date, amount
           FROM prep.int_investment_events__legs ORDER BY source_type"""
    ).fetchall()
    assert len(source_rows) == 2
    manual, plaid = source_rows
    assert (manual[:3], manual[4:]) == (
        ("manual", "user", "manual_transfer"),
        (date(2020, 3, 4), Decimal("-400")),
    )
    assert (plaid[:3], plaid[4:]) == (
        ("plaid", "origin", "plaid_transfer"),
        (None, None),
    )
    assert manual[3] and plaid[3]
    revisions = {row[:4] for row in source_rows}
    raw = _raw(db)
    matching = InvestmentMatchingService(db)
    matching.run(actor="test")
    assert ledger(db) == before
    (proposal,) = matching.pending()
    assert proposal["field_choices"] == []
    assert proposal["is_competing"] is False
    # Missing aggregator basis is eligible present-value evidence, not exact cash.
    assert proposal["confidence_band"] == "fuzzy"
    assert proposal["auto_eligible"] is False
    with operation():
        matching.decide(proposal["proposal_id"], decision="accept", actor="test")

    (transfer,) = ledger(db)
    assert transfer == {
        "investment_transaction_id": oldest["investment_transaction_id"],
        "event_group_id": oldest["event_group_id"],
        "account_id": "account",
        "security_id": "security",
        "type": "transfer_in",
        "subtype": None,
        "provider_type": "transfer",
        "provider_subtype": "transfer",
        "source_type": "plaid",
        "source_origin": "origin",
        "description": "Aggregator context",
        "reconciliation_status": "reconciled",
        "currency_code": "USD",
        "quantity": Decimal("4"),
        "price": Decimal("100"),
        "amount": Decimal("-400"),
        "fees": Decimal("0"),
        "trade_date": date(2026, 1, 10),
        "settlement_date": date(2026, 1, 10),
        "original_acquisition_date": date(2020, 3, 4),
        "updated_at": transfer["updated_at"],
    }
    _assert_finances(db, oldest["investment_transaction_id"])
    _assert_provenance(
        db,
        transaction_id=oldest["investment_transaction_id"],
        group_id=oldest["event_group_id"],
        proposal_id=proposal["proposal_id"],
        revisions=revisions,
        manual_version=manual[3],
    )
    assert matching.pending() == []
    assert _raw(db) == raw
    members = InvestmentEventMembersRepo(db).history()
    built = TransformService(db).apply()
    assert built.applied, built.error
    assert ledger(db) == [transfer]
    assert InvestmentEventMembersRepo(db).history() == members
    _assert_finances(db, oldest["investment_transaction_id"])
    _assert_provenance(
        db,
        transaction_id=oldest["investment_transaction_id"],
        group_id=oldest["event_group_id"],
        proposal_id=proposal["proposal_id"],
        revisions=revisions,
        manual_version=manual[3],
    )
    assert _raw(db) == raw
