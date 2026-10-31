"""Core projects exact published membership, including retained opening evidence."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlglot import exp
from sqlmesh.core.dialect import parse
from sqlmesh.core.macros import MacroEvaluator

import moneybin.sqlmesh.macros.investment_projection  # pyright: ignore[reportUnusedImport]  # noqa: F401  # register production macro
from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.investments.event_projection import read_rows
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
from tests.moneybin.test_investments.test_investment_opening_membership import (
    revise,
    seed_snapshot,
)


def install_model(db: Database, name: str) -> None:
    schema, model = name.split(".")
    path = Path("src/moneybin/sqlmesh/models") / schema / f"{model}.sql"
    assert path.exists(), f"Missing Golden model: {name}"
    statement = parse(path.read_text(), default_dialect="duckdb")[-1]
    query = MacroEvaluator(dialect="duckdb").transform(statement)
    assert isinstance(query, exp.Query)
    db.execute(
        f"CREATE OR REPLACE VIEW {name} AS {query.sql(dialect='duckdb')}"
    )  # fixed local model names and parsed query


def install_golden_models(db: Database) -> None:
    db.execute("""CREATE OR REPLACE VIEW prep.int_accounts__merged AS
        SELECT a.account_id, s.investment_source_type, s.investment_source_type_changed_at
        FROM core.dim_accounts AS a LEFT JOIN app.account_settings AS s USING (account_id)""")
    for name in (
        "prep.int_investment_events__source_projection",
        "prep.int_investment_events__opening_projection",
        "core.fct_investment_transactions",
    ):
        install_model(db, name)


def ledger(db: Database) -> list[dict[str, Any]]:
    return read_rows(
        db,
        "SELECT * FROM core.fct_investment_transactions ORDER BY investment_transaction_id",
    )


def test_core_hard_cut_preserves_sources_as_provenance_and_projects_each_leg_once(
    comparison_db: Database,
) -> None:
    db = comparison_db
    seed_manual_event(db, "manual")
    seed_plaid_event(db, "native")
    seed_manual_event(
        db, "other", type_="other", quantity=None, price=None, amount="-5"
    )
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    install_golden_models(db)
    rows = ledger(db)
    assert len(rows) == 3
    assert len({r["investment_transaction_id"] for r in rows}) == 3
    assert len({r["event_group_id"] for r in rows}) == 3
    assert all(
        len(r["investment_transaction_id"]) == len(r["event_group_id"]) == 12
        for r in rows
    )
    assert not {r["investment_transaction_id"] for r in rows} & {
        "manual",
        "native",
        "other",
    }
    assert sorted(r["amount"] for r in rows) == [
        Decimal(-100),
        Decimal(-100),
        Decimal(-5),
    ]
    assert db.execute(
        "SELECT investment_transaction_id FROM prep.stg_plaid__investment_transactions"
    ).fetchall() == [("native",)]
    assert {
        r[0]
        for r in db.execute(
            "SELECT source_transaction_id FROM raw.manual_investment_transactions"
        ).fetchall()
    } == {"manual", "other"}
    assert {r["investment_transaction_id"] for r in rows} == {
        r["investment_transaction_id"] for r in membership.project()
    }


def test_core_keeps_reviewed_revision_identity_and_currency_while_match_is_stale(
    comparison_db: Database,
) -> None:
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    db = comparison_db
    matching, proposal_id, oldest = registered_proposal(db)
    with operation():
        matching.decide(proposal_id, decision="accept", actor="test")
    install_golden_models(db)
    before = ledger(db)
    assert len(before) == 1 and before[0]["amount"] == Decimal(-100)
    changed = SyncInvestmentTransaction(
        investment_transaction_id="native",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, 10),
        name="Corrected",
        quantity=Decimal(1),
        amount=Decimal(150),
        price=Decimal(150),
        fees=Decimal(0),
        type="buy",
        subtype="buy",
    )
    at = datetime(2026, 2, 2, tzinfo=UTC)
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # retained synthetic revision
        [changed], "new_revision", at, at
    )
    db.execute("INSERT INTO core.dim_accounts VALUES ('other_account', 'CAD')")
    AccountLinksRepo(db).repoint(
        link_id="account_link",
        new_account_id="other_account",
        decided_by="user",
        actor="test",
    )
    db.execute(
        "UPDATE core.dim_accounts SET currency_code = 'CAD' WHERE account_id = 'account'"
    )
    InvestmentMembershipService(db).reconcile(actor="test")
    assert matching.history()[0]["status"] == "stale"
    assert ledger(db) == before
    assert before[0]["account_id"] == "account" and before[0]["currency_code"] == "USD"
    assert before[0]["investment_transaction_id"] == oldest["investment_transaction_id"]


def test_core_reconstructs_openings_from_selected_membership_inputs(
    comparison_db: Database,
) -> None:
    db = comparison_db
    seed_plaid_event(db, "buy")
    seed_snapshot(db, "first")
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    install_golden_models(db)
    before = ledger(db)
    opening = next(r for r in before if r["subtype"] == "opening_bootstrap")
    assert opening["quantity"] == Decimal(4)
    assert sum(r["quantity"] for r in before) == 5
    revise(db, "2")
    assert ledger(db) == before
    membership.reconcile(actor="test")
    after = ledger(db)
    advanced = next(r for r in after if r["subtype"] == "opening_bootstrap")
    assert advanced["quantity"] == Decimal(3)
    assert advanced["investment_transaction_id"] == opening["investment_transaction_id"]
    assert sum(r["quantity"] for r in after) == 5


def test_core_uses_explicit_choice_with_deterministic_representative(
    comparison_db: Database,
) -> None:
    from moneybin.services.investment_matching_service import InvestmentMatchingService

    db = comparison_db
    seed_manual_event(db, "manual", day=9)
    seed_plaid_event(db, "native", day=10, explicit_date=True)
    install_comparison_models(db)
    InvestmentMembershipService(db).reconcile(actor="test")
    matching = InvestmentMatchingService(db)
    matching.run()
    proposal = matching.pending()[0]
    conflict = next(c for c in proposal["field_choices"] if c["field"] == "trade_date")
    choice = next(c for c in conflict["choices"] if c["source_type"] == "manual")
    with operation():
        matching.decide(
            proposal["proposal_id"],
            decision="accept",
            actor="test",
            field_choices=[
                {
                    "conflict_id": conflict["conflict_id"],
                    "choice_id": choice["choice_id"],
                }
            ],
        )
    install_golden_models(db)
    rows = ledger(db)
    assert len(rows) == 1 and rows[0]["trade_date"] == date(2026, 1, 9)
    assert rows[0]["source_type"] == "plaid" and rows[0]["provider_type"] == "buy"


def test_explicit_source_choice_matches_core_and_preflight_ledger(
    comparison_db: Database,
) -> None:
    from moneybin.repositories.account_settings_repo import AccountSettingsRepo

    db = comparison_db
    seed_manual_event(db, "manual")
    seed_plaid_event(db, "native")
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    AccountSettingsRepo(db).set(
        account_id="account",
        display_name=None,
        official_name=None,
        last_four=None,
        account_subtype=None,
        holder_category=None,
        currency_code=None,
        credit_limit=None,
        archived=False,
        archived_at=None,
        include_in_net_worth=True,
        default_cost_basis_method=None,
        investment_source_type="manual",
        investment_source_type_changed_at=datetime.now(UTC),
        archived_changed_at=None,
        currency_code_changed_at=None,
        default_cost_basis_method_changed_at=None,
        actor="test",
    )
    install_golden_models(db)
    rows = ledger(db)
    assert len(rows) == 1 and rows[0]["source_type"] == "manual"
    projected = membership.project()
    assert len(projected) == 1
    assert (
        projected[0]["investment_transaction_id"]
        == rows[0]["investment_transaction_id"]
    )
    assert projected[0]["updated_at"] == rows[0]["updated_at"]
