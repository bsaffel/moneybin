"""Planner lifecycle rows survive service exit without touching the ledger."""

from moneybin.database import Database
from moneybin.services.investment_matching_service import InvestmentMatchingService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


def _seed(db: Database) -> None:
    seed_manual_event(db, "manual")
    seed_plaid_event(db, "native")
    install_comparison_models(db)


def test_run_persists_pending_and_repeated_planning_does_not_duplicate(
    comparison_db: Database,
) -> None:
    _seed(comparison_db)
    first = InvestmentMatchingService(comparison_db).run()
    assert first.proposed == 1
    rows = InvestmentMatchingService(comparison_db).pending()
    assert len(rows) == 1
    assert rows[0]["status"] == "pending"
    assert len(rows[0]["legs"]) == 2
    assert rows[0]["evidence"]
    second = InvestmentMatchingService(comparison_db).run()
    assert second.proposed == 0
    assert second.suppressed == 0
    assert second.unchanged == 1
    assert InvestmentMatchingService(comparison_db).pending() == rows


def test_arriving_alternative_stales_old_pending_and_preserves_history(
    comparison_db: Database,
) -> None:
    _seed(comparison_db)
    service = InvestmentMatchingService(comparison_db)
    service.run()
    old = service.pending()[0]["proposal_id"]
    seed_plaid_event(comparison_db, "alternative")
    result = service.run()
    assert result.stale == 1
    assert result.proposed == 2
    assert len(service.pending()) == 2
    assert all(row["is_competing"] for row in service.pending())
    assert any(
        row["proposal_id"] == old and row["status"] == "stale"
        for row in service.history()
    )


def test_planning_preserves_raw_and_existing_core_rows(
    comparison_db: Database,
) -> None:
    _seed(comparison_db)
    comparison_db.execute(
        "CREATE TABLE core.fct_investment_transactions AS SELECT 42 AS sentinel"
    )
    before = comparison_db.execute(
        "SELECT * FROM raw.manual_investment_transactions"
    ).fetchall()
    InvestmentMatchingService(comparison_db).run()
    assert (
        comparison_db.execute(
            "SELECT * FROM raw.manual_investment_transactions"
        ).fetchall()
        == before
    )
    assert comparison_db.execute(
        "SELECT * FROM core.fct_investment_transactions"
    ).fetchall() == [(42,)]


def test_display_only_identity_edits_preserve_pending_proposal(
    comparison_db: Database,
) -> None:
    _seed(comparison_db)
    service = InvestmentMatchingService(comparison_db)
    service.run()
    original = service.pending()[0]["proposal_id"]
    comparison_db.execute(
        "ALTER TABLE core.dim_accounts ADD COLUMN display_name VARCHAR"
    )
    comparison_db.execute("UPDATE core.dim_accounts SET display_name = 'New display'")
    result = service.run()
    assert result.proposed == result.stale == 0
    assert service.pending()[0]["proposal_id"] == original


def test_inherited_account_currency_changes_stale_proposal(
    comparison_db: Database,
) -> None:
    _seed(comparison_db)
    service = InvestmentMatchingService(comparison_db)
    service.run()
    original = service.pending()[0]["proposal_id"]
    comparison_db.execute("UPDATE core.dim_accounts SET currency_code = 'CAD'")
    result = service.run()
    assert result.stale == 1
    assert service.pending()[0]["proposal_id"] != original
