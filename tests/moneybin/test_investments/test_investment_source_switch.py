"""Source choice changes values while preserving the reviewed Match relationship."""

import pytest

from moneybin.database import Database
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.investment_membership_service import InvestmentMembershipService
from tests.moneybin.test_investments.test_investment_match_acceptance import (
    registered_proposal,
)
from tests.moneybin.test_investments.test_investment_membership_dependencies import (
    reviewed_profile,
    settings,
)


def test_manual_only_preserves_reviewed_match_and_golden_ids(
    comparison_db: Database,
) -> None:
    matching, proposal, _ = registered_proposal(comparison_db)
    matching.decide(proposal, decision="accept", actor="test")
    service = InvestmentMembershipService(comparison_db)
    before = service.project()[0]
    history = InvestmentEventMembersRepo(comparison_db).history()
    from moneybin.services.account_service import CLEAR, AccountService

    AccountService(comparison_db).settings_update(
        "account",
        investment_source_type="manual",
        actor="test",
        verify_source_choice=service.preview_source_choice("account", "manual").verify,
    )
    rows = service.project()
    assert len(rows) == 1
    assert rows[0]["source_type"] == "manual"
    assert rows[0]["investment_transaction_id"] == before["investment_transaction_id"]
    assert matching.history()[0]["status"] == "accepted"
    assert InvestmentEventMembersRepo(comparison_db).history() == history
    AccountService(comparison_db).settings_update(
        "account",
        investment_source_type=CLEAR,
        actor="test",
        verify_source_choice=service.preview_source_choice("account", None).verify,
    )
    assert service.project()[0]["source_type"] == "plaid"


def test_source_switch_python_and_sql_projection_agree(db: Database) -> None:
    from moneybin.services.account_service import AccountService
    from moneybin.services.mutation_context import operation

    matching, _ = reviewed_profile(db)
    service = InvestmentMembershipService(db)
    before = service.project()[0]
    preview = service.preview_source_choice("acc_1", "manual")
    assert preview.changes
    with operation():
        AccountService(db).settings_update(
            "acc_1",
            investment_source_type="manual",
            verify_source_choice=preview.verify,
            actor="test",
        )
    expected = service.project()[0]
    assert expected["source_type"] == "manual"
    assert expected["investment_transaction_id"] == before["investment_transaction_id"]
    assert db.execute(
        "SELECT investment_transaction_id, source_type FROM core.fct_investment_transactions"
    ).fetchall() == [(expected["investment_transaction_id"], "manual")]
    assert matching.history()[0]["status"] == "accepted"


def test_source_switch_requires_matching_preview_before_any_write(
    comparison_db: Database,
) -> None:
    from moneybin.errors import UserError
    from moneybin.services.account_service import AccountService
    from moneybin.services.mutation_context import operation

    matching, proposal, _ = registered_proposal(comparison_db)
    matching.decide(proposal, decision="accept", actor="test")
    service = InvestmentMembershipService(comparison_db)
    preview = service.preview_source_choice("account", "manual")
    before = InvestmentEventMembersRepo(comparison_db).history()
    with operation(), pytest.raises(UserError, match="confirmation"):
        AccountService(comparison_db).settings_update(
            "account", investment_source_type="manual", actor="test"
        )
    settings(comparison_db, currency="EUR")
    with operation(), pytest.raises(UserError, match="changed"):
        AccountService(comparison_db).settings_update(
            "account",
            investment_source_type="manual",
            verify_source_choice=preview.verify,
            actor="test",
        )
    assert InvestmentEventMembersRepo(comparison_db).history() == before
    assert comparison_db.execute(
        "SELECT investment_source_type FROM app.account_settings"
    ).fetchone() == (None,)


@pytest.mark.asyncio
async def test_mcp_source_switch_asks_once_and_rechecks_before_write(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from contextlib import contextmanager
    from typing import Any

    from moneybin.mcp.confirmation import ConfirmationGrant, _binding_digest  # pyright: ignore[reportPrivateUsage]  # exercise actual grant verification
    from moneybin.mcp.tools import accounts as tools

    reviewed_profile(db)

    @contextmanager
    def database(**kwargs: Any):
        yield db

    monkeypatch.setattr(tools, "get_database", database)
    messages: list[str] = []

    async def approve(**kwargs: Any) -> ConfirmationGrant:
        messages.append(kwargs["message"])
        assert (
            db.execute(
                "SELECT investment_source_type FROM app.account_settings"
            ).fetchone()
            is None
        )
        return ConfirmationGrant(_binding_digest(kwargs["binding"]))

    monkeypatch.setattr(tools, "grant_confirmation_or_raise", approve)
    result = await tools.accounts_set(
        account_id="acc_1", investment_source_type="manual"
    )
    assert len(messages) == 1
    assert "Matches" in messages[0]
    assert "source: plaid → manual" in messages[0]
    assert result.error is None
    assert db.execute(
        "SELECT source_type FROM core.fct_investment_transactions"
    ).fetchall() == [("manual",)]


def test_cli_source_switch_cancel_preserves_settings_and_match(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from contextlib import contextmanager
    from types import SimpleNamespace
    from typing import Any

    from typer.testing import CliRunner

    from moneybin.cli import app
    from moneybin.cli.commands import accounts as command

    matching, before = reviewed_profile(db)

    @contextmanager
    def database(**kwargs: Any):
        yield db

    monkeypatch.setattr(command, "get_database", database)
    monkeypatch.setattr(
        command, "get_terminal_policy", lambda: SimpleNamespace(interactive=True)
    )
    messages: list[str] = []

    def decline(message: str, **kwargs: Any) -> bool:
        messages.append(message)
        return False

    monkeypatch.setattr(command.typer, "confirm", decline)
    result = CliRunner().invoke(
        app, ["accounts", "set", "acc_1", "--investment-source-type", "manual"]
    )
    assert result.exit_code == 0, result.output
    assert len(messages) == 1
    assert "source: plaid → manual" in result.output
    assert (
        db.execute("SELECT investment_source_type FROM app.account_settings").fetchone()
        is None
    )
    assert InvestmentEventMembersRepo(db).active_members() == before
    assert matching.history()[0]["status"] == "accepted"


def test_source_switch_previews_date_change_and_refuses_lost_selected_lot(
    comparison_db: Database,
) -> None:
    from datetime import date
    from decimal import Decimal

    from moneybin.errors import UserError
    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from moneybin.services.investment_matching_service import InvestmentMatchingService
    from tests.moneybin.test_investments.comparison_helpers import (
        install_comparison_models,
        seed_manual_event,
        seed_plaid_event,
    )

    seed_manual_event(comparison_db, "manual", day=10)
    seed_plaid_event(comparison_db, "native", day=11)
    seed_plaid_event(
        comparison_db,
        "sell",
        type_="sell",
        subtype="sell",
        quantity="-1",
        amount="-100",
        day=10,
    )
    install_comparison_models(comparison_db)
    service = InvestmentMembershipService(comparison_db)
    service.reconcile(actor="test")
    matching = InvestmentMatchingService(comparison_db)
    matching.run()
    assert len(matching.pending()) == 1
    matching.decide(
        matching.pending()[0]["proposal_id"], decision="accept", actor="test"
    )
    rows = service.project()
    buy = next(row for row in rows if row["type"] == "buy")
    sell = next(row for row in rows if row["type"] == "sell")
    preview = service.preview_source_choice("account", "plaid")
    assert preview.changes[buy["investment_transaction_id"]]["trade_date"] == (
        date(2026, 1, 10),
        date(2026, 1, 11),
    )
    lot_id = compute_lot_id(
        "account", "security", date(2026, 1, 10), buy["investment_transaction_id"]
    )
    LotSelectionsRepo(comparison_db).set_for_disposal(
        investment_transaction_id=sell["investment_transaction_id"],
        selections=[(lot_id, Decimal(1))],
        actor="test",
    )
    before = InvestmentEventMembersRepo(comparison_db).history()
    with pytest.raises(UserError, match="selection"):
        service.preview_source_choice("account", "plaid")
    assert InvestmentEventMembersRepo(comparison_db).history() == before
    assert (
        comparison_db.execute(
            "SELECT investment_source_type FROM app.account_settings"
        ).fetchone()
        is None
    )


def test_source_preview_refuses_unreconciled_delivery(db: Database) -> None:
    from datetime import UTC, datetime

    from moneybin.errors import UserError
    from tests.moneybin.test_stg_plaid_investments import _manual_investment_txn  # pyright: ignore[reportPrivateUsage]  # synthetic deferred delivery

    reviewed_profile(db)
    _manual_investment_txn(
        db,
        account_id="acc_1",
        security_id="security",
        source_transaction_id="incoming",
        investment_transaction_id="incoming",
        created_at=datetime.now(UTC),
    )
    before = InvestmentEventMembersRepo(db).history()
    with pytest.raises(UserError) as caught:
        InvestmentMembershipService(db).preview_source_choice("acc_1", "manual")
    assert caught.value.code == "refresh_stale"
    assert InvestmentEventMembersRepo(db).history() == before
