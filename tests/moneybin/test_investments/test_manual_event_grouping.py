"""Manual Source groups are internal, atomic event structure."""

import inspect
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from moneybin.cli import app
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.services.investment_service import InvestmentService
from tests.moneybin.db_helpers import create_core_tables


def _manual_service(db: Database) -> InvestmentService:
    create_core_tables(db)
    db.execute("""
        INSERT INTO core.dim_accounts
            (account_id, account_type, institution_name, source_type)
        VALUES ('acct_brokerage', 'investment', 'Test Institution', 'manual')
    """)
    SecuritiesRepo(db).upsert(
        security_id="security",
        name="Test Security",
        security_type="equity",
        ticker="TEST",
        actor="cli",
    )
    return InvestmentService(db)


def test_cli_cannot_append_to_a_caller_group() -> None:
    with patch(
        "moneybin.cli.commands.investments.get_database",
        side_effect=AssertionError("caller group reached the database"),
    ):
        result = CliRunner().invoke(
            app,
            [
                "investments",
                "add",
                "--account",
                "account",
                "--type",
                "deposit",
                "--date",
                "2026-01-01",
                "--amount",
                "10",
                "--event-group",
                "shared",
            ],
        )
    assert result.exit_code == 2
    assert "No such option" in result.output
    assert "--event-group" in result.output


def test_single_event_interface_excludes_group_reference() -> None:
    assert (
        "event_group_id"
        not in inspect.signature(InvestmentService.record_event).parameters
    )


def test_batch_rejects_group_before_resolving_or_writing(db: Database) -> None:
    event: dict[str, Any] = {
        "account_ref": "unresolved",
        "security_ref": None,
        "type_": "deposit",
        "subtype": None,
        "trade_date": date(2026, 1, 1),
        "quantity": None,
        "price": None,
        "amount": Decimal("10"),
        "fees": None,
        "acquired": None,
        "basis": None,
        "event_group_id": "shared",
        "currency_code": "USD",
        "description": None,
    }
    with patch(
        "moneybin.services.account_service.AccountService.resolve_strict",
        side_effect=AssertionError("caller group reached account resolution"),
    ):
        with pytest.raises(UserError, match="group"):
            InvestmentService(db).record_events([event], actor="mcp", created_by="mcp")
    assert db.execute(
        "SELECT COUNT(*) FROM raw.manual_investment_transactions"
    ).fetchone() == (0,)


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("failure", [RuntimeError, KeyboardInterrupt])
def test_reinvest_failure_never_leaves_one_leg(
    db: Database, batch: bool, failure: type[BaseException]
) -> None:
    service = _manual_service(db)
    event: dict[str, Any] = {
        "account_ref": "acct_brokerage",
        "security_ref": "TEST",
        "type_": "reinvest",
        "subtype": "dividend",
        "trade_date": date(2026, 1, 1),
        "quantity": Decimal("1"),
        "price": Decimal("100"),
        "amount": Decimal("-100"),
        "fees": None,
        "acquired": None,
        "basis": None,
        "currency_code": "USD",
        "description": None,
    }
    original = InvestmentService._insert_event_row  # pyright: ignore[reportPrivateUsage]
    calls = 0

    def insert_then_fail(
        target: InvestmentService,
        *,
        import_id: str,
        account_id: str,
        row: dict[str, object],
    ) -> str:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise failure("injected second-leg failure")
        return original(target, import_id=import_id, account_id=account_id, row=row)

    with patch.object(
        InvestmentService,
        "_insert_event_row",
        autospec=True,
        side_effect=insert_then_fail,
    ):
        with pytest.raises(failure, match="second-leg failure"):
            if batch:
                service.record_events([event], actor="cli", created_by="cli")
            else:
                service.record_event(**event, actor="cli", created_by="cli")
    assert calls == 2
    assert db.execute(
        "SELECT COUNT(*) FROM raw.manual_investment_transactions"
    ).fetchone() == (0,)
    assert db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'investment.record'"
    ).fetchone() == (0,)
    assert db.execute(
        "SELECT status FROM raw.import_log WHERE format_name = 'manual_investment_entry'"
    ).fetchall() == [("failed",)]


@pytest.mark.parametrize("subtype", ["merger", "spin_off", "trade"])
def test_manual_entry_refuses_unsupported_compound_subtypes(
    db: Database, subtype: str
) -> None:
    service = _manual_service(db)
    with pytest.raises(UserError, match="subtype"):
        service.record_event(
            account_ref="acct_brokerage",
            security_ref="TEST",
            type_="transfer_in",
            subtype=subtype,
            trade_date=date(2026, 1, 1),
            quantity=Decimal("1"),
            price=None,
            amount=None,
            fees=None,
            acquired=None,
            basis=None,
            currency_code="USD",
            description=None,
            actor="cli",
            created_by="cli",
        )
    assert db.execute(
        "SELECT COUNT(*) FROM raw.manual_investment_transactions"
    ).fetchone() == (0,)
