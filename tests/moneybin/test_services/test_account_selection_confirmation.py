"""Open account merge approvals bind the exact lot selections they rewrite."""

from contextlib import nullcontext
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal

import pytest
from pytest_mock import MockerFixture

from moneybin import error_codes
from moneybin.cli.commands.accounts import links as cli_links
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.mcp.confirmation import ConfirmationBroker
from moneybin.mcp.tools import accounts as mcp_accounts
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.services.account_links_service import AccountLinksService
from tests.moneybin.test_services.test_account_identity_selections import (
    seed_account_selections,
)


@pytest.mark.parametrize("surface", ["cli", "mcp"])
@pytest.mark.parametrize("change", ["disposal", "lot", "quantity", "unchanged"])
def test_account_merge_confirmation_binds_exact_selections(
    db: Database,
    mocker: MockerFixture,
    surface: Literal["cli", "mcp"],
    change: str,
) -> None:
    original_lot = seed_account_selections(db)
    other_lot = compute_lot_id("a", "security", date(2026, 1, 1), "other_buy")
    db.execute("""
        INSERT INTO core.fct_investment_transactions (
            investment_transaction_id, account_id, security_id, type, trade_date,
            quantity, currency_code
        ) VALUES ('other_buy', 'a', 'security', 'buy', '2026-01-01', 10, 'USD'),
                 ('other_sell', 'a', 'security', 'sell', '2026-02-02', -2, 'USD')
    """)
    db.execute(
        """
        INSERT INTO core.fct_investment_lots (
            lot_id, account_id, security_id, acquisition_date, acquisition_type,
            source_transaction_id, currency_code, original_quantity,
            remaining_quantity
        ) VALUES (?, 'a', 'security', '2026-01-01', 'buy', 'other_buy', 'USD', 10, 10)
        """,
        [other_lot],
    )
    mocker.patch.object(AccountLinksService, "rematch_after_merge", return_value=None)
    mocker.patch.object(cli_links, "get_database", return_value=nullcontext(db))
    mocker.patch.object(mcp_accounts, "get_database", return_value=nullcontext(db))
    service = AccountLinksService(db)
    impact = service.accept_impact("a_b_", target_account_id="b")
    preview = cli_links._merge_preview("a_b_", "b")  # pyright: ignore[reportPrivateUsage]  # test exercises confirmation seam
    assert preview is not None
    binding = mcp_accounts._account_link_binding(  # pyright: ignore[reportPrivateUsage]  # test exercises confirmation seam
        decision_id="a_b_", target_account_id="b", impact=impact
    )
    broker = ConfirmationBroker()
    now = datetime.now(UTC)
    grant = broker.consume(broker.issue(binding, now=now), now=now)
    selections = LotSelectionsRepo(db)
    if change == "disposal":
        selections.set_for_disposal(
            investment_transaction_id="sell", selections=[], actor="cli"
        )
    if change != "unchanged":
        selections.set_for_disposal(
            investment_transaction_id="other_sell" if change == "disposal" else "sell",
            selections=[
                (
                    other_lot if change == "lot" else original_lot,
                    Decimal("1") if change == "quantity" else Decimal("2"),
                )
            ],
            actor="cli",
        )
    assert (
        service.accept_impact("a_b_", target_account_id="b").blast_radius
        == impact.blast_radius
    )
    before = {
        table: db.execute(f"SELECT * FROM app.{table} ORDER BY ALL").fetchall()  # noqa: S608  # fixed test table names
        for table in (
            "account_links",
            "account_link_decisions",
            "lot_selections",
            "audit_log",
        )
    }  # fixed test table names

    def apply() -> None:
        if surface == "cli":
            service.set(
                "a_b_",
                target_account_id="b",
                verify_accept=cli_links._drift_check(db, "a_b_", preview[0]),  # pyright: ignore[reportPrivateUsage]  # test exercises confirmation seam
            )
        else:
            mcp_accounts._apply_account_accept("a_b_", "b", grant)  # pyright: ignore[reportPrivateUsage]  # test exercises confirmation seam

    if change == "unchanged":
        apply()
        assert selections.list_for_disposal("sell") == [
            (compute_lot_id("b", "security", date(2026, 1, 1), "buy"), Decimal("2"))
        ]
    else:
        with pytest.raises(UserError) as exc:
            apply()
        assert exc.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
        for table, rows in before.items():
            assert (
                db.execute(f"SELECT * FROM app.{table} ORDER BY ALL").fetchall() == rows  # noqa: S608  # fixed test table names
            )
