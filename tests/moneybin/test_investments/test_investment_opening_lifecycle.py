"""Retained per-account snapshots drive exact rebuilt opening lots.

The first account holds five shares. A $50 pre-window five-share lot supplies
four shares/$40 before a one-share buy, then three shares/$30 after its
two-share correction. The second account first appears in the later receipt
and opens seven shares/$140. A later 50-share snapshot must not replace the
first account's anchor, even when extraction times tie and job names sort first.
"""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from moneybin.connectors.sync_models import (
    InstitutionResult,
    SyncHolding,
    SyncHoldingTaxLot,
)
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.transform_service import TransformService
from tests.moneybin.test_investments.comparison_helpers import (
    seed_account_metadata,
    seed_plaid_event,
)
from tests.moneybin.test_investments.test_investment_opening_membership import revise

pytestmark = pytest.mark.integration


def _snapshot(
    db: Database, receipt: str, positions: list[tuple[str, str, str]]
) -> None:
    """Retain real holdings and receipt inputs with a known pre-window tax lot."""
    holdings = [
        SyncHolding(
            account_id=account,
            provider_item_id="origin",
            security_id="native_security",
            quantity=Decimal(quantity),
            cost_basis=Decimal(basis),
            iso_currency_code="USD",
            tax_lots=[
                SyncHoldingTaxLot(
                    institution_lot_id="retained_lot",
                    original_purchase_datetime=datetime(2024, 1, 1, tzinfo=UTC),
                    quantity=Decimal(quantity),
                    cost_basis=Decimal(basis),
                    position_type="long",
                )
            ],
        )
        for account, quantity, basis in positions
    ]
    at = datetime(2026, 2, 1, tzinfo=UTC)
    extractor = PlaidExtractor(db)
    extractor._load_investment_holdings(  # pyright: ignore[reportPrivateUsage]  # retained synthetic source inputs
        holdings, {"origin": date(2026, 1, 1)}, {"origin"}, receipt, at, at
    )
    extractor._load_holdings_snapshots(  # pyright: ignore[reportPrivateUsage]  # real receipt ordering
        holdings,
        [
            InstitutionResult(
                provider_item_id="origin",
                status="completed",
                transactions_window_start=date(2026, 1, 1),
            )
        ],
        {"origin"},
        receipt,
        at,
        at,
    )


def _openings(db: Database) -> dict[str, dict[str, Any]]:
    return {
        row["bound_account_id"]: row
        for row in InvestmentEventMembersRepo(db).active_members()
        if row["member_kind"] == "opening_lot_reconstruction"
    }


def _assert_opening_finances(
    db: Database, members: dict[str, dict[str, Any]], *, gap: int, buy: int
) -> None:
    ids = {
        account: row["investment_transaction_id"] for account, row in members.items()
    }
    assert db.execute("""SELECT account_id, investment_transaction_id,
        trade_date, original_acquisition_date, quantity, amount
        FROM core.fct_investment_transactions WHERE subtype = 'opening_bootstrap'
        ORDER BY account_id""").fetchall() == [
        (
            "account",
            ids["account"],
            date(2025, 12, 31),
            date(2024, 1, 1),
            Decimal(gap),
            Decimal(-10 * gap),
        ),
        (
            "second_account",
            ids["second_account"],
            date(2025, 12, 31),
            date(2024, 1, 1),
            Decimal(7),
            Decimal(-140),
        ),
    ]
    expected_lots = {
        (account, compute_lot_id(account, "security", date(2024, 1, 1), identity))
        for account, identity in ids.items()
    }
    assert {
        (row[0], row[1])
        for row in db.execute("""SELECT account_id, lot_id
            FROM core.fct_investment_lots WHERE acquisition_type = 'transfer_in'""").fetchall()
    } == expected_lots
    assert db.execute("""SELECT account_id, original_quantity, remaining_quantity,
        cost_basis_total, cost_basis_remaining, basis_incomplete
        FROM core.fct_investment_lots WHERE acquisition_type = 'transfer_in'
        ORDER BY account_id""").fetchall() == [
        (
            "account",
            Decimal(gap),
            Decimal(gap),
            Decimal(10 * gap),
            Decimal(10 * gap),
            False,
        ),
        ("second_account", Decimal(7), Decimal(7), Decimal(140), Decimal(140), False),
    ]
    assert db.execute("""SELECT account_id, quantity, cost_basis
        FROM core.dim_holdings ORDER BY account_id""").fetchall() == [
        ("account", Decimal(5), Decimal(10 * gap + 100 * buy)),
        ("second_account", Decimal(7), Decimal(140)),
    ]


def test_real_rebuild_retains_tied_per_account_snapshot_and_advances_exact_opening(
    comparison_db: Database,
) -> None:
    db = comparison_db
    seed_account_metadata(
        db, "second_account", native="second_native", origin="origin", currency="USD"
    )
    seed_plaid_event(db, "buy")
    _snapshot(db, "z_first", [("native_account", "5", "50")])
    _snapshot(
        db, "a_later", [("native_account", "50", "500"), ("second_native", "7", "140")]
    )
    # Every fixture date and quantity is fixed before the production transform.
    receipts = db.execute("""SELECT source_file, extracted_at, ingestion_sequence
        FROM raw.plaid_investment_holdings_snapshots ORDER BY ingestion_sequence""").fetchall()
    assert [row[0] for row in receipts] == ["z_first", "a_later"]
    assert receipts[0][1] == receipts[1][1]
    assert receipts[0][2] < receipts[1][2]
    built = TransformService(db).apply()
    assert built.applied, built.error
    before = _openings(db)
    assert set(before) == {"account", "second_account"}
    contexts = {
        account: json.loads(row["reconstruction_evidence"])
        for account, row in before.items()
    }
    assert {account: context["snapshot"] for account, context in contexts.items()} == {
        "account": {
            "account_id": "native_account",
            "source_origin": "origin",
            "source_file": "z_first",
        },
        "second_account": {
            "account_id": "second_native",
            "source_origin": "origin",
            "source_file": "a_later",
        },
    }
    buy_member = next(
        row
        for row in InvestmentEventMembersRepo(db).active_members()
        if row["native_reference"] == "buy"
    )
    assert contexts["account"]["source_member_ids"] == [buy_member["membership_id"]]
    assert contexts["second_account"]["source_member_ids"] == []
    _assert_opening_finances(db, before, gap=4, buy=1)

    revise(db, "2")
    # Delivered Raw correction cannot rewrite the published opening evidence.
    assert _openings(db) == before
    assert {
        row["account_id"]: row["quantity"]
        for row in InvestmentMembershipService(db).project()
        if row["subtype"] == "opening_bootstrap"
    } == {"account": Decimal(4), "second_account": Decimal(7)}
    built = TransformService(db).apply()
    assert built.applied, built.error
    after = _openings(db)
    assert {
        account: (row["event_group_id"], row["investment_transaction_id"])
        for account, row in after.items()
    } == {
        account: (row["event_group_id"], row["investment_transaction_id"])
        for account, row in before.items()
    }
    assert after["account"]["membership_id"] != before["account"]["membership_id"]
    assert (
        after["account"]["observation_version"]
        != before["account"]["observation_version"]
    )
    updated = json.loads(after["account"]["reconstruction_evidence"])
    assert updated["snapshot"] == contexts["account"]["snapshot"]
    current_buy = next(
        row
        for row in InvestmentEventMembersRepo(db).active_members()
        if row["native_reference"] == "buy"
    )
    assert current_buy["observation_version"] != buy_member["observation_version"]
    assert updated["source_member_ids"] == [current_buy["membership_id"]]
    assert current_buy["membership_id"] != buy_member["membership_id"]
    assert after["second_account"] == before["second_account"]
    _assert_opening_finances(db, after, gap=3, buy=2)
    history = InvestmentEventMembersRepo(db).history()
    built = TransformService(db).apply()
    assert built.applied, built.error
    _assert_opening_finances(db, after, gap=3, buy=2)
    assert InvestmentEventMembersRepo(db).history() == history
