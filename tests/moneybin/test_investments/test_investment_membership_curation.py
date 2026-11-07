"""Complete selection remapping refuses ledger transitions that lose user curation."""

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo


def ledger_row(identity: str, *, type_: str, quantity: str, day: int) -> dict[str, Any]:
    return {
        "investment_transaction_id": identity,
        "account_id": "account",
        "security_id": "security",
        "trade_date": date(2026, 1, day),
        "original_acquisition_date": None,
        "type": type_,
        "quantity": Decimal(quantity),
        "price": Decimal("10"),
        "amount": -Decimal(quantity) * 10,
        "fees": Decimal(0),
        "currency_code": "USD",
    }


def test_complete_selection_migrates_disposal_and_acquisition_identity(
    db: Database,
) -> None:
    from moneybin.investments.event_curation import preflight_membership_transition

    current = [
        ledger_row("old_buy", type_="buy", quantity="2", day=1),
        ledger_row("old_sell", type_="sell", quantity="-1", day=2),
    ]
    proposed = [
        ledger_row("new_buy", type_="buy", quantity="2", day=1),
        ledger_row("new_sell", type_="sell", quantity="-1", day=2),
    ]
    old_lot = compute_lot_id("account", "security", date(2026, 1, 1), "old_buy")
    new_lot = compute_lot_id("account", "security", date(2026, 1, 1), "new_buy")
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="old_sell",
        selections=[(old_lot, Decimal(1))],
        actor="test",
    )
    remap = preflight_membership_transition(
        db, current, proposed, {"old_buy": "new_buy", "old_sell": "new_sell"}
    )
    assert remap.replacements == {"old_sell": [], "new_sell": [(new_lot, Decimal(1))]}
    assert LotSelectionsRepo(db).list_for_disposal("old_sell") == [
        (old_lot, Decimal(1))
    ]


def test_partial_or_unavailable_selection_blocks_before_writing(db: Database) -> None:
    from moneybin.investments.event_curation import preflight_membership_transition

    current = [
        ledger_row("buy", type_="buy", quantity="2", day=1),
        ledger_row("sell", type_="sell", quantity="-2", day=2),
    ]
    lot = compute_lot_id("account", "security", date(2026, 1, 1), "buy")
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="sell", selections=[(lot, Decimal(2))], actor="test"
    )
    proposed = [ledger_row("buy", type_="buy", quantity="1", day=1), current[1]]
    with pytest.raises(UserError, match="selection"):
        preflight_membership_transition(db, current, proposed, {})
    assert LotSelectionsRepo(db).list_for_disposal("sell") == [(lot, Decimal(2))]


@pytest.mark.parametrize("same", [True, False])
def test_converging_disposals_require_equal_complete_collections(
    db: Database, same: bool
) -> None:
    from moneybin.investments.event_curation import preflight_membership_transition

    current = [
        ledger_row("buy", type_="buy", quantity="4", day=1),
        ledger_row("sell_a", type_="sell", quantity="-1", day=2),
        ledger_row("sell_b", type_="sell", quantity="-1", day=2),
    ]
    lot = compute_lot_id("account", "security", date(2026, 1, 1), "buy")
    repo = LotSelectionsRepo(db)
    repo.set_for_disposal(
        investment_transaction_id="sell_a", selections=[(lot, Decimal(1))], actor="test"
    )
    repo.set_for_disposal(
        investment_transaction_id="sell_b",
        selections=[(lot, Decimal(1) if same else Decimal("0.5"))],
        actor="test",
    )
    proposed = [current[0], ledger_row("combined", type_="sell", quantity="-1", day=2)]
    if same:
        remap = preflight_membership_transition(
            db, current, proposed, {"sell_a": "combined", "sell_b": "combined"}
        )
        assert remap.replacements == {
            "sell_a": [],
            "sell_b": [],
            "combined": [(lot, Decimal(1))],
        }
    else:
        with pytest.raises(UserError, match="selection"):
            preflight_membership_transition(
                db, current, proposed, {"sell_a": "combined", "sell_b": "combined"}
            )


def test_moving_acquisition_later_checks_disposal_between_old_and_new_keys(
    db: Database,
) -> None:
    from moneybin.investments.event_curation import preflight_membership_transition

    buy = ledger_row("buy", type_="buy", quantity="2", day=1)
    sell = ledger_row("sell", type_="sell", quantity="-2", day=2)
    lot = compute_lot_id("account", "security", date(2026, 1, 1), "buy")
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="sell", selections=[(lot, Decimal(2))], actor="test"
    )
    with pytest.raises(UserError, match="selection"):
        preflight_membership_transition(
            db, [buy, sell], [{**buy, "trade_date": date(2026, 1, 3)}, sell], {}
        )


def test_unchanged_pre_boundary_election_does_not_need_remap(db: Database) -> None:
    from moneybin.investments.event_curation import preflight_membership_transition

    buy = ledger_row("buy", type_="buy", quantity="2", day=1)
    sell = ledger_row("sell", type_="sell", quantity="-1", day=2)
    later = ledger_row("later", type_="buy", quantity="2", day=3)
    lot = compute_lot_id("account", "security", date(2026, 1, 1), "buy")
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="sell", selections=[(lot, Decimal(1))], actor="test"
    )
    assert (
        preflight_membership_transition(
            db, [buy, sell, later], [buy, sell, {**later, "quantity": Decimal(3)}], {}
        ).replacements
        == {}
    )
