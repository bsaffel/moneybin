"""Identity changes must account for every stored lot selection before writes."""

from datetime import date
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.security_links_service import SecurityLinksService
from tests.moneybin.test_services.test_security_links_service import (
    add_disposal,
    add_lot,
)
from tests.moneybin.test_services.test_security_links_service import (
    merge_setup as merge_setup,
)


def test_security_merge_remaps_survivor_disposal_selecting_absorbed_lot(
    db: Database,
    merge_setup: dict[str, str],  # noqa: F811  # injected shared pytest fixture
) -> None:
    old = add_lot(db, security_id=merge_setup["provisional"])
    add_disposal(db, "sell_survivor", merge_setup["survivor"])
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="sell_survivor",
        selections=[(old, Decimal("2"))],
        actor="cli",
    )
    SecurityLinksService(db).accept_merge(
        merge_setup["decision_id"], into=merge_setup["survivor"]
    )
    expected = compute_lot_id(
        "acc_1", merge_setup["survivor"], date(2024, 3, 1), "itx_buy"
    )
    assert LotSelectionsRepo(db).list_for_disposal("sell_survivor") == [
        (expected, Decimal("2"))
    ]


def test_security_merge_refuses_when_selected_disposal_is_missing(
    db: Database,
    merge_setup: dict[str, str],  # noqa: F811  # injected shared pytest fixture
) -> None:
    lot = add_lot(db, security_id=merge_setup["provisional"])
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="missing_disposal",
        selections=[(lot, Decimal("2"))],
        actor="cli",
    )
    before = db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
    with pytest.raises(UserError):
        SecurityLinksService(db).accept_merge(
            merge_setup["decision_id"], into=merge_setup["survivor"]
        )
    assert (
        db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
        == before
    )


def test_account_merge_refuses_unmaterialized_selections_before_identity_writes(
    db: Database,
) -> None:
    AccountLinksRepo(db).insert(
        link_id="link_a",
        account_id="a",
        ref_kind="source_native",
        ref_value="native_a",
        source_type="manual",
        source_origin="user",
        decided_by="user",
        actor="cli",
    )
    AccountLinkDecisionsRepo(db).insert(
        decision_id="merge",
        provisional_account_id="a",
        candidate_account_id="b",
        confidence_score=None,
        match_signals={},
        decided_by="user",
        actor="cli",
    )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="sell",
        selections=[("old_lot", Decimal("2"))],
        actor="cli",
    )
    before = db.execute("SELECT * FROM app.account_links ORDER BY link_id").fetchall()
    audit_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="lot selection"):
        AccountLinksService(db).set("merge", target_account_id="b")
    assert (
        db.execute("SELECT * FROM app.account_links ORDER BY link_id").fetchall()
        == before
    )
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit_count
