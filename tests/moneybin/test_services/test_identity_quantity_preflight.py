"""Rekeying cannot silently invalidate a selection after positions are pooled."""

import pytest
from pytest_mock import MockerFixture

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import SecurityLinkDecisionsRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.security_links_service import SecurityLinksService
from tests.moneybin.test_services.test_account_identity_selections import (
    seed_account_selections,
)


@pytest.mark.parametrize("identity", ["account", "security"])
def test_earlier_survivor_disposal_cannot_consume_later_selected_donor_lot(
    db: Database,
    mocker: MockerFixture,
    identity: str,
) -> None:
    seed_account_selections(db)
    survivor_account = "b" if identity == "account" else "a"
    survivor_security = "security" if identity == "account" else "survivor"
    db.execute(
        """INSERT INTO core.fct_investment_transactions
        (investment_transaction_id, account_id, security_id, type, trade_date, quantity, amount, currency_code)
        VALUES ('earlier_sale', ?, ?, 'sell', DATE '2026-01-15', -10, 100, 'USD'),
               ('later_buy', ?, ?, 'buy', DATE '2026-01-20', 10, -100, 'USD')""",
        [survivor_account, survivor_security, survivor_account, survivor_security],
    )
    if identity == "security":
        for sid, creator in (("security", "plaid"), ("survivor", "user")):
            SecuritiesRepo(db).upsert(
                security_id=sid,
                name=sid,
                security_type="equity",
                created_by=creator,
                actor="cli",
            )
        SecurityLinksRepo(db).insert(
            security_id="security",
            ref_kind="plaid_security_id",
            ref_value="provider_ref",
            source_type="plaid",
            decided_by="auto",
            actor="cli",
        )
        decision = SecurityLinkDecisionsRepo(db).insert(
            ref_kind="plaid_security_id",
            ref_value="provider_ref",
            source_type="plaid",
            candidate_security_id="survivor",
            actor="cli",
        )
        assert decision.target_id is not None
        decision_id = decision.target_id
    else:
        mocker.patch.object(
            AccountLinksService, "rematch_after_merge", return_value=None
        )
        decision_id = "a_b_"
    before = db.execute("SELECT * FROM app.lot_selections").fetchall()
    audits = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="selection"):
        if identity == "account":
            AccountLinksService(db).set(decision_id, target_account_id="b")
        else:
            SecurityLinksService(db).accept_merge(decision_id, into="survivor")
    assert db.execute("SELECT * FROM app.lot_selections").fetchall() == before
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audits
