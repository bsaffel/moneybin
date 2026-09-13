"""Live manual disposal identity can prove a missing Core row is unrelated."""

from decimal import Decimal

import pytest
from pytest_mock import MockerFixture

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.security_links_service import SecurityLinksService
from tests.moneybin.test_services.test_account_identity_selections import (
    seed_account_selections,
)
from tests.moneybin.test_services.test_manual_identity_projection import (
    add_account_edge,
    add_account_terminal,
)
from tests.moneybin.test_services.test_security_links_service import add_lot
from tests.moneybin.test_services.test_security_links_service import (
    merge_setup as merge_setup,
)


@pytest.mark.parametrize(
    "scope",
    [
        "unrelated",
        "inherited_unrelated",
        "a",
        "b",
        "missing",
        "ambiguous",
        "affected_lot",
    ],
)
def test_account_merge_scopes_unmaterialized_manual_disposal(
    db: Database, mocker: MockerFixture, scope: str
) -> None:
    affected_lot = seed_account_selections(db)
    unrelated_lot = add_lot(
        db,
        security_id="other_security",
        account_id="unrelated",
        source_transaction_id="other_buy",
    )
    if scope != "missing":
        account = scope if scope in ("a", "b") else "unrelated"
        if scope in ("inherited_unrelated", "ambiguous"):
            account = "ancestor"
            add_account_terminal(db, "unrelated")
            add_account_edge(db, "ancestor", "unrelated")
            if scope == "ambiguous":
                add_account_edge(db, "ancestor", "a")
        db.execute(
            """
            INSERT INTO raw.manual_investment_transactions (
                source_transaction_id, investment_transaction_id, import_id,
                account_id, security_id, type, trade_date, quantity, amount, created_by
            ) VALUES ('manual_disposal', 'unmaterialized', 'fixture_import', ?,
                      'other_security', 'sell', '2026-01-01', -1, 10, 'cli')
            """,
            [account],
        )
    selections = LotSelectionsRepo(db)
    selected = affected_lot if scope == "affected_lot" else unrelated_lot
    selections.set_for_disposal(
        investment_transaction_id="unmaterialized",
        selections=[(selected, Decimal("1"))],
        actor="cli",
    )
    mocker.patch.object(AccountLinksService, "rematch_after_merge", return_value=None)
    before = {
        table: db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()  # noqa: S608  # fixed test table names
        for table in (
            "raw.manual_investment_transactions",
            "app.account_links",
            "app.account_link_decisions",
            "app.lot_selections",
            "app.audit_log",
        )
    }
    service = AccountLinksService(db)
    if scope in ("unrelated", "inherited_unrelated"):
        impact = service.accept_impact("a_b_", target_account_id="b")
        assert impact.lot_selection_disposal_ids == ("sell",)
        service.set("a_b_", target_account_id="b")
        assert selections.list_for_disposal("unmaterialized") == [
            (selected, Decimal("1"))
        ]
        assert (
            db.execute(
                "SELECT * FROM raw.manual_investment_transactions ORDER BY ALL"
            ).fetchall()
            == before["raw.manual_investment_transactions"]
        )
    else:
        with pytest.raises(UserError, match="lot selection"):
            service.set("a_b_", target_account_id="b")
        for table, rows in before.items():
            assert db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall() == rows  # noqa: S608  # fixed test table names


@pytest.mark.parametrize(
    "scope",
    [
        "unrelated",
        "effective_unrelated",
        "provisional",
        "survivor",
        "missing",
        "unresolved",
        "affected_lot",
    ],
)
def test_security_merge_scopes_unmaterialized_manual_disposal(
    db: Database, merge_setup: dict[str, str], scope: str
) -> None:
    affected_lot = add_lot(db, security_id=merge_setup["provisional"])
    unrelated_lot = add_lot(
        db, security_id="unrelated", source_transaction_id="other_buy"
    )
    if scope != "missing":
        security = merge_setup.get(scope, "unrelated")
        if scope == "effective_unrelated":
            security = merge_setup["provisional"]
        db.execute(
            """INSERT INTO raw.manual_investment_transactions (
                source_transaction_id, investment_transaction_id, import_id,
                account_id, security_id, type, trade_date, quantity, amount, created_by
            ) VALUES ('manual_disposal', 'unmaterialized', 'fixture_import', 'acc_1',
                      ?, 'sell', '2026-01-01', -1, 10, 'cli')""",
            [None if scope == "unresolved" else security],
        )
        if scope == "effective_unrelated":
            SecurityLinksRepo(db).insert(
                security_id="unrelated",
                ref_kind="manual_investment_transaction_id",
                ref_value="manual_disposal",
                source_type="manual",
                decided_by="user",
                actor="cli",
            )
    selections = LotSelectionsRepo(db)
    selected = affected_lot if scope == "affected_lot" else unrelated_lot
    selections.set_for_disposal(
        investment_transaction_id="unmaterialized",
        selections=[(selected, Decimal("1"))],
        actor="cli",
    )
    before = {
        table: db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()  # noqa: S608  # fixed test table names
        for table in (
            "raw.manual_investment_transactions",
            "app.security_links",
            "app.security_link_decisions",
            "app.securities",
            "app.lot_selections",
            "app.audit_log",
        )
    }
    service = SecurityLinksService(db)
    if scope in ("unrelated", "effective_unrelated"):
        impact = service.accept_impact(
            merge_setup["decision_id"], into=merge_setup["survivor"]
        )
        assert impact.lot_selection_disposal_ids == ()
        service.accept_merge(merge_setup["decision_id"], into=merge_setup["survivor"])
        assert selections.list_for_disposal("unmaterialized") == [
            (selected, Decimal("1"))
        ]
        assert (
            db.execute(
                "SELECT * FROM raw.manual_investment_transactions ORDER BY ALL"
            ).fetchall()
            == before["raw.manual_investment_transactions"]
        )
    else:
        with pytest.raises(UserError, match="lot selection"):
            service.accept_merge(
                merge_setup["decision_id"], into=merge_setup["survivor"]
            )
        for table, rows in before.items():
            assert db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall() == rows  # noqa: S608  # fixed test table names
