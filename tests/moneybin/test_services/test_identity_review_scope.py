"""Unrelated dangling selections do not prevent scoped identity operations."""

from datetime import date
from decimal import Decimal
from importlib import import_module

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.security_links_service import SecurityLinksService
from tests.moneybin.db_helpers import create_core_dim_stub_views
from tests.moneybin.migration_helpers import run_migration
from tests.moneybin.test_migration_v061 import seed_legacy_manual_identity_schema
from tests.moneybin.test_services.test_account_identity_selections import (
    seed_account_selections,
)
from tests.moneybin.test_services.test_security_links_service import (
    add_disposal,
    add_lot,
)
from tests.moneybin.test_services.test_security_links_service import (
    merge_setup as merge_setup,
)


def add_dangling_selection(db: Database, identity: str | None) -> None:
    """A known disposal proves scope even when its selected lot is absent."""
    if identity is not None:
        db.execute(
            """INSERT INTO core.fct_investment_transactions
            (investment_transaction_id, account_id, security_id, trade_date,
             type, quantity, currency_code)
            VALUES ('dangling_sell', ?, ?, DATE '2026-02-01', 'sell', -1, 'USD')""",
            [identity, identity],
        )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="dangling_sell",
        selections=[("missing_lot", Decimal("1"))],
        actor="cli",
    )


@pytest.mark.parametrize("identity", ["unrelated", "a", "b", None])
def test_account_preview_scopes_missing_lot_to_its_disposal(
    db: Database, identity: str | None
) -> None:
    seed_account_selections(db)
    add_dangling_selection(db, identity)
    before = db.execute("SELECT * FROM app.lot_selections ORDER BY 1, 2").fetchall()
    audit = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    if identity == "unrelated":
        impact = AccountLinksService(db).accept_impact("a_b_", target_account_id="b")
        assert impact.lot_selection_disposal_ids == ("sell",)
        assert impact.blast_radius["lot_selections"] == 1
    else:
        with pytest.raises(UserError, match="lot selection"):
            AccountLinksService(db).accept_impact("a_b_", target_account_id="b")
    assert (
        db.execute("SELECT * FROM app.lot_selections ORDER BY 1, 2").fetchall()
        == before
    )
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit


@pytest.mark.parametrize("identity", ["unrelated", "provisional", "survivor", None])
def test_security_merge_scopes_missing_lot_to_its_disposal(
    db: Database, merge_setup: dict[str, str], identity: str | None
) -> None:
    provisional, survivor = merge_setup["provisional"], merge_setup["survivor"]
    lot = add_lot(db, security_id=provisional)
    add_disposal(db, "affected_sell", provisional)
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="affected_sell",
        selections=[(lot, Decimal("2"))],
        actor="cli",
    )
    add_dangling_selection(
        db, merge_setup.get(identity, identity) if identity else None
    )
    selections = db.execute("SELECT * FROM app.lot_selections ORDER BY 1, 2").fetchall()
    links = db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
    audit = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    service = SecurityLinksService(db)
    if identity == "unrelated":
        impact = service.accept_impact(merge_setup["decision_id"], into=survivor)
        assert impact.lot_selection_disposal_ids == ("affected_sell",)
        service.accept_merge(merge_setup["decision_id"], into=survivor)
        assert LotSelectionsRepo(db).list_for_disposal("affected_sell") == [
            (
                compute_lot_id("acc_1", survivor, date(2024, 3, 1), "itx_buy"),
                Decimal("2"),
            )
        ]
        assert LotSelectionsRepo(db).list_for_disposal("dangling_sell") == [
            ("missing_lot", Decimal("1"))
        ]
    else:
        with pytest.raises(UserError, match="selection"):
            service.accept_merge(merge_setup["decision_id"], into=survivor)
        assert (
            db.execute("SELECT * FROM app.lot_selections ORDER BY 1, 2").fetchall()
            == selections
        )
        assert (
            db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
            == links
        )
        assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit


@pytest.mark.parametrize("identity", ["unrelated", "a", "b", None])
def test_upgrade_scopes_missing_lot_to_its_disposal(
    db: Database, identity: str | None
) -> None:
    seed_legacy_manual_identity_schema(db)
    create_core_dim_stub_views(db)
    db.execute("""INSERT INTO app.account_link_decisions
        (decision_id, provisional_account_id, candidate_account_id, status,
         decided_by, decided_at)
        VALUES ('historical_merge', 'a', 'b', 'accepted', 'user', CURRENT_TIMESTAMP)""")
    add_dangling_selection(db, identity)
    raw = db.execute(
        "SELECT * FROM raw.manual_investment_transactions ORDER BY 1"
    ).fetchall()
    links = db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
    selections = db.execute("SELECT * FROM app.lot_selections ORDER BY 1, 2").fetchall()
    migration = import_module("moneybin.sql.migrations.V061__immutable_manual_identity")
    if identity == "unrelated":
        run_migration(db, migration.migrate)
        constraints = db.execute("""SELECT constraint_text FROM duckdb_constraints()
            WHERE schema_name = 'app' AND table_name = 'security_links'""").fetchall()
        assert any("manual_investment_transaction_id" in row[0] for row in constraints)
    else:
        with pytest.raises(RuntimeError, match="lot selection"):
            run_migration(db, migration.migrate)
    assert (
        db.execute(
            "SELECT * FROM raw.manual_investment_transactions ORDER BY 1"
        ).fetchall()
        == raw
    )
    assert (
        db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
        == links
    )
    assert (
        db.execute("SELECT * FROM app.lot_selections ORDER BY 1, 2").fetchall()
        == selections
    )


@pytest.mark.parametrize("identity", ["unrelated", "a", "b"])
def test_account_preview_checks_live_selection_scope(
    db: Database, identity: str
) -> None:
    seed_account_selections(db)
    lot = add_lot(
        db,
        security_id="other_security",
        account_id="other_account",
        source_transaction_id="other_buy",
    )
    add_disposal(db, "other_sell", "other_security")
    db.execute(
        "UPDATE core.fct_investment_transactions SET account_id = 'other_account' WHERE investment_transaction_id = 'other_sell'"
    )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="other_sell",
        selections=[(lot, Decimal("1"))],
        actor="cli",
    )
    db.execute(
        """INSERT INTO raw.manual_investment_transactions
        (source_transaction_id, import_id, account_id, security_id, type, trade_date,
         quantity, amount, created_by, investment_transaction_id)
        VALUES ('other_buy', 'test_import', ?, 'other_security', 'buy', DATE '2024-03-01',
                100, -10000, 'cli', 'other_buy')""",
        [identity],
    )
    if identity == "unrelated":
        impact = AccountLinksService(db).accept_impact("a_b_", target_account_id="b")
        assert impact.lot_selection_disposal_ids == ("sell",)
    else:
        with pytest.raises(UserError, match="lot selection"):
            AccountLinksService(db).accept_impact("a_b_", target_account_id="b")


@pytest.mark.parametrize("identity", ["unrelated", "provisional", "survivor"])
def test_security_preview_checks_live_selection_scope(
    db: Database, merge_setup: dict[str, str], identity: str
) -> None:
    lot = add_lot(db, security_id="other_security", source_transaction_id="other_buy")
    add_disposal(db, "other_sell", "other_security")
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="other_sell",
        selections=[(lot, Decimal("1"))],
        actor="cli",
    )
    db.execute(
        """INSERT INTO raw.manual_investment_transactions
        (source_transaction_id, import_id, account_id, security_id, type, trade_date,
         quantity, amount, created_by, investment_transaction_id)
        VALUES ('other_buy', 'test_import', 'acc_1', ?, 'buy', DATE '2024-03-01',
                100, -10000, 'cli', 'other_buy')""",
        [merge_setup.get(identity, identity)],
    )
    if identity == "unrelated":
        impact = SecurityLinksService(db).accept_impact(
            merge_setup["decision_id"], into=merge_setup["survivor"]
        )
        assert impact.lot_selection_disposal_ids == ()
    else:
        with pytest.raises(UserError, match="selection"):
            SecurityLinksService(db).accept_impact(
                merge_setup["decision_id"], into=merge_setup["survivor"]
            )


@pytest.mark.parametrize("identity", ["unrelated", "a", "b"])
def test_upgrade_checks_frozen_disposal_scope(db: Database, identity: str) -> None:
    seed_legacy_manual_identity_schema(db)
    create_core_dim_stub_views(db)
    db.execute("""INSERT INTO app.account_link_decisions
        (decision_id, provisional_account_id, candidate_account_id, status,
         decided_by, decided_at)
        VALUES ('historical_merge', 'a', 'b', 'accepted', 'user', CURRENT_TIMESTAMP)""")
    add_dangling_selection(db, "unrelated")
    db.execute(
        """UPDATE raw.manual_investment_transactions
        SET investment_transaction_id = 'dangling_sell', account_id = ?
        WHERE source_transaction_id = 'two'""",
        [identity],
    )
    migration = import_module("moneybin.sql.migrations.V061__immutable_manual_identity")
    if identity == "unrelated":
        run_migration(db, migration.migrate)
    else:
        with pytest.raises(RuntimeError, match="lot selection"):
            run_migration(db, migration.migrate)
