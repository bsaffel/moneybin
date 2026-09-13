"""Direct account merge approvals bind the manual observations they reroute."""

from contextlib import nullcontext
from datetime import UTC, datetime
from typing import Literal

import pytest
from pytest_mock import MockerFixture

from moneybin import error_codes
from moneybin.cli.commands.accounts import links as cli_links
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.identity import manual_identity_sql
from moneybin.mcp.confirmation import ConfirmationBroker
from moneybin.mcp.tools import accounts as mcp_accounts
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.account_links_service import AccountLinksService
from tests.moneybin.db_helpers import create_core_dim_stub_views, create_core_tables
from tests.moneybin.test_services.test_manual_identity_projection import (
    add_account_edge,
    add_account_terminal,
    add_manual_observation,
)


@pytest.mark.parametrize("surface", ["cli", "mcp"])
@pytest.mark.parametrize(
    "change",
    [
        "record",
        "revert",
        "replace",
        "identity",
        "survivor_record",
        "unrelated_record",
        "unchanged",
    ],
)
def test_account_merge_confirmation_binds_affected_manual_identity(
    db: Database,
    mocker: MockerFixture,
    surface: Literal["cli", "mcp"],
    change: str,
) -> None:
    create_core_tables(db)
    create_core_dim_stub_views(db)
    db.execute("""
        INSERT INTO core.dim_accounts (account_id, display_name, currency_code)
        VALUES ('a', 'First', 'USD'), ('b', 'Second', 'USD')
    """)
    add_account_terminal(db, "a")
    add_account_terminal(db, "b")
    add_account_edge(db, "a", "b", status="pending")
    # Frozen ancestor observations are affected through their effective account.
    add_account_edge(db, "ancestor", "a")
    add_manual_observation(db, "ancestor", "manual")
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
    if change == "record":
        add_manual_observation(db, "a", "new_manual")
    elif change in ("survivor_record", "unrelated_record"):
        add_manual_observation(
            db, "b" if change == "survivor_record" else "unrelated", "new_manual"
        )
    elif change in ("revert", "replace"):
        db.execute(
            "DELETE FROM raw.manual_investment_transactions WHERE source_transaction_id = 'manual'"
        )
        if change == "replace":
            add_manual_observation(db, "ancestor", "replacement")
    elif change == "identity":
        SecuritiesRepo(db).upsert(
            security_id="other_security",
            name="Synthetic",
            security_type="equity",
            actor="cli",
        )
        SecurityLinksRepo(db).insert(
            security_id="other_security",
            ref_kind="manual_investment_transaction_id",
            ref_value="manual",
            source_type="manual",
            decided_by="user",
            actor="cli",
        )
    assert (
        service.accept_impact("a_b_", target_account_id="b").blast_radius
        == impact.blast_radius
    )
    before = {
        table: db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()  # noqa: S608  # fixed test table names
        for table in (
            "raw.manual_investment_transactions",
            "app.account_links",
            "app.account_link_decisions",
            "app.security_links",
            "app.audit_log",
        )
    }

    def apply() -> None:
        if surface == "cli":
            service.set(
                "a_b_",
                target_account_id="b",
                verify_accept=cli_links._drift_check(db, "a_b_", preview[0]),  # pyright: ignore[reportPrivateUsage]  # test exercises confirmation seam
            )
        else:
            mcp_accounts._apply_account_accept("a_b_", "b", grant)  # pyright: ignore[reportPrivateUsage]  # test exercises confirmation seam

    if change in ("unchanged", "unrelated_record"):
        apply()
        assert db.execute(
            f"SELECT source_transaction_id, account_id FROM ({manual_identity_sql()}) WHERE source_transaction_id = 'manual'"  # noqa: S608  # canonical repository identity query
        ).fetchall() == [("manual", "b")]
        assert db.execute(
            "SELECT account_id FROM raw.manual_investment_transactions WHERE source_transaction_id = 'manual'"
        ).fetchall() == [("ancestor",)]
    else:
        with pytest.raises(UserError) as exc:
            apply()
        assert exc.value.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
        for table, rows in before.items():
            assert db.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall() == rows  # noqa: S608  # fixed test table names
