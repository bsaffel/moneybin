"""Legacy group hints never authorize partially matched compound events."""

import pytest

from moneybin.database import Database
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
)
from tests.moneybin.test_investments.test_investment_golden_projection import (
    install_golden_models,
)


@pytest.mark.parametrize(
    "shape",
    [
        "merger",
        "spin_off",
        "per_lot",
        "different_account",
        "different_currency",
        "reused_hint",
        "extra_reinvest_member",
    ],
)
def test_unverified_legacy_group_preserves_every_row_but_matches_none(
    comparison_db: Database, shape: str
) -> None:
    SecuritiesRepo(comparison_db).upsert(
        security_id="second_security",
        name="Second Synthetic Security",
        security_type="equity",
        actor="test",
    )
    comparison_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('second_account', 'USD')"
    )
    if shape in {"merger", "spin_off", "per_lot"}:
        seed_manual_event(
            comparison_db,
            "out",
            type_="return_of_capital" if shape == "spin_off" else "transfer_out",
            group="shared",
            quantity="1",
            amount="100",
        )
        seed_manual_event(comparison_db, "in", type_="transfer_in", group="shared")
        comparison_db.execute("""
            UPDATE raw.manual_investment_transactions
            SET security_id = 'second_security' WHERE source_transaction_id = 'in'
        """)
        if shape == "per_lot":
            seed_manual_event(
                comparison_db, "out_lot2", type_="transfer_out", group="shared"
            )
    elif shape == "reused_hint":
        seed_manual_event(comparison_db, "buy1", group="shared")
        seed_manual_event(comparison_db, "buy2", group="shared", day=20)
    else:
        seed_manual_event(
            comparison_db,
            "acquisition",
            type_="reinvest",
            subtype="dividend",
            group="shared",
        )
        seed_manual_event(
            comparison_db,
            "income",
            type_="dividend",
            group="shared",
            quantity=None,
            price=None,
            amount="100",
        )
        if shape == "different_account":
            comparison_db.execute("""
                UPDATE raw.manual_investment_transactions SET account_id = 'second_account'
                WHERE source_transaction_id = 'income'
            """)
        elif shape == "different_currency":
            comparison_db.execute("""
                UPDATE raw.manual_investment_transactions SET currency_code = 'CAD'
                WHERE source_transaction_id = 'income'
            """)
        else:
            seed_manual_event(comparison_db, "extra", group="shared")
    before = comparison_db.execute("""
        SELECT * FROM raw.manual_investment_transactions ORDER BY source_transaction_id
    """).fetchall()
    install_comparison_models(comparison_db)
    legs = comparison_db.execute("""
        SELECT source_group_reference, is_match_eligible
        FROM prep.int_investment_events__legs
    """).fetchall()
    assert legs == [("shared", False)] * len(before)
    assert comparison_db.execute("""
        SELECT member_count FROM prep.int_investment_events__headers
    """).fetchall() == [(1,)] * len(before)
    assert (
        comparison_db.execute("""
        SELECT * FROM raw.manual_investment_transactions ORDER BY source_transaction_id
    """).fetchall()
        == before
    )

    expected = comparison_db.execute("""SELECT source_transaction_id, account_id,
        security_id, type, quantity, amount, trade_date
        FROM raw.manual_investment_transactions ORDER BY source_transaction_id""").fetchall()
    membership = InvestmentMembershipService(comparison_db)
    membership.reconcile(actor="test")
    install_golden_models(comparison_db)
    assert (
        comparison_db.execute("""SELECT m.native_reference, g.account_id,
        g.security_id, g.type, g.quantity, g.amount, g.trade_date
        FROM core.fct_investment_transactions AS g
        JOIN app.investment_event_members AS m
          ON g.investment_transaction_id = m.investment_transaction_id
        WHERE m.retired_at IS NULL ORDER BY m.native_reference""").fetchall()
        == expected
    )
    active = InvestmentEventMembersRepo(comparison_db).active_members()
    assert len(active) == len(before)
    assert len({r["event_group_id"] for r in active}) == len(before)
    assert len({r["investment_transaction_id"] for r in active}) == len(before)
    assert comparison_db.execute("""SELECT DISTINCT l.source_group_reference
        FROM app.investment_event_members AS m
        JOIN prep.int_investment_events__legs AS l
          USING (source_type, source_origin, native_reference, observation_version)
        WHERE m.retired_at IS NULL""").fetchall() == [("shared",)]
    assert all(r["decision_id"] is None for r in active)
    assert not {r["event_group_id"] for r in active} & {"shared"}
    matching = InvestmentMatchingService(comparison_db)
    matching.run()
    assert matching.pending() == []
    membership.reconcile(actor="test")
    assert InvestmentEventMembersRepo(comparison_db).active_members() == active
    assert (
        comparison_db.execute("""SELECT * FROM raw.manual_investment_transactions
        ORDER BY source_transaction_id""").fetchall()
        == before
    )
