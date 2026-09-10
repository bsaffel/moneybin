"""Legacy group hints never authorize partially matched compound events."""

import pytest

from moneybin.database import Database
from moneybin.repositories.securities_repo import SecuritiesRepo
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
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
