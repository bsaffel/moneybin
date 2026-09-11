"""Candidate boundaries never infer missing identities or transfer counterparts."""

import pytest

from moneybin.database import Database
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


@pytest.mark.parametrize(
    ("manual_type", "plaid_quantity", "expected"),
    [
        ("transfer_in", "1", [(True,)]),
        ("transfer_out", "-1", [(True,)]),
        ("transfer_in", "-1", []),
        ("transfer_out", "1", []),
    ],
)
def test_transfers_compare_only_same_direction_across_sources(
    comparison_db: Database,
    manual_type: str,
    plaid_quantity: str,
    expected: list[tuple[bool]],
) -> None:
    seed_manual_event(
        comparison_db,
        "manual",
        type_=manual_type,
        quantity="1" if manual_type == "transfer_in" else "-1",
        amount="0",
        price=None,
    )
    seed_plaid_event(
        comparison_db,
        "plaid",
        type_="transfer",
        subtype="transfer",
        quantity=plaid_quantity,
        amount="0",
    )
    install_comparison_models(comparison_db)
    assert comparison_db.execute("""
        SELECT member_count FROM prep.int_investment_events__headers
    """).fetchall() == [(1,), (1,)]
    assert (
        comparison_db.execute("""
        SELECT is_candidate FROM prep.int_investment_events__evidence
    """).fetchall()
        == expected
    )


def test_same_source_origin_rows_do_not_compare_with_each_other(
    comparison_db: Database,
) -> None:
    seed_manual_event(comparison_db, "first")
    seed_manual_event(comparison_db, "second")
    install_comparison_models(comparison_db)
    assert comparison_db.execute("""
        SELECT COUNT(*) FROM prep.int_investment_events__evidence
    """).fetchone() == (0,)


@pytest.mark.parametrize("missing", ["account", "security", "currency"])
def test_unresolved_identity_or_effective_currency_remains_ineligible(
    comparison_db: Database, missing: str
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "plaid")
    if missing == "account":
        comparison_db.execute("DELETE FROM core.dim_accounts")
    elif missing == "security":
        comparison_db.execute("DELETE FROM app.security_links")
    else:
        comparison_db.execute("UPDATE core.dim_accounts SET currency_code = NULL")
    install_comparison_models(comparison_db)
    assert (
        comparison_db.execute("""
        SELECT is_candidate FROM prep.int_investment_events__evidence
    """).fetchall()
        == []
    )
    assert comparison_db.execute("""
        SELECT COUNT(*) FROM prep.int_investment_events__legs
    """).fetchone() == (2,)
