"""Source-neutral comparison preserves complete event shapes and exact evidence."""

import pytest

from moneybin.database import Database
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


def test_singletons_have_versioned_source_neutral_legs(comparison_db: Database) -> None:
    seed_manual_event(comparison_db, "manual_one")
    seed_plaid_event(comparison_db, "plaid_one")
    install_comparison_models(comparison_db)
    legs = comparison_db.execute("""SELECT source_type, native_reference, observation_version,
        trade_date_basis, currency_code, original_acquisition_date
        FROM prep.int_investment_events__legs ORDER BY source_type""").fetchall()
    assert len(legs) == 2
    assert legs[0][0:2] == ("manual", "manual_one")
    assert str(legs[0][2]).startswith("manual_")
    assert legs[0][3:] == ("explicit", "USD", None)
    assert str(legs[1][2]).startswith("plaid_")
    assert legs[1][3:] == ("posting_fallback", "USD", None)
    assert comparison_db.execute("""SELECT member_count, is_match_eligible
        FROM prep.int_investment_events__headers""").fetchall() == [
        (1, True),
        (1, True),
    ]


@pytest.mark.parametrize(
    ("funding", "income", "plaid_subtype"),
    [
        ("dividend", "dividend", "dividend reinvestment"),
        ("interest", "interest", "interest reinvestment"),
        (
            "capital_gain",
            "capital_gain_distribution",
            "long-term capital gain reinvestment",
        ),
    ],
)
def test_reinvest_pairing_is_complete_and_source_neutral(
    comparison_db: Database, funding: str, income: str, plaid_subtype: str
) -> None:
    seed_manual_event(
        comparison_db, "manual_buy", type_="reinvest", subtype=funding, group="legacy"
    )
    seed_manual_event(
        comparison_db,
        "manual_income",
        type_=income,
        group="legacy",
        quantity=None,
        price=None,
        amount="100",
    )
    seed_plaid_event(comparison_db, "plaid_buy", subtype=plaid_subtype)
    income_subtype = (
        "long-term capital gain" if income == "capital_gain_distribution" else income
    )
    seed_plaid_event(
        comparison_db,
        "plaid_income",
        type_="cash",
        subtype=income_subtype,
        quantity=None,
        amount="-100",
        day=13,
    )
    install_comparison_models(comparison_db)
    headers = comparison_db.execute("""SELECT source_type, event_type, member_count,
        is_match_eligible FROM prep.int_investment_events__headers ORDER BY source_type""").fetchall()
    assert headers == [("manual", "reinvest", 2, True), ("plaid", "reinvest", 2, True)]


@pytest.mark.parametrize(
    "case", ["missing", "mistyped", "four_days", "cash", "ambiguous"]
)
def test_plaid_incomplete_or_ambiguous_reinvest_stays_singleton(
    comparison_db: Database, case: str
) -> None:
    seed_plaid_event(comparison_db, "acquisition", subtype="dividend reinvestment")
    if case != "missing":
        seed_plaid_event(
            comparison_db,
            "income",
            type_="cash",
            subtype="interest" if case == "mistyped" else "dividend",
            quantity=None,
            amount="-90" if case == "cash" else "-100",
            day=14 if case == "four_days" else 10,
        )
    if case == "ambiguous":
        seed_plaid_event(
            comparison_db,
            "income2",
            type_="cash",
            subtype="dividend",
            quantity=None,
            amount="-100",
        )
    install_comparison_models(comparison_db)
    assert comparison_db.execute(
        "SELECT MAX(member_count) FROM prep.int_investment_events__headers"
    ).fetchone() == (1,)
    assert comparison_db.execute(
        "SELECT is_match_eligible FROM prep.int_investment_events__legs WHERE native_reference = 'acquisition'"
    ).fetchone() == (False,)


@pytest.mark.parametrize("subtype", ["merger", "spin off", "trade", "split"])
def test_plaid_unsupported_shapes_cannot_match_partially(
    comparison_db: Database, subtype: str
) -> None:
    seed_plaid_event(comparison_db, "unsupported", type_="transfer", subtype=subtype)
    install_comparison_models(comparison_db)
    assert comparison_db.execute(
        "SELECT is_match_eligible, supports_split FROM prep.int_investment_events__legs"
    ).fetchall() == [(False, False)]


@pytest.mark.parametrize(
    ("left", "right", "within"),
    [
        ("1", "1.000001", True),
        ("1", "1.0000010001", False),
        ("0", "0", True),
        ("0", "0.000001", True),
        ("0", "0.0000010001", False),
        ("1000", "1000.00001", True),
        ("1000", "1000.000010001", False),
        ("1.000001", "1", True),
        ("0.0000010001", "0", False),
    ],
)
def test_quantity_evidence_has_symmetric_zero_safe_boundaries(
    comparison_db: Database, left: str, right: str, within: bool
) -> None:
    seed_manual_event(comparison_db, "manual", quantity=left)
    seed_plaid_event(comparison_db, "plaid", quantity=right)
    install_comparison_models(comparison_db)
    assert comparison_db.execute(
        "SELECT quantity_within_tolerance FROM prep.int_investment_events__evidence"
    ).fetchall() == [(within,)]


@pytest.mark.parametrize(("amount", "within"), [("100.01", True), ("100.02", False)])
def test_cash_evidence_does_not_use_description_similarity(
    comparison_db: Database, amount: str, within: bool
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "plaid", amount=amount)
    install_comparison_models(comparison_db)
    assert comparison_db.execute(
        "SELECT amount_within_tolerance, is_candidate FROM prep.int_investment_events__evidence"
    ).fetchall() == [(within, within)]


@pytest.mark.parametrize(("day", "within"), [(15, True), (16, False)])
def test_trade_candidate_date_window_is_five_days(
    comparison_db: Database, day: int, within: bool
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "plaid", day=day)
    install_comparison_models(comparison_db)
    assert comparison_db.execute(
        "SELECT date_within_tolerance, is_candidate FROM prep.int_investment_events__evidence"
    ).fetchall() == ([(True, True)] if within else [])
