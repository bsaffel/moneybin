"""Comparison structure and blocking regressions from the independent review."""

import pytest

from moneybin.database import Database
from moneybin.repositories.securities_repo import SecuritiesRepo
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)

_FUNDING = {
    "dividend": ("dividend", "dividend", "dividend reinvestment"),
    "interest": ("interest", "interest", "interest reinvestment"),
    "capital_gain": (
        "capital_gain_distribution",
        "long-term capital gain",
        "long-term capital gain reinvestment",
    ),
}


def test_blocked_income_leg_cannot_leave_a_partial_reinvest_candidate(
    comparison_db: Database,
) -> None:
    seed_manual_event(
        comparison_db, "mbuy", type_="reinvest", subtype="dividend", group="g"
    )
    seed_manual_event(
        comparison_db,
        "mincome",
        type_="dividend",
        group="g",
        quantity=None,
        price=None,
        amount="100",
    )
    seed_plaid_event(comparison_db, "pbuy", subtype="dividend reinvestment", day=15)
    seed_plaid_event(
        comparison_db,
        "pincome",
        type_="cash",
        subtype="dividend",
        quantity=None,
        amount="-100",
        day=18,
    )
    install_comparison_models(comparison_db)
    assert comparison_db.execute("""
        SELECT member_count, is_match_eligible
        FROM prep.int_investment_events__headers ORDER BY source_type
    """).fetchall() == [(2, True), (2, True)]
    assert comparison_db.execute("""
        SELECT COUNT(*) FROM prep.int_investment_events__evidence WHERE is_candidate
    """).fetchone() == (0,)


@pytest.mark.parametrize("manual_funding", _FUNDING)
@pytest.mark.parametrize("plaid_funding", _FUNDING)
def test_reinvest_candidates_require_complete_same_funding_shape(
    comparison_db: Database, manual_funding: str, plaid_funding: str
) -> None:
    seed_manual_event(
        comparison_db, "mbuy", type_="reinvest", subtype=manual_funding, group="g"
    )
    seed_manual_event(
        comparison_db,
        "mincome",
        type_=_FUNDING[manual_funding][0],
        group="g",
        quantity=None,
        price=None,
        amount="100",
    )
    seed_plaid_event(comparison_db, "pbuy", subtype=_FUNDING[plaid_funding][2])
    seed_plaid_event(
        comparison_db,
        "pincome",
        type_="cash",
        subtype=_FUNDING[plaid_funding][1],
        quantity=None,
        amount="-100",
    )
    install_comparison_models(comparison_db)
    assert comparison_db.execute("""
        SELECT member_count, is_match_eligible
        FROM prep.int_investment_events__headers ORDER BY source_type
    """).fetchall() == [(2, True), (2, True)]
    expected = 2 if manual_funding == plaid_funding else 0
    assert comparison_db.execute("""
        SELECT COUNT(*) FROM prep.int_investment_events__evidence WHERE is_candidate
    """).fetchone() == (expected,)


@pytest.mark.parametrize("route", ["absent", "reversed", "conflicting"])
def test_plaid_dimension_fallback_does_not_ratify_account_identity(
    comparison_db: Database, route: str
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "plaid")
    comparison_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('native_account', 'USD')"
    )
    comparison_db.execute("""
        UPDATE raw.manual_investment_transactions SET account_id = 'native_account'
    """)
    if route == "absent":
        comparison_db.execute("DELETE FROM app.account_links")
    elif route == "reversed":
        comparison_db.execute("UPDATE app.account_links SET status = 'reversed'")
    else:
        comparison_db.execute("""
            INSERT INTO app.account_links
            (link_id, account_id, ref_kind, ref_value, source_type, source_origin,
             status, decided_by, decided_at)
            VALUES ('conflicting', 'native_account', 'source_native',
                    'native_account', 'plaid', 'origin', 'accepted', 'user',
                    CURRENT_TIMESTAMP)
        """)
    install_comparison_models(comparison_db)
    assert comparison_db.execute("""
        SELECT native_reference, has_resolved_identity
        FROM prep.int_investment_events__observations WHERE source_type = 'plaid'
    """).fetchall() == [("plaid", False)]
    assert comparison_db.execute("""
        SELECT COUNT(*) FROM prep.int_investment_events__evidence WHERE is_candidate
    """).fetchone() == (0,)


@pytest.mark.parametrize("blocker", ["account", "security", "currency", "date"])
def test_evidence_blocks_unrelated_neighborhoods_before_pair_expansion(
    comparison_db: Database, blocker: str
) -> None:
    count = 8
    for index in range(count):
        seed_manual_event(comparison_db, f"manual_{index}", day=10)
        seed_plaid_event(comparison_db, f"plaid_{index}", day=10)
    if blocker == "account":
        comparison_db.execute("INSERT INTO core.dim_accounts VALUES ('other', 'USD')")
        comparison_db.execute(
            "UPDATE raw.manual_investment_transactions SET account_id = 'other'"
        )
    elif blocker == "security":
        SecuritiesRepo(comparison_db).upsert(
            security_id="other",
            name="Other Security",
            security_type="equity",
            actor="test",
        )
        comparison_db.execute(
            "UPDATE raw.manual_investment_transactions SET security_id = 'other'"
        )
    elif blocker == "currency":
        comparison_db.execute(
            "UPDATE raw.manual_investment_transactions SET currency_code = 'EUR'"
        )
    else:
        comparison_db.execute(
            "UPDATE raw.manual_investment_transactions SET trade_date = DATE '2025-01-10'"
        )
    seed_manual_event(comparison_db, "control", day=10)
    install_comparison_models(comparison_db)
    assert comparison_db.execute("""
        SELECT COUNT(*) FROM prep.int_investment_events__headers WHERE is_match_eligible
    """).fetchone() == (2 * count + 1,)
    assert comparison_db.execute("""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE is_candidate)
        FROM prep.int_investment_events__evidence
    """).fetchone() == (count, count)


def test_a_cash_only_plaid_event_carries_a_generation_it_can_be_validated_with(
    comparison_db: Database,
) -> None:
    """A dividend with no security key stays match-eligible, so it must carry both.

    `routing_generations` is LEFT JOINed on the source key, so an event that has
    none misses it. `has_resolved_identity` still admits the event — no security
    means the security clause is vacuous and `dividend` is not one of the types
    that demands one — and `InvestmentMatchDetails` declares both generations as
    required `str`, so a NULL here fails validation for the whole collection
    rather than the one row.
    """
    seed_plaid_event(
        comparison_db,
        "pdividend",
        type_="cash",
        subtype="dividend",
        quantity=None,
        security_id=None,
    )
    install_comparison_models(comparison_db)
    assert comparison_db.execute("""
        SELECT
            account_identity_generation IS NOT NULL,
            security_identity_generation IS NOT NULL,
            has_resolved_identity
        FROM prep.int_investment_events__observations
        WHERE native_reference = 'pdividend'
    """).fetchone() == (True, True, True)
