"""Comparison eligibility and projection materiality remain separate contracts."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
)


def _native(
    db: Database,
    *,
    day: int = 10,
    explicit: bool = False,
    type_: str = "buy",
    subtype: str = "buy",
    currency: str | None = None,
) -> None:
    row = SyncInvestmentTransaction(
        investment_transaction_id="native",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, day),
        transaction_datetime=datetime(2026, 1, day, tzinfo=UTC) if explicit else None,
        name="Different description",
        quantity=Decimal("1"),
        amount=Decimal("100"),
        price=Decimal("100"),
        fees=Decimal("0"),
        iso_currency_code=currency,
        type=type_,
        subtype=subtype,
    )
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]
        [row],
        "sync_native",
        datetime(2026, 2, 1, tzinfo=UTC),
        datetime(2026, 2, 1, tzinfo=UTC),
    )


@pytest.mark.parametrize(("explicit", "conflict"), [(True, True), (False, False)])
def test_explicit_trade_date_difference_is_material_inside_candidate_window(
    comparison_db: Database, explicit: bool, conflict: bool
) -> None:
    seed_manual_event(comparison_db, "manual")
    _native(comparison_db, day=11, explicit=explicit)
    install_comparison_models(comparison_db)
    assert comparison_db.execute("""SELECT is_candidate, trade_date_conflict
        FROM prep.int_investment_events__evidence""").fetchall() == [(True, conflict)]


def test_currency_contradiction_never_becomes_field_choice(
    comparison_db: Database,
) -> None:
    seed_manual_event(comparison_db, "manual")
    _native(comparison_db, currency="CAD")
    install_comparison_models(comparison_db)
    assert (
        comparison_db.execute(
            "SELECT is_candidate FROM prep.int_investment_events__evidence"
        ).fetchall()
        == []
    )


@pytest.mark.parametrize(
    "case", ["missing_income", "wrong_income", "different_date", "extra_member"]
)
def test_shared_incomplete_manual_hint_cannot_match_individual_members(
    comparison_db: Database, case: str
) -> None:
    seed_manual_event(
        comparison_db, "acquisition", type_="reinvest", subtype="dividend", group="hint"
    )
    seed_manual_event(
        comparison_db,
        "income",
        type_="buy"
        if case == "missing_income"
        else "interest"
        if case == "wrong_income"
        else "dividend",
        group="hint",
        quantity=None,
        price=None,
        amount="100",
        day=11 if case == "different_date" else 10,
    )
    if case == "extra_member":
        seed_manual_event(comparison_db, "extra", group="hint")
    install_comparison_models(comparison_db)
    rows = comparison_db.execute(
        "SELECT member_count, is_match_eligible FROM prep.int_investment_events__headers"
    ).fetchall()
    assert len(rows) == (3 if case == "extra_member" else 2)
    assert all(row == (1, False) for row in rows)


def test_lone_unused_manual_hint_is_singleton_provenance(
    comparison_db: Database,
) -> None:
    seed_manual_event(comparison_db, "manual", group="unused")
    install_comparison_models(comparison_db)
    assert comparison_db.execute(
        "SELECT source_group_reference, is_match_eligible FROM prep.int_investment_events__legs"
    ).fetchall() == [("unused", True)]


def test_manual_reinvest_uses_existing_default_dividend_funding(
    comparison_db: Database,
) -> None:
    seed_manual_event(
        comparison_db, "acquisition", type_="reinvest", subtype=None, group="minted"
    )
    seed_manual_event(
        comparison_db,
        "income",
        type_="dividend",
        group="minted",
        quantity=None,
        price=None,
        amount="100",
    )
    install_comparison_models(comparison_db)
    assert comparison_db.execute(
        "SELECT member_count, is_match_eligible FROM prep.int_investment_events__headers"
    ).fetchall() == [(2, True)]


def test_capabilities_explicitly_disable_native_correction_and_reversal(
    comparison_db: Database,
) -> None:
    seed_manual_event(comparison_db, "manual")
    _native(comparison_db)
    install_comparison_models(comparison_db)
    assert (
        comparison_db.execute("""SELECT supports_native_relationships,
        supports_corrections, supports_reversals FROM prep.int_investment_events__headers""").fetchall()
        == [(False, False, False), (False, False, False)]
    )
