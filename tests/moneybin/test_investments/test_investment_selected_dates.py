"""Source pairing windows do not constrain selected cross-source dates."""

from datetime import UTC, date, datetime
from decimal import Decimal

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.investments.event_choices import validate_selection
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.tables import INVESTMENT_EVENT_LEGS
from tests.moneybin.test_investments.comparison_helpers import install_comparison_models


def test_valid_source_pairs_preserve_explicit_selected_dates_beyond_pairing_window(
    comparison_db: Database,
) -> None:
    AccountLinksRepo(comparison_db).insert(
        link_id="second_origin",
        account_id="account",
        ref_kind="source_native",
        ref_value="native_account",
        source_type="plaid",
        source_origin="origin_b",
        decided_by="user",
        actor="test",
    )
    observations: list[SyncInvestmentTransaction] = []
    for origin, acquisition_day, income_day, explicit_role in (
        ("origin", 10, 12, "acquisition"),
        ("origin_b", 12, 14, "income"),
    ):
        for role, day in (("acquisition", acquisition_day), ("income", income_day)):
            acquisition = role == "acquisition"
            observations.append(
                SyncInvestmentTransaction(
                    investment_transaction_id=f"{origin}_{role}",
                    account_id="native_account",
                    provider_item_id=origin,
                    security_id="native_security",
                    date=date(2026, 1, day),
                    transaction_datetime=(
                        datetime(2026, 1, day, 12, tzinfo=UTC)
                        if role == explicit_role
                        else None
                    ),
                    name="Synthetic reinvestment",
                    quantity=Decimal("1") if acquisition else None,
                    amount=Decimal("100") if acquisition else Decimal("-100"),
                    price=Decimal("100") if acquisition else None,
                    fees=Decimal("0"),
                    type="buy" if acquisition else "cash",
                    subtype="dividend reinvestment" if acquisition else "dividend",
                )
            )
    PlaidExtractor(comparison_db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]
        observations,
        "sync_dates",
        datetime(2026, 2, 1, tzinfo=UTC),
        datetime(2026, 2, 1, tzinfo=UTC),
    )
    install_comparison_models(comparison_db)
    assert comparison_db.execute(
        """SELECT member_count, is_match_eligible
        FROM prep.int_investment_events__headers"""
    ).fetchall() == [(2, True), (2, True)]
    assert comparison_db.execute(
        """SELECT is_candidate FROM prep.int_investment_events__evidence"""
    ).fetchall() == [(True,), (True,)]
    service = InvestmentMatchingService(comparison_db)
    service.run()
    proposals = service.pending()
    assert len(proposals) == 1
    proposal = proposals[0]
    assert not proposal["field_choices"]
    cursor = comparison_db.execute(
        f"SELECT * FROM {INVESTMENT_EVENT_LEGS.full_name}"  # noqa: S608  # fixed TableRef
    )
    assert cursor.description is not None
    names = [column[0] for column in cursor.description]
    legs = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    selected = validate_selection(legs, proposal["field_choices"], [])
    assert {row["leg_role"]: row["trade_date"] for row in selected} == {
        "acquisition": date(2026, 1, 10),
        "income": date(2026, 1, 14),
    }
    assert all(row["trade_date_basis"] == "explicit" for row in selected)
