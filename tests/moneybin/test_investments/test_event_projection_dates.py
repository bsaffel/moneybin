"""Split dates and acquired dates have stricter roles than candidate date bands."""

import pytest

from moneybin.database import Database
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
)


def _manual_sources(
    db: Database,
    type_: str,
    right_date: str = "2026-01-10",
    right_quantity: str = "2",
    left_acquired: str | None = None,
    right_acquired: str | None = None,
) -> None:
    # Distinct source origins exercise the adapter-neutral field contract.
    for native, trade_date, quantity, acquired in [
        ("left", "2026-01-10", "2", left_acquired),
        ("right", right_date, right_quantity, right_acquired),
    ]:
        db.execute(
            """INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, source_origin, import_id, account_id, security_id,
            type, trade_date, settlement_date, quantity, original_acquisition_date,
            currency_code, created_by
        ) VALUES (?, ?, 'import', 'account', 'security', ?, ?, '2026-01-11', ?, ?, 'USD', 'cli')""",
            [native, native, type_, trade_date, quantity, acquired],
        )
    install_comparison_models(db)


@pytest.mark.parametrize(
    ("day", "quantity", "eligible"),
    [
        ("2026-01-10", "2", True),
        ("2026-01-10", "2.0000000001", False),
        ("2026-01-11", "2", False),
    ],
)
def test_split_requires_same_trade_date_and_exact_ratio(
    comparison_db: Database, day: str, quantity: str, eligible: bool
) -> None:
    _manual_sources(comparison_db, "split", right_date=day, right_quantity=quantity)
    assert comparison_db.execute(
        "SELECT is_candidate FROM prep.int_investment_events__evidence"
    ).fetchall() == ([(eligible,)] if day == "2026-01-10" else [])


@pytest.mark.parametrize(
    ("left", "right", "conflict"),
    [
        ("2020-01-01", "2020-01-01", False),
        ("2020-01-01", "2020-01-02", True),
        (None, "2020-01-02", False),
        ("2020-01-01", None, False),
    ],
)
def test_acquired_date_is_projection_only(
    comparison_db: Database, left: str | None, right: str | None, conflict: bool
) -> None:
    _manual_sources(
        comparison_db, "transfer_in", left_acquired=left, right_acquired=right
    )
    assert comparison_db.execute(
        "SELECT is_candidate, original_acquisition_date_conflict FROM prep.int_investment_events__evidence"
    ).fetchall() == [(True, conflict)]
