"""SimpleFIN signed balance observations are exact and deterministic."""

from decimal import Decimal

import pytest

from moneybin.database import Database
from tests.moneybin.test_extractors.test_simplefin_extractor import load, source
from tests.moneybin.test_stg_simplefin import bind_account, materialize

pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest.mark.parametrize("balance", ["-100.00", "25.00"])
def test_credit_debt_and_overpayment_preserve_provider_sign(
    db: Database, balance: str
) -> None:
    load(db, source(account_changes={"balance": balance, "currency": "EUR"}))
    bind_account(db, kind="credit", subtype="credit card")
    materialize(db)
    assert db.execute("""SELECT balance, currency_code FROM core.fct_balances
        WHERE source_type='simplefin'""").fetchall() == [(Decimal(balance), "EUR")]


def test_same_day_latest_provider_instant_is_not_arrival_order(db: Database) -> None:
    load(
        db,
        source(
            batch=2,
            day=2,
            account_changes={"balance": "120.00", "balance-date": 1789866000},
        ),
        job=2,
    )
    load(db, source(), job=1)
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT balance FROM core.fct_balances WHERE source_type='simplefin'"
    ).fetchall() == [(Decimal("120.00"),)]
    assert db.execute("SELECT count(*) FROM raw.simplefin_balances").fetchone() == (2,)


def test_unrepresentable_balance_is_not_available_balance_or_zero(db: Database) -> None:
    load(
        db, source(account_changes={"balance": "-100.001", "available-balance": "5.00"})
    )
    bind_account(db, kind="credit", subtype="credit card")
    materialize(db)
    assert db.execute(
        "SELECT count(*) FROM core.dim_accounts WHERE account_id='canonical'"
    ).fetchone() == (1,)
    assert db.execute(
        "SELECT count(*) FROM core.fct_balances WHERE source_type='simplefin'"
    ).fetchone() == (0,)


def test_balance_revision_return_and_immutable_tie_use_broker_order(
    db: Database,
) -> None:
    load(db, source(batch=3, day=3), job=3)
    load(db, source(batch=2, day=2, account_changes={"balance": "-100.00"}), job=2)
    load(db, source(), job=1)
    bind_account(db)
    materialize(db)
    assert db.execute(
        "SELECT balance FROM core.fct_balances WHERE source_type='simplefin'"
    ).fetchall() == [(Decimal("100.25"),)]
    assert db.execute("SELECT count(*) FROM raw.simplefin_balances").fetchone() == (3,)


def test_historical_report_retains_malformed_balance_correction(db: Database) -> None:
    from moneybin.reports._framework.catalog import get_report_catalog
    from moneybin.services.transform_service import TransformService
    from tests.moneybin.test_services.test_sync_completeness import (
        _history,  # pyright: ignore[reportPrivateUsage]
    )

    original_instant = 1789862400
    for number, amount, instant in (
        (1, "100.00", original_instant),
        (2, "not-money", original_instant),
        (3, "125.00", original_instant + 3 * 86400),
    ):
        data = source(
            batch=number,
            day=number,
            account_changes={
                "balance": amount,
                "balance-date": instant,
                "transactions": [],
            },
        )
        data.history = _history(number)
        load(db, data, job=number, day=number)
    bind_account(db)
    materialize(db)
    parameters = {"from_date": "2026-09-20", "to_date": "2026-09-20"}

    result = get_report_catalog().execute(
        db,
        report_id="core:net_worth_accounts",
        parameters=parameters,
        limit=10,
    )

    assert result.records[0]["account_balance"] == Decimal("100.00")
    coverage = result.to_envelope().to_dict()["summary"]["sync_completeness"]
    assert coverage["complete"] is False
    assert "invalid_observation" in {reason["kind"] for reason in coverage["reasons"]}

    correction = source(
        batch=4,
        day=4,
        account_changes={
            "balance": "105.00",
            "balance-date": original_instant,
            "transactions": [],
        },
    )
    correction.history = _history(4)
    load(db, correction, job=4, day=4)
    TransformService(db).restate_models(["core.fct_balances_daily"])
    corrected = get_report_catalog().execute(
        db,
        report_id="core:net_worth_accounts",
        parameters=parameters,
        limit=10,
    )
    assert corrected.records[0]["account_balance"] == Decimal("105.00")
    assert (
        corrected.to_envelope().to_dict()["summary"]["sync_completeness"]["complete"]
        is True
    )
