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
