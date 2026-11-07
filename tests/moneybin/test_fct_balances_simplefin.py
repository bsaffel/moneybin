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
    from moneybin.connectors.sync_models import ScopeOutcome
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
        data.outcomes = [
            ScopeOutcome(
                account_id="acct",
                native_scope=data.history.accounts[0].native_scope,
                window=data.batches[0].window,
                status="complete",
                error_code=None,
            )
        ]
        load(db, data, job=number, day=number)
    bind_account(db)
    applied = TransformService(db).apply()
    assert applied.applied, applied.error
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
    correction.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=correction.history.accounts[0].native_scope,
            window=correction.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    load(db, correction, job=4, day=4)
    # The delivery also appends Account evidence, so its binding inputs need rebuilding.
    assert db.execute(
        "SELECT COUNT(DISTINCT batch_id) FROM raw.simplefin_accounts"
    ).fetchone() == (4,)
    applied = TransformService(db).apply()
    assert applied.applied, applied.error
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


@pytest.mark.parametrize("other_source", ["ofx", "plaid"])
def test_classification_change_preserves_newer_balance_observation(
    db: Database, other_source: str
) -> None:
    """Editing eligibility advances freshness without creating financial evidence."""
    from moneybin.services.account_service import AccountService
    from moneybin.services.transform_service import TransformService

    load(db, source())
    bind_account(db)
    if other_source == "ofx":
        db.execute("""INSERT INTO raw.ofx_balances
            (account_id, statement_end_date, statement_end_source_date, ledger_balance,
             ledger_balance_date, ledger_balance_source_date, currency_code,
             source_type, source_origin, source_file, extracted_at, loaded_at)
            VALUES ('canonical', '2026-09-20'::TIMESTAMPTZ, '2026-09-20'::DATE,
                    250.00, '2026-09-20'::TIMESTAMPTZ, '2026-09-20'::DATE,
                    'USD', 'ofx', 'synthetic-bank', 'synthetic-statement',
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""")
    else:
        db.execute("""INSERT INTO raw.plaid_accounts
            (account_id, account_type, account_subtype, source_file, source_type,
             source_origin, extracted_at, loaded_at)
            VALUES ('canonical', 'depository', 'checking', 'synthetic-sync',
                    'plaid', 'synthetic-item', CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP)""")
        db.execute("""INSERT INTO raw.plaid_balances
            (account_id, balance_date, current_balance, iso_currency_code,
             source_file, source_type, source_origin, extracted_at, loaded_at)
            VALUES ('canonical', '2026-09-20'::DATE, 250.00, 'USD',
                    'synthetic-sync', 'plaid', 'synthetic-item', CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP)""")
    materialize(db)
    before = db.execute("""SELECT updated_at FROM core.fct_balances
        WHERE account_id='canonical' AND source_type='simplefin'""").fetchone()
    assert before is not None
    assert db.execute("""SELECT balance, observation_source
        FROM core.fct_balances_daily
        WHERE account_id='canonical' AND balance_date='2026-09-20'""").fetchone() == (
        Decimal("250.00"),
        other_source,
    )

    AccountService(db).settings_update(
        "canonical", actor="cli", account_subtype="savings"
    )

    after = db.execute("""SELECT updated_at FROM core.fct_balances
        WHERE account_id='canonical' AND source_type='simplefin'""").fetchone()
    assert after is not None
    assert after[0] > before[0]
    assert db.execute("""SELECT balance, observation_source
        FROM core.fct_balances_daily
        WHERE account_id='canonical' AND balance_date='2026-09-20'""").fetchone() == (
        Decimal("250.00"),
        other_source,
    )

    TransformService(db).restate_models(["core.fct_balances_daily"])
    assert (
        db.execute("""SELECT updated_at FROM core.fct_balances
        WHERE account_id='canonical' AND source_type='simplefin'""").fetchone()
        == after
    )
    assert db.execute("""SELECT balance, observation_source
        FROM core.fct_balances_daily
        WHERE account_id='canonical' AND balance_date='2026-09-20'""").fetchone() == (
        Decimal("250.00"),
        other_source,
    )
