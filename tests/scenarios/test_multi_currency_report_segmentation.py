"""Scenario: a mixed-currency profile gets sub-totals, never a blended figure.

multi-currency.md Requirement 5 (no silent blend) and Requirement 7 (report
guard) through the real pipeline — Plaid load → account resolution →
``transform`` → SQLMesh-built ``reports.*`` views. The unit tests in
``tests/moneybin/test_reports/test_sql_models.py`` install each model's SQL
against hand-made source tables; this one proves the same invariant survives
the deployed views, the staging currency capture, and the account-inheritance
COALESCE between them.

**The mixed fixture is the discriminating one.** A single-currency profile and
an all-foreign profile both return one row per grain whether or not
``currency_code`` is in the GROUP BY, so neither can tell a segmented model
from a blending one. Only a profile holding more than one currency at the same
grain separates them: segmented returns three rows summing to their own
currencies, blended returns one row summing to 375.

The second test covers the **unknown-currency segment**, which is the half of
Requirement 5 that a mixed-but-known fixture cannot reach. It is also the end
of the inheritance chain: a transaction with no captured currency falls back to
``core.dim_accounts.currency_code``, so if that column ever resolves to a
literal again, this test is what notices — the NULL segment and ``system
doctor``'s ``currency_integrity`` failure both stop being reachable at once,
silently, while every other currency test stays green.
"""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
from typing import cast

import pytest

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.services.account_resolution_types import SourceAccount
from moneybin.services.account_resolver import AccountResolver
from moneybin.services.doctor_service import DoctorService
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

_ACCOUNT = "plaid_acct_multicurrency"
_MONTH = "2026-05"
_DATE = "2026-05-11"

# Ground truth, hand-derived from the payload below before running anything.
# Plaid reports an expense as a positive amount; staging flips the sign, so each
# raw amount N becomes an outflow of -N in core.
#   USD: 100.00 + 25.00 = 125.00 outflow over 2 transactions
#   EUR: 200.00                  = 200.00 outflow over 1 transaction
#   GBP:  50.00                  =  50.00 outflow over 1 transaction
# A blended total would be 375.00 across 4 transactions in a single row.
_EXPECTED_OUTFLOW = {
    "USD": (Decimal("-125.00"), 2),
    "EUR": (Decimal("-200.00"), 1),
    "GBP": (Decimal("-50.00"), 1),
}
_BLENDED_OUTFLOW = Decimal("-375.00")

_TRANSACTIONS = (
    ("txn_mc_usd_1", "100.00", "USD", "GROCERY MART"),
    ("txn_mc_usd_2", "25.00", "USD", "GROCERY MART"),
    ("txn_mc_eur_1", "200.00", "EUR", "GROCERY MART"),
    ("txn_mc_gbp_1", "50.00", "GBP", "GROCERY MART"),
)

_PAYLOAD: dict[str, object] = {
    "accounts": [
        {
            "account_id": _ACCOUNT,
            "account_type": "depository",
            "account_subtype": "checking",
            "institution_name": "MultiCurrencyBank",
            "official_name": "Multi Currency Checking",
            "mask": "7301",
        },
    ],
    "transactions": [
        {
            "transaction_id": transaction_id,
            "account_id": _ACCOUNT,
            "transaction_date": _DATE,
            "amount": amount,
            "description": description,
            "merchant_name": "Grocery Mart",
            "iso_currency_code": currency,
            "pending": False,
        }
        for transaction_id, amount, currency, description in _TRANSACTIONS
    ],
    "balances": [],
    "removed_transactions": [],
    "metadata": {
        "job_id": "55555555-5555-5555-5555-555555555555",
        "synced_at": "2026-05-12T12:00:00Z",
        "institutions": [
            {
                "provider_item_id": "item_multicurrency",
                "institution_name": "MultiCurrencyBank",
                "status": "completed",
                "transaction_count": len(_TRANSACTIONS),
            }
        ],
    },
}


@pytest.mark.scenarios
@pytest.mark.slow
def test_mixed_currency_profile_gets_sub_totals_not_a_blended_figure() -> None:
    """Three currencies on one account yield three sub-totals in every report."""
    scenario = Scenario(
        scenario="multi-currency-report-segmentation",
        setup=SetupSpec(persona="curator"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        sync_data = SyncDataResponse.model_validate(_PAYLOAD)
        loader = PlaidExtractor(db)
        loader.load(sync_data, job_id=sync_data.metadata.job_id)

        item_by_account = loader.build_account_to_item_map(sync_data)
        account = sync_data.accounts[0]
        AccountResolver(db, actor="system").resolve(
            SourceAccount(
                source_type="plaid",
                source_origin=item_by_account[account.account_id],
                source_account_key=account.account_id,
                account_name=account.official_name or account.account_id,
                account_number=None,
                last_four=account.mask,
                institution=account.institution_name,
            )
        )

        run_step("transform", scenario.setup, db, env=env)

        cash_flow = db.execute(
            """
            SELECT currency_code, SUM(outflow), SUM(txn_count)
            FROM reports.cash_flow
            WHERE year_month = ?
            GROUP BY currency_code
            ORDER BY currency_code
            """,
            [_MONTH],
        ).fetchall()
        assert {row[0]: (row[1], row[2]) for row in cash_flow} == _EXPECTED_OUTFLOW, (
            "reports.cash_flow must sub-total each currency; a single row "
            f"totalling {_BLENDED_OUTFLOW} means the currencies blended"
        )

        spending = db.execute(
            """
            SELECT currency_code, SUM(total_spend)
            FROM reports.spending_trend
            WHERE year_month = ?
            GROUP BY currency_code
            ORDER BY currency_code
            """,
            [_MONTH],
        ).fetchall()
        assert {row[0]: row[1] for row in spending} == {
            currency: -outflow for currency, (outflow, _) in _EXPECTED_OUTFLOW.items()
        }, "reports.spending_trend must sub-total each currency"

        # Resolve the merchant the pipeline actually minted rather than
        # assuming its canonical label; all four fixture rows share one.
        merchants = db.execute(
            """
            SELECT m.currency_code, m.total_spend
            FROM reports.merchant_activity AS m
            WHERE m.merchant_id IS NOT DISTINCT FROM (
                SELECT DISTINCT t.merchant_id
                FROM core.fct_transactions AS t
                WHERE t.transaction_date = ?
            )
            ORDER BY m.currency_code
            """,
            [_DATE],
        ).fetchall()
        assert {row[0]: row[1] for row in merchants} == {
            currency: -outflow for currency, (outflow, _) in _EXPECTED_OUTFLOW.items()
        }, "one merchant billed in three currencies must not sum into one figure"

        # Every amount-bearing row names its currency: a NULL here would be an
        # amount whose unit nothing downstream could recover.
        unnamed = db.execute(
            "SELECT COUNT(*) FROM core.fct_transactions WHERE currency_code IS NULL"
        ).fetchone()
        assert unnamed is not None
        assert unnamed[0] == 0


@pytest.mark.scenarios
@pytest.mark.slow
def test_a_transaction_with_no_captured_currency_stays_unknown() -> None:
    """An uncaptured currency reaches core as NULL and fails system doctor.

    The account carries no currency of its own either — no balance rows, no
    user override — so nothing in the chain can supply one. That is the whole
    point: the honest answer is "unknown", and Requirement 8 forbids inventing
    one. Assert both ends, because they fail together: the NULL segment in the
    ledger, and the doctor check whose entire `fail` branch depends on a NULL
    being able to exist at all.
    """
    scenario = Scenario(
        scenario="multi-currency-unknown-segment",
        setup=SetupSpec(persona="curator"),
        pipeline=[],
    )
    payload = deepcopy(_PAYLOAD)
    transactions = cast(list[dict[str, object]], payload["transactions"])
    transactions[0]["iso_currency_code"] = None

    with scenario_env(scenario) as (db, _tmp, env):
        sync_data = SyncDataResponse.model_validate(payload)
        loader = PlaidExtractor(db)
        loader.load(sync_data, job_id=sync_data.metadata.job_id)

        item_by_account = loader.build_account_to_item_map(sync_data)
        account = sync_data.accounts[0]
        AccountResolver(db, actor="system").resolve(
            SourceAccount(
                source_type="plaid",
                source_origin=item_by_account[account.account_id],
                source_account_key=account.account_id,
                account_name=account.official_name or account.account_id,
                account_number=None,
                last_four=account.mask,
                institution=account.institution_name,
            )
        )

        run_step("transform", scenario.setup, db, env=env)

        unknown = db.execute(
            "SELECT COUNT(*) FROM core.fct_transactions WHERE currency_code IS NULL"
        ).fetchone()
        assert unknown is not None
        assert unknown[0] == 1, (
            "the one transaction with no iso_currency_code must stay unknown; "
            "a count of 0 means something down the chain invented a currency"
        )

        # The unknown row is its own segment, not folded into a known one.
        segments = db.execute(
            "SELECT currency_code FROM reports.cash_flow WHERE year_month = ?",
            [_MONTH],
        ).fetchall()
        assert None in {row[0] for row in segments}

        currency_check = next(
            invariant
            for invariant in DoctorService(db).run_all().invariants
            if invariant.name == "currency_integrity"
        )
        assert currency_check.status == "fail"
