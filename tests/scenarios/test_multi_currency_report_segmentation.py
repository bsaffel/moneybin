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

**Not covered here: the unknown-currency segment.** A Plaid transaction with no
``iso_currency_code`` does not reach ``core.fct_transactions`` as NULL —
``fct_transactions`` inherits ``core.dim_accounts.currency_code``, which still
resolves ``COALESCE(s.currency_code, 'USD')``. So the NULL segment the models
handle (and that ``system doctor``'s ``currency_integrity`` check reports) is
not reachable end-to-end yet. The models' NULL handling is covered by
``test_net_worth_keeps_unknown_currency_as_its_own_segment``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.services.account_resolution_types import SourceAccount
from moneybin.services.account_resolver import AccountResolver
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
