"""Scenario: cross-source transaction merge prefers a known currency over NULL.

Written during M1K.1 Task 5 self-review while investigating whether
``int_transactions__merged.sql``'s bare
``ARG_MIN(m.currency_code, COALESCE(sp.priority, 2147483647))`` (unlike ~15
sibling fields in the same model, which wrap their priority computation in an
explicit ``CASE WHEN NOT field IS NULL`` guard) could let a NULL from the
highest-priority source beat a real value from a lower-priority source now
that ``currency_code`` can be honestly NULL (M1K.1 Task 4 stopped
``int_transactions__unioned`` from defaulting every arm to 'USD').

Verified this is not a gap: DuckDB's ``ARG_MIN(arg, val)`` ignores rows where
``arg`` IS NULL (confirmed against a live DuckDB connection and by diffing the
CASE-wrapped form against the bare form on this exact fixture — identical
output). The wrapper on sibling fields guards against a different failure
mode (an unmapped ``source_type`` making ``val`` itself NULL, not ``arg``);
``currency_code``'s bare form was already correct. This scenario stays as a
permanent regression guard: if ``ARG_MIN``'s NULL-skipping semantics ever stop
holding (a DuckDB behavior change, or a future edit that swaps in a different
aggregate), this test catches the resulting silent currency loss.

Seeds one OFX transaction (no CURDEF → NULL currency_code, default
source_priority rank 3, higher priority) and one Plaid twin
(iso_currency_code='EUR', default source_priority rank 4, lower priority) on
the same account/date/amount so the real matcher auto-merges them. Asserts
the merged row's currency_code is 'EUR' — the only known value in the group —
even though the higher-priority OFX source has no currency of its own.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import polars as pl
import pytest

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.database import Database
from moneybin.extractors.plaid import PlaidExtractor
from moneybin.services.account_resolution_types import SourceAccount
from moneybin.services.account_resolver import AccountResolver
from moneybin.tables import OFX_ACCOUNTS, OFX_TRANSACTIONS
from tests.scenarios._runner.loader import Scenario, SetupSpec
from tests.scenarios._runner.runner import scenario_env
from tests.scenarios._runner.steps import run_step

_PLAID_ACCOUNT = "plaid_acct_currency_gap"
_OFX_ACCOUNT = "ofx_acct_currency_gap"
_DEDUP_DATE = "2026-06-01"
# Plaid raw amount is positive (Plaid: positive = expense); staging flips to
# -63.40. The OFX twin's raw amount must already be -63.40 so both unioned
# rows carry the identical signed amount and the cross-source dedup blocks
# match.
_PLAID_RAW_AMOUNT = "63.40"
_OFX_RAW_AMOUNT = Decimal("-63.40")
_KNOWN_CURRENCY = "EUR"

_PLAID_PAYLOAD: dict[str, object] = {
    "accounts": [
        {
            "account_id": _PLAID_ACCOUNT,
            "account_type": "depository",
            "account_subtype": "checking",
            "institution_name": "CurrencyGapBank",
            "official_name": "Currency Gap Checking",
            "mask": "4002",
        },
    ],
    "transactions": [
        {
            "transaction_id": "txn_plaid_currency_gap",
            "account_id": _PLAID_ACCOUNT,
            "transaction_date": _DEDUP_DATE,
            "amount": _PLAID_RAW_AMOUNT,
            "description": "EURO MERCHANT CO",
            "merchant_name": "Euro Merchant Co",
            "iso_currency_code": _KNOWN_CURRENCY,
            "pending": False,
        },
    ],
    "balances": [],
    "removed_transactions": [],
    "metadata": {
        "job_id": "44444444-4444-4444-4444-444444444444",
        "synced_at": "2026-06-01T12:00:00Z",
        "institutions": [
            {
                "provider_item_id": "item_currency_gap",
                "institution_name": "CurrencyGapBank",
                "status": "completed",
                "transaction_count": 1,
            }
        ],
    },
}


def _seed_ofx_twin(db: Database, *, account_id: str) -> None:
    """Insert one raw OFX account + transaction (the dedup twin of the Plaid row).

    Mirrors ``tests.scenarios._runner.fixture_loader`` column shapes. No
    ``currency_code`` is set on the transaction — a real OFX file lacking a
    CURDEF element leaves it NULL (Task 2's honest-capture behavior), which is
    the precondition for this bug: the higher-priority source has no currency
    of its own.
    """
    now = datetime.now(UTC)
    db.ingest_dataframe(
        OFX_ACCOUNTS.full_name,
        pl.DataFrame([
            {
                "account_id": account_id,
                "routing_number": None,
                "account_type": "CHECKING",
                "institution_org": "fixture",
                "institution_fid": None,
                "source_origin": "fixture",
                "source_file": "currency-gap-fixture",
                "extracted_at": now,
            }
        ]),
        on_conflict="upsert",
    )
    db.ingest_dataframe(
        OFX_TRANSACTIONS.full_name,
        pl.DataFrame([
            {
                "source_transaction_id": "ofx_currency_gap_001",
                "account_id": account_id,
                "transaction_type": "DEBIT",
                "date_posted": datetime(2026, 6, 1, tzinfo=UTC).replace(tzinfo=None),
                "amount": _OFX_RAW_AMOUNT,
                "payee": "EURO MERCHANT CO",
                "memo": None,
                "check_number": None,
                "source_file": "currency-gap-fixture",
                "source_origin": "fixture",
                "extracted_at": now,
            }
        ]).with_columns(pl.col("amount").cast(pl.Decimal(18, 2))),
        on_conflict="insert",
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_merge_prefers_known_currency_over_higher_priority_null() -> None:
    """OFX+Plaid dedup must not discard a known currency behind a NULL winner.

    Ground truth from input: the OFX twin carries no currency (NULL); the
    Plaid twin carries 'EUR'. OFX outranks Plaid in the default
    source_priority, but DuckDB's ARG_MIN ignores rows whose ``arg`` is NULL,
    so it does not pick OFX's NULL over Plaid's known value. The merged row's
    currency_code must be 'EUR' — the only known value in the group —
    regardless of which source wins the other fields.
    """
    scenario = Scenario(
        scenario="currency-code-null-preference-merge",
        setup=SetupSpec(persona="curator"),
        pipeline=[],
    )
    with scenario_env(scenario) as (db, _tmp, env):
        # --- Load Plaid, resolve its account → canonical id C ---
        sync_data = SyncDataResponse.model_validate(_PLAID_PAYLOAD)
        loader = PlaidExtractor(db)
        loader.load(sync_data, job_id=sync_data.metadata.job_id)

        item_by_account = loader.build_account_to_item_map(sync_data)
        acct_resolver = AccountResolver(db, actor="system")
        plaid_acc = sync_data.accounts[0]
        resolved = acct_resolver.resolve(
            SourceAccount(
                source_type="plaid",
                source_origin=item_by_account[plaid_acc.account_id],
                source_account_key=plaid_acc.account_id,
                account_name=plaid_acc.official_name or plaid_acc.account_id,
                account_number=None,
                last_four=plaid_acc.mask,
                institution=plaid_acc.institution_name,
            )
        )
        canonical_account_id = resolved.account_id

        # --- Seed the OFX twin and force its account onto the same canonical id ---
        # Explicit binding (decided_by='user') is a real mechanism; the
        # *account* match is not the subject under test — cross-source account
        # matching is proven separately in test_account_identity_cross_source.py.
        _seed_ofx_twin(db, account_id=_OFX_ACCOUNT)
        acct_resolver.resolve(
            SourceAccount(
                source_type="ofx",
                source_origin="fixture",
                source_account_key=_OFX_ACCOUNT,
                account_name="Currency Gap Checking (OFX)",
                account_number=None,
                last_four="4002",
                institution="fixture",
                explicit_account_id=canonical_account_id,
            )
        )

        # --- Real pipeline: transform → dedup match → transform ---
        run_step("transform", scenario.setup, db, env=env)
        run_step("match", scenario.setup, db, env=env)
        run_step("transform", scenario.setup, db, env=env)

        # --- Merged-model assertion ---
        # Derived from input: exactly the Plaid/OFX pair collapses
        # (source_count = 2); OFX (rank 3) outranks Plaid (rank 4) in the
        # default source_priority, so canonical_source_type is 'ofx' but the
        # only known currency in the group is Plaid's 'EUR'.
        dedup_row = db.execute(
            """
            SELECT canonical_source_type, currency_code, source_count
            FROM prep.int_transactions__merged
            WHERE amount = ? AND source_count = 2
            """,
            [_OFX_RAW_AMOUNT],
        ).fetchone()
        assert dedup_row is not None, (
            "expected one OFX+Plaid-deduped merged row for the currency-gap pair"
        )
        assert dedup_row[0] == "ofx", (
            f"merge winner should be 'ofx' (outranks plaid); got {dedup_row[0]!r}"
        )
        assert dedup_row[1] == _KNOWN_CURRENCY, (
            "currency_code must fall back to the only known value in the group "
            f"('{_KNOWN_CURRENCY}') instead of the higher-priority source's NULL; "
            f"got {dedup_row[1]!r}"
        )
