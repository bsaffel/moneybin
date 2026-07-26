"""Integration tests for currency_code on core.fct_balances / fct_balances_daily.

multi-currency.md Requirement 3. Seeds raw.*/app.* directly (mirrors
test_fct_balances_plaid.py) to isolate the SQL from the extractor path.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration

_ITEM = "item_test_currency"


def _insert_plaid_account(db: Database, *, native_key: str) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_accounts
            (account_id, account_type, account_subtype, institution_name,
             official_name, mask, source_file, source_type, source_origin,
             extracted_at, loaded_at)
        VALUES (?, 'depository', 'checking', 'TestBank', 'Acct', '0000',
                'sync_test', 'plaid', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [native_key, _ITEM],
    )


def _insert_plaid_balance(
    db: Database, *, native_key: str, currency_code: str | None
) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_balances
            (account_id, balance_date, current_balance, available_balance,
             iso_currency_code, source_file, source_type, source_origin,
             extracted_at, loaded_at)
        VALUES (?, '2026-07-01'::DATE, 500.00, 500.00, ?, 'sync_test', 'plaid',
                ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [native_key, currency_code, _ITEM],
    )


def _accept_link(db: Database, *, native_key: str, canonical_id: str) -> None:
    db.execute(
        """
        INSERT INTO app.account_links
            (link_id, account_id, ref_kind, ref_value, source_type,
             source_origin, status, decided_by, decided_at)
        VALUES (?, ?, 'source_native', ?, 'plaid', ?, 'accepted', 'auto',
                CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [f"link_{native_key}", canonical_id, native_key, _ITEM],
    )


def _insert_ofx_account(db: Database, *, account_id: str) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_accounts
            (account_id, account_type, source_file, extracted_at,
             source_type, source_origin)
        VALUES (?, 'CHECKING', 'ofx_test', CURRENT_TIMESTAMP, 'ofx', 'test_bank')
        """,  # noqa: S608  # test fixture, not executing user SQL
        [account_id],
    )


def _insert_ofx_balance(
    db: Database,
    *,
    account_id: str,
    on_date: str,
    balance: str,
    source_file: str,
    currency_code: str = "USD",
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_balances
            (account_id, statement_end_date, ledger_balance, ledger_balance_date,
             source_file, extracted_at, source_type, source_origin, currency_code)
        VALUES (?, ?::TIMESTAMP, ?::DECIMAL(18, 2), ?::TIMESTAMP, ?,
                CURRENT_TIMESTAMP, 'ofx', 'test_bank', ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [account_id, on_date, balance, on_date, source_file, currency_code],
    )


def _insert_ofx_transaction_on(
    db: Database,
    *,
    txn_id: str,
    account_id: str,
    on_date: str,
    amount: str,
    currency_code: str,
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_transactions
            (source_transaction_id, account_id, transaction_type, date_posted,
             amount, payee, source_file, extracted_at, source_type,
             source_origin, currency_code)
        VALUES (?, ?, 'DEBIT', ?::TIMESTAMP, ?::DECIMAL(18, 2), 'Test Payee',
                'ofx_test', CURRENT_TIMESTAMP, 'ofx', 'test_bank', ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [txn_id, account_id, on_date, amount, currency_code],
    )


def _seed_mixed_currency_account(db: Database, *, account_id: str) -> None:
    """A USD account whose 07-02 activity is one USD and one EUR transaction.

    Both transactions are needed to isolate the defect: with only the EUR one,
    a carry that correctly excludes it and a carry that never applied any
    adjustment produce the same number.
    """
    _insert_ofx_account(db, account_id=account_id)
    _insert_ofx_balance(
        db,
        account_id=account_id,
        on_date="2026-07-01",
        balance="1000.00",
        source_file="ofx_test_open",
    )
    # 873.00 = 1000 - 100 USD - the EUR 25 settling at ~27 USD. MoneyBin cannot
    # know that rate, which is exactly why the EUR row must not enter the carry.
    _insert_ofx_balance(
        db,
        account_id=account_id,
        on_date="2026-07-03",
        balance="873.00",
        source_file="ofx_test_close",
    )
    _insert_ofx_transaction_on(
        db,
        txn_id=f"{account_id}_usd",
        account_id=account_id,
        on_date="2026-07-02",
        amount="-100.00",
        currency_code="USD",
    )
    _insert_ofx_transaction_on(
        db,
        txn_id=f"{account_id}_eur",
        account_id=account_id,
        on_date="2026-07-02",
        amount="-25.00",
        currency_code="EUR",
    )


@pytest.mark.slow
def test_foreign_currency_transaction_stays_out_of_the_carried_balance(
    db: Database,
) -> None:
    """A transaction in another currency is never added to the carried balance.

    multi-currency.md Requirement 5. The carry is denominated in the observation's
    currency, so adding a EUR amount to a USD balance sums unlike units — and the
    blend is invisible downstream, because the resulting row still claims USD.
    """
    _seed_mixed_currency_account(db, account_id="bal_mix")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT balance, is_observed FROM core.fct_balances_daily
        WHERE account_id = 'bal_mix' AND balance_date = '2026-07-02'::DATE
        """
    ).fetchone()
    assert row is not None
    assert row[1] is False, "07-02 has no observation; it must be interpolated"
    # 1000 - 100 USD. Summing the EUR 25 as though it were dollars gives 875.00.
    assert row[0] == Decimal("900.00")


@pytest.mark.slow
def test_unconverted_foreign_activity_surfaces_as_reconciliation_drift(
    db: Database,
) -> None:
    """Excluding a foreign transaction leaves the gap visible, not silent.

    The next observation's reconciliation_delta is the movement the carry could
    not explain, which is what `reports.balance_drift` already reports on.
    """
    _seed_mixed_currency_account(db, account_id="bal_drift")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT reconciliation_delta FROM core.fct_balances_daily
        WHERE account_id = 'bal_drift' AND balance_date = '2026-07-03'::DATE
        """
    ).fetchone()
    assert row is not None
    # 873.00 observed - 900.00 carried = the 27.00 of EUR spending MoneyBin
    # cannot express in USD. Blending the raw 25.00 in would report -2.00.
    assert row[0] == Decimal("-27.00")


@pytest.mark.slow
def test_a_currency_change_between_observations_yields_no_reconciliation_delta(
    db: Database,
) -> None:
    """When the observed currency changes, the prior carry is not comparable.

    `core.fct_balances` resolves currency per row, so a corrected re-import can
    legitimately move an account from USD to EUR between two observations.
    Subtracting the USD carry from the EUR observation would produce a number in
    no unit at all — and it would be labelled with the *new* currency, so
    `reports.balance_drift`'s currency_mismatch guard (which compares the row
    against the account) cannot see it: both sides read EUR and agree. NULL is
    the honest answer, and balance_drift already renders it as `no-data`.
    """
    _insert_ofx_account(db, account_id="bal_switch")
    _insert_ofx_balance(
        db,
        account_id="bal_switch",
        on_date="2026-07-01",
        balance="1000.00",
        source_file="ofx_switch_usd",
        currency_code="USD",
    )
    _insert_ofx_balance(
        db,
        account_id="bal_switch",
        on_date="2026-07-03",
        balance="900.00",
        source_file="ofx_switch_eur",
        currency_code="EUR",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT reconciliation_delta, currency_code FROM core.fct_balances_daily
        WHERE account_id = 'bal_switch' AND balance_date = '2026-07-03'::DATE
        """
    ).fetchone()
    assert row is not None
    # Blending would report 900 - 1000 = -100.00, a EUR-labelled USD difference.
    assert row[0] is None
    assert row[1] == "EUR", "the later observation's own currency must win"


@pytest.mark.slow
def test_plaid_balance_currency_captured(db: Database) -> None:
    """Plaid's iso_currency_code lands as currency_code on core.fct_balances."""
    _insert_plaid_account(db, native_key="p_eur")
    _insert_plaid_balance(db, native_key="p_eur", currency_code="EUR")
    _accept_link(db, native_key="p_eur", canonical_id="canoneur00000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT currency_code FROM core.fct_balances WHERE account_id = ?",
        ["canoneur00000001"],
    ).fetchone()
    assert row is not None
    assert row[0] == "EUR"


@pytest.mark.slow
def test_balance_currency_inherits_from_account_when_unknown(db: Database) -> None:
    """A balance with no captured currency inherits the account's currency_code."""
    _insert_plaid_account(db, native_key="p_gbp")
    _insert_plaid_balance(db, native_key="p_gbp", currency_code=None)
    _accept_link(db, native_key="p_gbp", canonical_id="canongbp00000001")
    db.execute(
        "INSERT INTO app.account_settings (account_id, currency_code) VALUES (?, ?)",
        ["canongbp00000001", "GBP"],
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT currency_code FROM core.fct_balances_daily WHERE account_id = ?",
        ["canongbp00000001"],
    ).fetchone()
    assert row is not None
    assert row[0] == "GBP"
