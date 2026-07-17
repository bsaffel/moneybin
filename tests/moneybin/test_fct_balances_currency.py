"""Integration tests for currency_code on core.fct_balances / fct_balances_daily.

multi-currency.md Requirement 3. Seeds raw.*/app.* directly (mirrors
test_fct_balances_plaid.py) to isolate the SQL from the extractor path.
"""

from __future__ import annotations

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
