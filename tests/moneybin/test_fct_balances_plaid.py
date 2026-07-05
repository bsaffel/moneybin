"""Integration tests for the Plaid branch of core.fct_balances.

Covers two transforms unique to the plaid_balances CTE that the golden-fixture
tests (test_stg_plaid.py) don't exercise:

- **Liability sign** — Plaid reports credit/loan balances as a positive amount
  owed, but core.fct_balances / reports.net_worth treat liabilities as negative
  (net_worth sums balances; positive = asset, negative = liability). The CTE
  must negate credit/loan balances.
- **Null current balance** — SyncBalance.current_balance is nullable; a NULL
  would become a false $0 anchor once fct_balances_daily._to_decimal() coerces
  it, so such rows must be dropped.

Seeds raw.* + app.account_links directly (mirrors test_dim_accounts_merge.py) to
isolate the CTE SQL from the extractor/resolver path.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration

_ITEM = "item_test_plaid"


def _insert_plaid_account(
    db: Database,
    *,
    native_key: str,
    account_type: str,
    subtype: str = "checking",
    official_name: str = "Acct",
    mask: str = "0000",
) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_accounts
            (account_id, account_type, account_subtype, institution_name,
             official_name, mask, source_file, source_type, source_origin,
             extracted_at, loaded_at)
        VALUES (?, ?, ?, 'TestBank', ?, ?, 'sync_test', 'plaid', ?,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [native_key, account_type, subtype, official_name, mask, _ITEM],
    )


def _insert_plaid_balance(
    db: Database,
    *,
    native_key: str,
    current: str | None,
    available: str = "0.00",
    balance_date: str = "2026-04-08",
) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_balances
            (account_id, balance_date, current_balance, available_balance,
             source_file, source_type, source_origin, extracted_at, loaded_at)
        VALUES (?, ?::DATE, ?, ?, 'sync_test', 'plaid', ?,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [native_key, balance_date, current, available, _ITEM],
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
def test_plaid_credit_balance_negated_as_liability(db: Database) -> None:
    """Plaid credit/loan balances (positive amount owed) become negative liabilities."""
    _insert_plaid_account(
        db,
        native_key="p_credit",
        account_type="credit",
        subtype="credit card",
        official_name="Card",
        mask="9012",
    )
    _insert_plaid_balance(
        db, native_key="p_credit", current="850.00", available="4150.00"
    )
    _accept_link(db, native_key="p_credit", canonical_id="canoncredit0001")

    _insert_plaid_account(
        db,
        native_key="p_check",
        account_type="depository",
        official_name="Checking",
        mask="1234",
    )
    _insert_plaid_balance(
        db, native_key="p_check", current="1200.00", available="1200.00"
    )
    _accept_link(db, native_key="p_check", canonical_id="canoncheck00001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = dict(
        db.execute(
            "SELECT account_id, balance FROM core.fct_balances WHERE source_type = 'plaid'"
        ).fetchall()
    )
    assert rows["canoncredit0001"] == Decimal("-850.00"), (
        "credit liability must be negated"
    )
    assert rows["canoncheck00001"] == Decimal("1200.00"), (
        "depository asset stays positive"
    )


@pytest.mark.slow
def test_plaid_null_current_balance_filtered(db: Database) -> None:
    """A Plaid balance with NULL current_balance must not create a false $0 anchor."""
    _insert_plaid_account(
        db,
        native_key="p_null",
        account_type="depository",
        official_name="OnlyAvail",
        mask="3456",
    )
    _insert_plaid_balance(db, native_key="p_null", current=None, available="100.00")
    _accept_link(db, native_key="p_null", canonical_id="canonnull000001")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    count = db.execute(
        "SELECT COUNT(*) FROM core.fct_balances WHERE account_id = ?",
        ["canonnull000001"],
    ).fetchone()
    assert count is not None
    assert count[0] == 0, "null current_balance must be filtered, not anchored at 0"
