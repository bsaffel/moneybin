"""Integration tests for currency_code defaulting in prep.int_transactions__unioned.

Requirement 2 (multi-currency.md): no arm may COALESCE/hardcode an unknown
currency to 'USD'. Seeds raw.* directly (mirrors test_fct_balances_plaid.py)
to isolate the union SQL from the extractor path.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


def _insert_ofx_transaction(
    db: Database, *, txn_id: str, account_id: str, currency_code: str | None
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_transactions
            (source_transaction_id, account_id, transaction_type, date_posted,
             amount, payee, source_file, extracted_at, source_type,
             source_origin, currency_code)
        VALUES (?, ?, 'DEBIT', '2026-07-01'::TIMESTAMP, -10.00, 'Test Payee',
                'ofx_test', CURRENT_TIMESTAMP, 'ofx', 'test_bank', ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [txn_id, account_id, currency_code],
    )


def _insert_manual_transaction(
    db: Database, *, txn_id: str, account_id: str, currency_code: str | None
) -> None:
    db.execute(
        """
        INSERT INTO raw.manual_transactions
            (source_transaction_id, import_id, account_id, transaction_date,
             amount, description, created_by, currency_code)
        VALUES (?, 'manual_test_import', ?, '2026-07-01'::DATE, -10.00,
                'Test Manual Entry', 'cli', ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [txn_id, account_id, currency_code],
    )


@pytest.mark.slow
def test_ofx_arm_leaves_currency_null_when_curdef_missing(db: Database) -> None:
    """An OFX file with no captured CURDEF must NOT be relabeled USD in the union."""
    _insert_ofx_transaction(db, txn_id="t1", account_id="a1", currency_code=None)

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT currency_code FROM prep.int_transactions__unioned "
        "WHERE source_transaction_id = 't1'"
    ).fetchone()
    assert row is not None
    assert row[0] is None, "unknown currency must stay NULL, never default to USD"


@pytest.mark.slow
def test_ofx_arm_passes_through_captured_curdef(db: Database) -> None:
    """A captured non-USD CURDEF must survive the union unchanged."""
    _insert_ofx_transaction(db, txn_id="t2", account_id="a1", currency_code="EUR")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT currency_code FROM prep.int_transactions__unioned "
        "WHERE source_transaction_id = 't2'"
    ).fetchone()
    assert row is not None
    assert row[0] == "EUR"


@pytest.mark.slow
def test_manual_arm_leaves_currency_null_when_unspecified(db: Database) -> None:
    """A manual entry with no currency must NOT be relabeled USD in the union."""
    _insert_manual_transaction(db, txn_id="m1", account_id="a1", currency_code=None)

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT currency_code FROM prep.int_transactions__unioned "
        "WHERE source_transaction_id = 'm1'"
    ).fetchone()
    assert row is not None
    assert row[0] is None, "unknown currency must stay NULL, never default to USD"


@pytest.mark.slow
def test_manual_arm_passes_through_explicit_currency(db: Database) -> None:
    """A captured non-USD manual currency must survive the union unchanged."""
    _insert_manual_transaction(db, txn_id="m2", account_id="a1", currency_code="EUR")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        "SELECT currency_code FROM prep.int_transactions__unioned "
        "WHERE source_transaction_id = 'm2'"
    ).fetchone()
    assert row is not None
    assert row[0] == "EUR"
