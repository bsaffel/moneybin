"""Integration tests for currency_code defaulting in prep.int_transactions__unioned.

Requirement 2 (multi-currency.md): no arm may COALESCE/hardcode an unknown
currency to 'USD'. Seeds raw.* directly (mirrors test_fct_balances_plaid.py)
to isolate the union SQL from the extractor path.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


def _insert_ofx_transaction(
    db: Database,
    *,
    txn_id: str,
    account_id: str,
    currency_code: str | None,
    transaction_date: str = "2026-07-01",
    amount: str = "-10.00",
    to_amount: str | None = None,
    to_currency: str | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_transactions
            (source_transaction_id, account_id, transaction_type, date_posted,
             amount, payee, source_file, extracted_at, source_type,
             source_origin, currency_code, to_amount, to_currency)
        VALUES (?, ?, 'DEBIT', ?::TIMESTAMP, ?::DECIMAL(18,2), 'Test Payee',
                'ofx_test', CURRENT_TIMESTAMP, 'ofx', 'test_bank', ?, ?, ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [
            txn_id,
            account_id,
            transaction_date,
            amount,
            currency_code,
            to_amount,
            to_currency,
        ],
    )


def _insert_manual_transaction(
    db: Database,
    *,
    txn_id: str,
    account_id: str,
    currency_code: str | None,
    transaction_date: str = "2026-07-01",
    amount: str = "-10.00",
    to_amount: str | None = None,
    to_currency: str | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO raw.manual_transactions
            (source_transaction_id, import_id, account_id, transaction_date,
             amount, description, created_by, currency_code, to_amount,
             to_currency)
        VALUES (?, 'manual_test_import', ?, ?::DATE, ?::DECIMAL(18,2),
                'Test Manual Entry', 'cli', ?, ?, ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [
            txn_id,
            account_id,
            transaction_date,
            amount,
            currency_code,
            to_amount,
            to_currency,
        ],
    )


def _insert_tabular_transaction(
    db: Database,
    *,
    txn_id: str,
    account_id: str,
    to_amount: str,
    to_currency: str,
) -> None:
    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             currency, to_amount, to_currency, source_file, source_type,
             source_origin, import_id)
        VALUES (?, ?, '2026-07-01'::DATE, -10.00, 'Test Tabular Entry',
                'USD', ?, ?, 'tabular_test', 'csv', 'test_bank',
                'tabular_test_import')
        """,  # noqa: S608  # test fixture, not executing user SQL
        [txn_id, account_id, to_amount, to_currency],
    )


def _insert_plaid_transaction(
    db: Database,
    *,
    txn_id: str,
    account_id: str,
    to_amount: str,
    to_currency: str,
) -> None:
    db.execute(
        """
        INSERT INTO raw.plaid_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             iso_currency_code, to_amount, to_currency, source_file,
             source_origin)
        VALUES (?, ?, '2026-07-01'::DATE, 10.00, 'Test Plaid Entry',
                'USD', ?, ?, 'plaid_test', 'test_bank')
        """,  # noqa: S608  # test fixture, not executing user SQL
        [txn_id, account_id, to_amount, to_currency],
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


@pytest.mark.slow
def test_received_leg_and_provenance_reach_each_prep_layer_unchanged(
    db: Database,
) -> None:
    """Every raw source preserves one atomic received leg through merged prep."""
    expected = [
        ("csv", "tabular-fx", Decimal("90.10"), "EUR"),
        ("manual", "manual-fx", Decimal("92.30"), "EUR"),
        ("ofx", "ofx-fx", Decimal("89.00"), "EUR"),
        ("plaid", "plaid-fx", Decimal("91.20"), "EUR"),
    ]
    _insert_ofx_transaction(
        db,
        txn_id="ofx-fx",
        account_id="ofx-account",
        currency_code="USD",
        to_amount="89.00",
        to_currency="EUR",
    )
    _insert_tabular_transaction(
        db,
        txn_id="tabular-fx",
        account_id="tabular-account",
        to_amount="90.10",
        to_currency="EUR",
    )
    _insert_plaid_transaction(
        db,
        txn_id="plaid-fx",
        account_id="plaid-account",
        to_amount="91.20",
        to_currency="EUR",
    )
    _insert_manual_transaction(
        db,
        txn_id="manual-fx",
        account_id="manual-account",
        currency_code="USD",
        to_amount="92.30",
        to_currency="EUR",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    for model in ("unioned", "matched"):
        rows = db.execute(
            "SELECT source_type, source_transaction_id, to_amount, to_currency "  # noqa: S608  # closed internal model set
            f"FROM prep.int_transactions__{model} WHERE NOT to_amount IS NULL "
            "ORDER BY source_type"
        ).fetchall()
        assert rows == expected

    merged = db.execute(
        """
        SELECT conversion_source_type, conversion_source_transaction_id,
               to_amount, to_currency
        FROM prep.int_transactions__merged
        WHERE NOT to_amount IS NULL
        ORDER BY conversion_source_type
        """
    ).fetchall()
    assert merged == expected

    core_columns = {
        row[1]
        for row in db.execute("PRAGMA table_info('core.fct_transactions')").fetchall()
    }
    assert {
        "to_amount",
        "to_currency",
        "conversion_source_type",
        "conversion_source_origin",
        "conversion_source_transaction_id",
    }.isdisjoint(core_columns)


@pytest.mark.slow
def test_merged_received_leg_prefers_one_complete_pair_with_its_provenance(
    db: Database,
) -> None:
    """A complete lower-priority pair beats an incomplete higher-priority row."""
    _insert_ofx_transaction(
        db,
        txn_id="ofx-partial-fx",
        account_id="shared-account",
        currency_code="USD",
        to_amount="88.00",
        to_currency=None,
    )
    _insert_manual_transaction(
        db,
        txn_id="manual-complete-fx",
        account_id="shared-account",
        currency_code="GBP",
        transaction_date="2026-07-02",
        amount="-90.00",
        to_amount="100.00",
        to_currency="USD",
    )
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES (
            'fx-pair', 'ofx-partial-fx', 'ofx', 'test_bank',
            'manual-complete-fx', 'manual', 'user', 'shared-account', 1.0,
            '{}', 'dedup', '3', NULL, 'accepted', 'test pair', 'auto',
            CURRENT_TIMESTAMP
        )
        """
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT conversion_from_date, conversion_from_amount,
               conversion_from_currency, to_amount, to_currency, conversion_source_type,
               conversion_source_origin, conversion_source_transaction_id
        FROM prep.int_transactions__merged
        WHERE account_id = 'shared-account'
        """
    ).fetchone()
    assert row == (
        date(2026, 7, 2),
        Decimal("-90.00"),
        "GBP",
        Decimal("100.00"),
        "USD",
        "manual",
        "user",
        "manual-complete-fx",
    )

    conversion = db.execute(
        """
        SELECT from_date, from_amount, from_currency, to_amount, to_currency,
               from_source_transaction_id
        FROM core.bridge_currency_conversions
        WHERE source_shape = 'single_row'
        """
    ).fetchone()
    assert conversion == (
        date(2026, 7, 2),
        Decimal("90.00"),
        "GBP",
        Decimal("100.00"),
        "USD",
        "manual-complete-fx",
    )
